""""""

from whoosh.fields import SchemaClass, TEXT, KEYWORD, ID, STORED, DATETIME, NGRAMWORDS, NGRAM
import os
from ambry.util import memoize


class DatasetSchema(SchemaClass):

    vid = ID(stored=True, unique=True)  # Object id
    bvid = ID(stored=True) # bundle vid
    type = ID(stored=True)

    title = NGRAMWORDS()

    keywords = KEYWORD  # Lists of coverage identifiers, ISO time values and GVIDs, source names, source abbrev
    doc = TEXT  # Generated document for the core of the topic search

class IdentifierSchema(SchemaClass):

    """Schema that maps well-known names to ID values, such as county names, summary level names, etc. """

    identifier = ID(stored=True)  # Partition versioned id
    type = ID(stored=True)
    name = NGRAM(phrase=True, stored=True, minsize=2, maxsize=8)


class Search(object):

    def __init__(self, library):

        self.library = library

        self.cache = self.library._doc_cache

        self.d_index_dir = self.cache.path('search/dataset', propagate=False, missing_ok=True)
        self.i_index_dir = self.cache.path( 'search/identifiers', propagate=False, missing_ok=True)
        self._dataset_index = None
        self._dataset_writer = None
        self._identifier_index = None

    def reset(self):
        from shutil import rmtree

        if os.path.exists(self.d_index_dir):
            rmtree(self.d_index_dir)

        self._dataset_index = None


        self._dataset_index = None

    def get_or_new_index(self, schema, dir):

        from whoosh.index import create_in, open_dir

        if not os.path.exists(dir):
            os.makedirs(dir)
            index = create_in(dir, schema)

        else:
            index = open_dir(dir)

        return index

    def commit(self):

        if self._dataset_writer:
            self._dataset_writer.commit()
            self._dataset_writer = None



    @property
    def dataset_index(self):
        from whoosh.index import create_in, open_dir

        if not self._dataset_index:
            self._dataset_index = self.get_or_new_index(DatasetSchema,self.d_index_dir)

        return self._dataset_index

    @property
    def dataset_writer(self):
        if not self._dataset_writer:
            self._dataset_writer = self.dataset_index.writer()
        return self._dataset_writer

    @property
    @memoize
    def all_datasets(self):
        return set([x for x in self.datasets])

    def dataset_doc(self, bundle):
        from geoid.civick import GVid

        e = bundle.database.session.execute

        q = """SELECT t_name, c_name, c_description FROM columns
                JOIN tables ON c_t_vid = t_vid WHERE t_d_vid = '{}' """.format(str(bundle.identity.vid))

        doc = u'\n'.join([unicode(x) for x in [bundle.metadata.about.title,
                                               bundle.metadata.about.summary,
                                               bundle.identity.id_,
                                               bundle.identity.vid,
                                               bundle.identity.source,
                                               bundle.identity.name,
                                               bundle.identity.vname,
                                               bundle.metadata.documentation.main,
                                               '\n'.join([' '.join(list(t)) for t in e(q)])]])

        # From the source, make a varity of combinations for keywords:
        # foo.bar.com -> "foo foo.bar foo.bar.com bar.com"
        p = unicode(bundle.identity.source).split('.')
        sources = ( ['.'.join(g) for g in [p[-i:] for i in range(2, len(p) + 1)]]
                    + ['.'.join(g) for g in [p[:i] for i in range(0, len(p))]])

        # Re-calculate the summarization of grains, since the geoid 0.0.7 package had a bug where state level
        # summaries had the same value as state-level allvals
        def resum(g):
            try:
                return str(GVid.parse(g).summarize())
            except KeyError:
                return g


        keywords = u' '.join(unicode(x) for x in [
            list(bundle.metadata.about.groups) + list(bundle.metadata.about.tags) +
            [resum(g) for g in bundle.metadata.coverage.grain] +
            list(bundle.metadata.coverage.geo) +
            list(bundle.metadata.coverage.time) +
            sources])

        d = dict(
            type=u'b',
            vid=unicode(bundle.identity.vid),
            bvid=unicode(bundle.identity.vid),
            title=unicode(bundle.identity.name) + u' ' + unicode(bundle.metadata.about.title),
            doc=unicode(doc),
            keywords=unicode(keywords)
        )

        return d

    def index_dataset(self, bundle, force=False):

        if bundle.identity.vid in self.all_datasets and not force:
            return

        d = self.dataset_doc(bundle)

        if force:
            self.dataset_writer.delete_by_term(
                'vid', unicode(
                    bundle.identity.vid))

        self.dataset_writer.add_document(**d)

        self.all_datasets.add(bundle.identity.vid)

    def index_partition(self, p, force=False):
        from geoid.civick  import GVid
        if p.identity.vid in self.all_partitions and not force:
            return

        schema = '\n'.join(
            "{} {} {} {} {}".format(
                c.id_,
                c.vid,
                c.name,
                c.altname,
                c.description) for c in p.table.columns)

        values = ''

        for col_name, stats in p.stats.items():
            if stats.uvalues:
                values += ' '.join(stats.uvalues) + '\n'

        # Re-calculate the summarization of grains, since the geoid 0.0.7 package had a bug where state level
        # summaries had the same value as state-level allvals
        def resum(g):
            try:
                return str(GVid.parse(g).summarize())
            except KeyError:
                return g


        keywords = (
            '\n'.join(p.data.get('geo_coverage', [])) + '\n' +
            '\n'.join([resum(g) for g in p.data.get('geo_grain', [])]) + '\n' +
            '\n'.join(str(x) for x in p.data.get('time_coverage', []))
        )

        self.dataset_writer.add_document(
            type=u'p',
            vid=unicode(p.identity.vid),
            bvid=unicode(p.identity.as_dataset().vid),
            title=unicode(p.table.description),
            keywords=unicode(keywords),
            doc=unicode(values + '\n' + schema + '\n'
                        u' '.join([unicode(p.identity.vid), unicode(p.identity.id_),
                                 unicode(p.identity.name), unicode(p.identity.vname)]))
        )


        self.all_partitions.add(p.identity.vid)

    def index_datasets(self, tick_f = None):

        ds_vids = [ds.vid for ds in self.library.datasets()]

        dataset_n = 0
        partition_n = 0

        def tick(d,p):
            if tick_f:
                tick_f("datasets: {} partitions: {}".format(d,p))

        for vid in ds_vids:

            if vid in self.all_datasets:
                continue

            dataset_n += 1
            tick(dataset_n, partition_n)

            bundle = self.library.bundle(vid)

            self.index_dataset(bundle)

            for p in bundle.partitions:
                self.index_partition(p)
                partition_n += 1
                tick(dataset_n, partition_n)

            bundle.close()

        self.commit()

    @property
    def datasets(self):

        for x in self.dataset_index.searcher().documents():
            if x['type'] == 'b':
                yield x['vid']

    def search_datasets(self, search_phrase, limit=None):
        """Search for just the datasets."""
        from collections import defaultdict

        from whoosh.qparser import QueryParser

        parser = QueryParser("doc", schema=self.dataset_index.schema)

        query = parser.parse(search_phrase)

        datasets = defaultdict(lambda: dict(score=0, pvids=set()))

        with self.dataset_index.searcher() as searcher:

            results = searcher.search(query, limit=limit)

            for hit in results:

                vid = hit.get('vid', False)
                bvid = hit.get('bvid', vid)

                pvid = vid if vid != bvid else None

                datasets[bvid]['score'] += hit.score
                datasets[bvid]['pvids'].add(pvid)

        return datasets

    def make_query_from_terms(self, terms):
        """ Create a Whoosh query from decomposed search terms
        """

        if not isinstance(terms, dict):
            stp = SearchTermParser()
            terms = stp.parse(terms)

        b_keywords = list()
        p_keywords = list()
        b_doc = list()
        p_doc = list()

        # The top level ( title, names, keywords, doc ) will get ANDed together

        if terms.get('about', False):
            b_doc.append(terms['about'])
            p_doc.append(terms['about'])


        if terms.get('with', False):
            p_doc.append(terms['with'])

        if terms.get('source', False):
            b_keywords.append(terms['source'])

        if terms.get('in', False):
            place_vids = self.expand_place_ids(terms['in'])
            p_keywords.append(place_vids)

        if terms.get('by', False):
            p_keywords.append(terms['by'])

        frm_to = self.from_to_as_term(terms.get('from', None), terms.get('to', None))

        if frm_to:
            p_keywords.append(frm_to)

        def or_join(terms):

            if isinstance(terms, (tuple, list)):
                return '(' +' OR '.join(terms) + ')'
            else:
                return terms

        def and_join(terms):
            return '('+ ' AND '.join([ or_join(t) for t in terms]) + ')'

        def kwd_term(keyword, terms):
            if terms:
                return keyword+":("+and_join(terms) +')'
            else:
                return None

        def per_type_terms(ttype, terms):

            terms = [ x for x in terms if bool(x)]

            if not terms:
                return ''

            return "( type:{} AND {} )".format(ttype, ' AND '.join(terms))

        def bp_terms(terms):

            return ' OR '.join([x for x in terms if bool(x)])


        return bp_terms([per_type_terms('b',[kwd_term("keywords",b_keywords), kwd_term("doc",b_doc)]),
                        per_type_terms('p',[kwd_term("keywords",p_keywords), kwd_term("doc",p_doc)])])

    def search_bundles(self, search, limit=None):
        """Search for datasets and partitions using a structured search object.

        :param search: a dict, with values for each of the search components.
        :param limit:
        :return:

        """
        from ..identity import ObjectNumber
        from collections import defaultdict
        from geoid.civick import GVid

        # If the search term has not been decomposed, decompose it.
        if search.get('all', False):
            search = SearchTermParser().parse(search['all'])

        about_term = source_term = with_term = grain_term = years_term = in_term = ''

        if search.get('source', False):
            source_term = "keywords:" + search.get('source', '').strip()

        if search.get('about', False):
            about_term = "doc:({})".format(search.get('about', '').strip())

        d_term = ' AND '.join(x for x in [source_term, about_term] if bool(x))

        # This is the doc terms we'll move to the partition search if the partition search returns nothing
        # but only if the about term was the only one specified.
        dt_p_term = about_term if not source_term else None

        if search.get('in', False):
            place_vids = list(
                x[1] for x in self.search_identifiers(
                    search['in']))  # COnvert generator to list

            if place_vids:

                # Add the 'all region' gvids for the higher level

                all_set = set(str(GVid.parse(x).allval()) for x in place_vids)

                place_vids += list(all_set)

                in_term = "keywords:({})".format(' OR '.join(place_vids))

        if search.get('by', False):
            grain_term = "keywords:" + search.get('by', '').strip()

        # The wackiness with the converts to int and str, and adding ' ', is because there
        # can't be a space between the 'TO' and the brackets in the time range
        # when one end is open

        if search.get('from', False):
            try:
                from_year = str(int(search.get('from', False))) + ' '
            except ValueError:
                pass
        else:
            from_year = ''

        if search.get('to', False):
            try:
                to_year = ' ' + str(int(search.get('to', False)))
            except ValueError:
                pass
        else:
            to_year = ''

        if bool(from_year) or bool(to_year):

            years_term = "keywords:[{}TO{}]".format(from_year, to_year)

        if search.get('with', False):
            with_term = 'doc:({})'.format(search.get('with', False))

        if bool(d_term):
            # list(...) : the return from search_datasets is a generator, so it
            # can only be read once.
            bvids = list(self.search_datasets(d_term))

        else:
            bvids = []

        p_term = ' AND '.join(
            x for x in [ in_term, years_term, grain_term, with_term] if bool(x))

        if bool(p_term):
            if bvids:
                p_term += " AND bvid:({})".format(' OR '.join(bvids))
            elif dt_p_term:
                # In case the about term didn't generate any hits for the
                # bundle.
                p_term += " AND {}".format(dt_p_term)
        else:
            if not bvids and dt_p_term:
                p_term = dt_p_term

        if p_term:
            pvids = list(self.search_partitions(p_term))

            if pvids:
                bp_set = defaultdict(set)
                for p in pvids:
                    bvid = str(ObjectNumber.parse(p).as_dataset)
                    bp_set[bvid].add(p)

                rtrn = {b: list(p) for b, p in bp_set.items()}
            else:
                rtrn = {}

        else:

            rtrn = {b: [] for b in bvids}

        return (d_term, p_term, search), rtrn



    @property
    def partitions(self):

        for x in self.dataset_index.searcher().documents():
            if x['type'] == 'p':
                yield x['vid']

    @property
    @memoize
    def all_partitions(self):
        return set([x for x in self.partitions])

    def search_partitions(self, search_phrase, limit=None):
        from whoosh.qparser import QueryParser

        from whoosh.qparser import QueryParser

        parser = QueryParser("doc", schema=self.dataset_index.schema)

        query = parser.parse(search_phrase)

        with self.dataset_index.searcher() as searcher:

            results = searcher.search(query, limit=limit)

            for hit in results:
                vid = hit.get('vid', False)
                if vid:
                    yield vid

    @property
    def identifier_index(self):
        from whoosh.index import create_in, open_dir

        if not self._identifier_index:
            self._identifier_index = self.get_or_new_index(
                IdentifierSchema,
                self.i_index_dir)

        return self._identifier_index

    def index_identifiers(self, identifiers):

        index = self.identifier_index

        writer = index.writer()

        all_names = set([x['name'] for x in index.searcher().documents()])

        for i in identifiers:

            # Could use **i, but expanding it provides a  check on contents of
            # i
            if i['name'] not in all_names:

                writer.add_document(
                    identifier=unicode(i['identifier']),
                    type=unicode(i['type']),
                    name=unicode(i['name']),
                )

        writer.commit()

    def search_identifiers(self, search_phrase, limit=10):

        from whoosh.qparser import QueryParser
        from whoosh import scoring

        parser = QueryParser("name", schema=self.identifier_index.schema)

        query = parser.parse(search_phrase)

        class PosSizeWeighting(scoring.WeightingModel):

            def __init__(self):
                pass

            def scorer(self, searcher, fieldname, text, qf=1):
                return self.PosSizeScorer(searcher, fieldname, text, qf=qf)

            class PosSizeScorer(scoring.BaseScorer):

                def __init__(self, searcher, fieldname, text, qf=1):

                    self.searcher = searcher
                    self.fieldname = fieldname
                    self.text = text
                    self.qf = qf
                    self.bmf25 = scoring.BM25F()

                def max_quality(self):
                    return 40

                def score(self, matcher):
                    poses = matcher.value_as("positions")
                    return (2.0 /(poses[0] +1) + 1.0 / (len(self.text) / 4 + 1) +
                            self.bmf25.scorer(searcher, self.fieldname, self.text).score(matcher))

        with self.identifier_index.searcher(weighting=PosSizeWeighting()) as searcher:

            results = searcher.search(query, limit=limit)

            for hit in results:
                vid = hit.get('identifier', False)
                name = hit.get('name', False)
                t = hit.get('type', False)
                if vid:
                    yield hit.score, vid, t, name

    def expand_place_ids(self,terms):
        """ Lookup all of the place identifiers to get gvids

        :param terms:
        :return:
        """
        from geoid.civick import GVid

        place_vids = []
        first_type = None

        for score, vid, t, name in self.search_identifiers(terms):

            if not first_type: first_type = t

            if t != first_type: # Ignore ones that aren't the same type as the best match
                continue

            place_vids.append(vid)

        if place_vids:
            # Add the 'all region' gvids for the higher level

            all_set = set(str(GVid.parse(x).allval()) for x in place_vids)

            place_vids += list(all_set)

            return place_vids

        else:
            return terms


    @property
    def identifiers(self):
        for x in self.identifier_index.searcher().documents():
            yield x

    @property
    @memoize
    def all_identifiers(self):
        return {x['identifier']: x['name'] for x in self.identifiers}

    @property
    @memoize
    def identifier_map(self):
        m = {}
        for x in self.identifiers:
            if len(x['name']) > 2:  # Exclude state abbreviations
                m[x['identifier']] = x['name']

        return m

    def from_to_as_term(self, frm, to):
        """ Turn from and to into the query format.
        :param frm:
        :param to:
        :return:
        """

        # The wackiness with the convesion to int and str, and adding ' ', is because there
        # can't be a space between the 'TO' and the brackets in the time range
        # when one end is open

        if frm:
            try:
                from_year = str(int(frm)) + ' '
            except ValueError:
                pass
        else:
            from_year = ''

        if to:
            try:
                to_year = ' ' + str(int(to))
            except ValueError:
                pass
        else:
            to_year = ''

        if bool(from_year) or bool(to_year):
            return "[{}TO{}]".format(from_year, to_year)
        else:
            return None



class SearchTermParser(object):
    """Decompose a search term in to conceptual parts, according to the Ambry search model."""
    TERM = 0
    QUOTEDTERM = 1
    LOGIC = 2
    MARKER = 3
    YEAR = 4

    types = {
        TERM: 'TERM',
        QUOTEDTERM: 'TERM',
        LOGIC: 'LOGIC',
        MARKER: 'MARKER',
        YEAR: 'YEAR'
    }

    marker_terms = {
        'about': 'about' ,
        'in': ('coverage', 'grain'),
        'by': ('grain'),
        'with': 'with',
        'from': ('year', 'source'),
        'to': ('year'),
        'source': ('source')}

    by_terms = 'state county zip zcta tract block blockgroup place city cbsa msa'.split()

    @staticmethod
    def s_quotedterm(scanner, token):
        return (SearchTermParser.QUOTEDTERM, token.lower().strip())

    @staticmethod
    def s_term(scanner, token):
        return (SearchTermParser.TERM, token.lower().strip())

    @staticmethod
    def s_domainname(scanner, token):
        return (SearchTermParser.TERM, token.lower().strip())

    @staticmethod
    def s_markerterm(scanner, token):
        return (SearchTermParser.MARKER, token.lower().strip())

    @staticmethod
    def s_year(scanner, token):
        return (SearchTermParser.YEAR, int(token.lower().strip()))

    def __init__(self):
        # I have no idea which one to pick
        from nltk.stem.lancaster import LancasterStemmer
        import re

        mt = '|'.join(
            [r"\b" + x.upper() + r"\b" for x in self.marker_terms.keys()])

        self.scanner = re.Scanner([
            (r"\s+", None),
            (r"['\"](.*?)['\"]", self.s_quotedterm),
            (mt.lower(), self.s_markerterm),
            (mt, self.s_markerterm),
            (r"19[789]\d|20[012]\d", self.s_year),  # From 1970 to 2029
            (r"(?:[\w\-\.?])+", self.s_domainname),
            (r".+?\b", self.s_term),
        ])

        self.stemmer = LancasterStemmer()

        self.by_terms = [self.stem(x) for x in self.by_terms]

    def scan(self, s):
        s = ' '.join(s.splitlines())  # make a single line
        # Returns one item per line, but we only have one line
        return self.scanner.scan(s)[0]

    def stem(self, w):
        return self.stemmer.stem(w)

    def parse(self, s):

        toks = self.scan(s)

        # Assume the first term is ABOUT, if it is not marked with a marker.
        if toks[0][0] == self.TERM or toks[0][0] == self.QUOTEDTERM:
            toks = [(self.MARKER, 'about')] + toks

        # Group the terms by their marker.
        last_marker = None
        bymarker = []
        for t in toks:
            if t[0] == self.MARKER:

                bymarker.append((t[1], []))
            else:
                bymarker[-1][1].append(t)

        # Convert some of the markers based on their contents
        comps = []
        for t in bymarker:
            t = list(t)
            if t[0] == 'in' and len(t[1]) == 1 and isinstance(t[1][0][1], basestring) and self.stem(t[1][0][1]) in self.by_terms:
                t[0] = 'by'

            # If the from term isn't an integer, then it is really a source.
            if t[0] == 'from' and len(t[1]) == 1 and t[1][0][0] != self.YEAR:
                t[0] = 'source'

            comps.append(t)

        # Join all of the terms into single marker groups
        groups = {marker: [] for marker, _ in comps}

        for marker, terms in comps:
            groups[marker] += [term for marker, term in terms]

        for marker, terms in groups.items():

            if len(terms) > 1:
                if marker in ('in'):
                    groups[marker] = ' '.join(terms)
                else:
                    groups[marker] = '(' + ' OR '.join(terms) + ')'
            elif len(terms) == 1:
                groups[marker] = terms[0]
            else:
                pass

        return groups
