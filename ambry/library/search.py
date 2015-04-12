"""

"""

from whoosh.fields import SchemaClass, TEXT, KEYWORD, ID, STORED, DATETIME, NGRAMWORDS, NGRAM
import os
from ambry.util import memoize


class DatasetSchema(SchemaClass):

    vid = ID(stored=True, unique=True) # Bundle versioned id
    title = NGRAMWORDS(stored=True)
    source = NGRAMWORDS()  # Source ( domain ) of the
    doc = TEXT # Generated document for the core of the topic search

class PartitionSchema(SchemaClass):

    vid = ID(stored=True, unique=True) # Partition versioned id
    bvid = ID(stored=True, unique=True)  # Partition versioned id

    title = TEXT(stored=True)  # Title of the main table
    names = NGRAMWORDS()

    doc = TEXT # Generated document for the core of the topic search
    coverage = TEXT  # Lists of coverage identifiers, ISO time values and GVIDs
    values = TEXT  # List of uvalues from the stats for each column
    schema = TEXT  # List of uvalues from the stats for each column

class IdentifierSchema(SchemaClass):
    """Schema that maps well-known names to ID values, such as county names, summary level names, etc. """

    identifier = ID(stored=True) # Partition versioned id
    type=ID() #
    name = NGRAM(phrase=True, stored=True, minsize=2, maxsize=8)

class Search(object):

    def __init__(self, library):

        self.library = library

        self.cache = self.library._doc_cache

        self.d_index_dir = self.cache.path('search/dataset', propagate = False, missing_ok=True) # Return root directory
        self.p_index_dir = self.cache.path('search/partition', propagate=False, missing_ok=True)  # Return root directory
        self.i_index_dir = self.cache.path('search/identifiers', propagate=False, missing_ok=True)  # Return root directory

        self._dataset_index = None
        self._partition_index = None
        self._identifier_index = None

        self._dataset_writer = None
        self._partition_writer = None


    def reset(self):
        from shutil import rmtree

        if os.path.exists(self.d_index_dir):
            rmtree(self.d_index_dir)

        self._dataset_index = None

        if os.path.exists(self.p_index_dir):
            rmtree(self.p_index_dir)

        self._partition_index = None

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

        if self._partition_writer:
            self._partition_writer.commit()
            self._partition_writer = None


    @property
    def dataset_index(self):
        from whoosh.index import create_in, open_dir

        if not self._dataset_index:
            self._dataset_index =  self.get_or_new_index(DatasetSchema, self.d_index_dir)

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


    def index_dataset(self, bundle ):

        if bundle.identity.vid in self.all_datasets:
            return

        doc = u'\n'.join([ unicode(x) for x in [bundle.metadata.about.title,
                bundle.metadata.about.summary,
                bundle.identity.id_, bundle.identity.vid,
                bundle.identity.source,
                bundle.identity.name, bundle.identity.vname,
                bundle.metadata.documentation.main]])

        self.dataset_writer.add_document(
            vid=unicode(bundle.identity.vid),
            title=unicode(bundle.identity.name)+u' '+unicode(bundle.metadata.about.title),
            source=unicode(bundle.identity.source),
            doc=unicode(doc),

        )

        self.all_datasets.add(bundle.identity.vid )

    def index_datasets(self):

        ds_vids = [ds.vid for ds in self.library.datasets()]

        for vid in ds_vids:

            if vid in self.all_datasets:
                continue

            bundle = self.library.bundle(vid)

            self.index_dataset(bundle)

            for p in bundle.partitions:
                self.index_partition(p)

            bundle.close()

        self.commit()

    @property
    def datasets(self):

        for x in self.dataset_index.searcher().documents():
            yield x['vid']

    def search_datasets(self, search_phrase, limit = None):
        """Search for just the datasets"""
        from whoosh.qparser import QueryParser

        from whoosh.qparser import QueryParser

        parser = QueryParser("doc", schema = self.dataset_index.schema)

        query =  parser.parse(search_phrase)

        with self.dataset_index.searcher() as searcher:

            results = searcher.search(query, limit=limit)

            for hit in results:
                vid = hit.get('vid', False)
                if vid:
                    yield vid

    def search_bundles(self, search, limit=None):
        """

        Search for datasets and partitions using a structured search object

        :param search: a dict, with values for each of the search components.
        :param limit:
        :return:
        """
        from ..identity import ObjectNumber
        from collections import defaultdict

        bvid_term = with_term = grain_term = years_term = in_term = ''
        if search.get('in', False):
            place_vids = self.search_identifiers(search['in'])

            if place_vids:
                in_term = "coverage:({})".format(' OR '.join(x[1] for x in place_vids))

        if search.get('by', False):
            grain_term = "coverage:"+search.get('by','').strip()

        # The wackiness with the converts to int and str, and adding ' ', is because there
        # can't be a space between the 'TO' and the brackets in the time range when one end is open
        try:
            from_year = str(int(search.get('from', False)))+' '
        except ValueError:
            from_year = ''

        try:
            to_year = ' '+str(int(search.get('to', False)))
        except ValueError:
            to_year = ''

        if from_year or to_year:
            years_term = "coverage:[{}TO{}]".format(from_year, to_year)

        if search.get('with', False):
            with_term = 'schema:({})'.format(search.get('with', False))


        if search.get('about', False):
            # list(...) : the return from search_datasets is a generator, so it can only be read once.
            bvids =  list(self.search_datasets(search['about']))
            print search['about'], list(bvids)
        else:
            bvids = []

        p_term = ' AND '.join(x for x in [in_term, years_term, grain_term, with_term] if bool(x))

        if bool(p_term):
            if bvids:
                p_term += " AND bvid:({})".format(' OR '.join(bvids))
            else:
                # In case the about term didn't generate any hits for the bundle.
                p_term += " AND doc:({})".format(search['about'])
        else:
            if not bvids:
                p_term = "doc:({})".format(search['about'])


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

            rtrn = { b:[] for b in bvids }

        return (search['about'], p_term), rtrn

    @property
    def partition_index(self):
        from whoosh.index import create_in, open_dir

        if not self._partition_index:
            self._partition_index = self.get_or_new_index(PartitionSchema, self.p_index_dir)

        return self._partition_index

    @property
    def partition_writer(self):
        if not self._partition_writer:
            self._partition_writer = self.partition_index.writer()
        return self._partition_writer

    def index_partition(self, p):

        if p.identity.vid in self.all_partitions:
            return

        schema = '\n'.join("{} {} {} {} {}".format(c.id_, c.vid, c.name, c.altname, c.description) for c in p.table.columns)

        values = ''

        for col_name, stats in p.stats.items():
            if stats.uvalues:
                values += ' '.join(stats.uvalues) + '\n'

        coverage = (
            '\n'.join(p.data.get('geo_coverage', [])) + '\n' +
            '\n'.join(p.data.get('geo_grain', [])) + '\n' +
            '\n'.join(str(x) for x in p.data.get('time_coverage', []))
        )

        self.partition_writer.add_document(
            vid=unicode(p.identity.vid),
            bvid=unicode(p.identity.as_dataset().vid),
            names=u' '.join([unicode(p.identity.vid), unicode(p.identity.id_),
                             unicode(p.identity.name), unicode(p.identity.vname)]),
            title=unicode(p.table.description),
            schema=unicode(schema),
            coverage=unicode(coverage),
            values=unicode(values),
            doc = unicode(coverage+'\n'+values+'\n'+schema)
        )

        self.all_partitions.add(p.identity.vid)

    @property
    def partitions(self):

        for x in self.partition_index.searcher().documents():
            yield x['vid']

    @property
    @memoize
    def all_partitions(self):
        return set([x for x in self.partitions])

    def search_partitions(self, search_phrase, limit=None):
        from whoosh.qparser import QueryParser

        from whoosh.qparser import QueryParser

        parser = QueryParser("doc", schema=self.partition_index.schema)

        query = parser.parse(search_phrase)

        with self.partition_index.searcher() as searcher:

            results = searcher.search(query, limit=limit)

            for hit in results:
                vid = hit.get('vid', False)
                if vid:
                    yield vid


    @property
    def identifier_index(self):
        from whoosh.index import create_in, open_dir

        if not self._identifier_index:
            self._identifier_index = self.get_or_new_index(IdentifierSchema, self.i_index_dir)

        return self._identifier_index

    def index_identifiers(self, identifiers):

        index = self.identifier_index

        writer = index.writer()

        all_names = set([ x['name'] for x in index.searcher().documents() ])

        for i in identifiers:

            # Could use **i, but expanding it provides a  check on contents of i
            if i['name'] not in all_names:

                writer.add_document(
                    identifier = unicode(i['identifier']),
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
                    return ( 2.0 / (poses[0] + 1) +
                             1.0 / ( len(self.text) / 4 + 1 ) +
                             self.bmf25.scorer(searcher, self.fieldname, self.text).score(matcher) )


        with self.identifier_index.searcher(weighting=PosSizeWeighting()) as searcher:

            results = searcher.search(query, limit=10)

            for hit in results:
                vid = hit.get('identifier', False)
                name = hit.get('name', False)
                if vid:
                    yield hit.score, vid, name

    @property
    def identifiers(self):

        for x in self.partition_index.searcher().documents():
            yield x['vid'], x['name']
