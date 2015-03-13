"""

"""

from whoosh.fields import SchemaClass, TEXT, KEYWORD, ID, STORED, DATETIME
import os

class BundleSchema(SchemaClass):

    vid = ID(stored=True, unique=True) # bundle VID

    title = TEXT(stored=True)
    summary = TEXT(stored=True)
    documentation = TEXT

    topic = KEYWORD # Groups, keywords and tags, probably.
    source = KEYWORD # fragments of sources


class PartitionSchema(SchemaClass):

    vid = ID(stored=True, unique=True) # vid of partition.

    grain = KEYWORD   # Index table ids, from foreign key values.
    geo_bound = KEYWORD # Geoids, from geoid fields state, county, place, school district, metro
    time_bound = KEYWORD # list of years or other iso dates, from year or date coumns.
    variables = KEYWORD # list of cannonical variables, or variable keywords. From schema.
    categoricals = KEYWORD # Categorical values? From codes or categorical values.


class DatasetSchema(SchemaClass):
    vid = ID(stored=True, unique=True) # Bundle versioned id
    id = ID(stored=True, unique=False) # Unversioned id

    title = TEXT(stored=True, field_boost=2.0)
    summary = TEXT(stored=True, field_boost=2.0)
    source = KEYWORD(field_boost=2.0) # Source ( domain ) of the

    doc = TEXT # Generated document for the core of the topic search

    @classmethod
    def make_doc(cls, d):
        """Create a Dict for loading into the DataSet schema.
        """
        doc = """
        Title
        Summary
        Internal Documentation
        Source agency

        Full Data dictionary, including table and column ids

        """


class PartitionSchema(SchemaClass):
    vid = ID(stored=True, unique=True) # Partition versioned id
    id = ID(stored=True, unique=False) # Unversioned id

    bvid = ID(stored=True, unique=True)  # Bundle versioned id

    title = TEXT(stored=True, field_boost=2.0) # Title of the table

    doc = TEXT # Generated document for the core of the topic search

    geo_coverage = ID(stored=True)
    grain = ID # vid of table or column for geo area or entity
    years = KEYWORD # Each year the dataset covers as a seperate entry
    detail = KEYWORD # age, rage, income and other common variable

    @classmethod
    def make_doc(cls, d):
        doc = ""

search_fields = ['identity','title','summary','keywords', 'groups','text','time','space','grain']



class Search(object):

    index_name = 'search_index'

    def __init__(self, doc_cache):

        self.doc_cache = doc_cache
        self.cache = self.doc_cache.cache

        self.index_dir = self.cache.path(self.index_name, propagate = False, missing_ok=True) # Return root directory

        self._ix = None

    def reset(self):

        if os.path.exists(self.index_dir):
            from shutil import rmtree
            rmtree(self.index_dir)

        self._ix = None



    @property
    def ix(self):
        from whoosh.index import create_in, open_dir

        if not self._ix:

            if not os.path.exists(self.index_dir):
                os.makedirs(self.index_dir)
                self._ix = create_in(self.index_dir, AmbrySchema)

            else:
                self._ix = open_dir(self.index_dir)


        return self._ix

    def index(self, reset=False):

        if reset:
            self.reset()

        writer = self.ix.writer()

        self.index_library(writer)

        self.index_tables(writer)

        self.index_databases(writer)

        writer.commit()

    def index_library(self, writer):

        l = self.doc_cache.get_library()

        for k, v in l['bundles'].items():

            b = self.doc_cache.get_bundle(k)

            if not b:
                print 'No bundle!', k
                continue

            try:
                a = b['meta'].get('about', {})
            except:
                continue

            keywords = a.get('keywords', []) + [ str(x) for x in b['identity'].values() if x ]

            def identity_parts(ident):
                parts = [ str(v) for v in ident.values() if v]

                # Add variations of the source, so that 'sandag' will match 'sandag.org'
                source_parts = ident['source'].split('.')
                prefixes = []
                while len(source_parts) >= 1:
                    prefixes.append(source_parts.pop(0))

                    parts.append('.'.join(source_parts))
                    parts.append('.'.join(prefixes))

                return u' '.join(parts)

            d = dict(
                type=u'bundle',
                vid=b['identity']['vid'],
                d_vid=b['identity']['vid'],
                fqname = b['identity']['fqname'],
                identity=identity_parts(b['identity']),
                title=a.get('title', u'') or u'',
                summary=a.get('summary', u'') or u'',
                keywords=u' '.join(keywords),
                groups=u' '.join(x for x in a.get('groups', []) if x) or u'',
                text=b['meta'].get('documentation', {}).get('main', u'') or u''
            )

            writer.add_document(**d)

    def index_tables(self, writer):
        import json

        l = self.doc_cache.get_library()

        for i, (k, b) in enumerate(l['bundles'].items()):
            s = self.doc_cache.get_schema(k)

            for t_vid, t in s.items():

                columns = u''
                keywords  = [t['vid'], t['id_']]
                for c_vid, c in t['columns'].items():

                    columns += u'{} {}\n'.format(c['name'], c.get('description',''))

                    keywords.append(c.get('altname',u''))
                    keywords.append(c['id_'])
                    keywords.append(c['vid'])
                    if c['name'].startswith(c['id_']):
                        vid, bare_name = c['name'].split('_',1)
                        keywords.append(bare_name)
                    else:
                        keywords.append(c['name'])

                d = dict(
                    vid=t_vid,
                    d_vid=b['identity']['vid'],
                    fqname=t['name'],
                    type=u'table',
                    title=t['name'],
                    summary=unicode(t.get('description','')),
                    keywords=keywords,
                    text=columns
                )

                writer.add_document(**d)

    def index_databases(self, writer):

        l = self.doc_cache.get_library()

        for i, (k, b) in enumerate(l['stores'].items()):
            names = set()

            s = self.doc_cache.get_store(k)

            for tvid, table in s['tables'].items():

                names.add(table['id_'])
                names.add(table['vid'])
                names.add(table['name'])
                names.add(table.get('altname',u''))

                if 'installed_names' in table:
                    names.update(set(table['installed_names']))


            d = dict(
                vid=s['uid'],
                d_vid=None,
                fqname=s['dsn'],
                type=u'store',
                title=s['title'],
                summary=s['summary'] if s['summary'] else u'',
                keywords=u' '.join(name for name in names if name)
            )

            try:
                writer.add_document(**d)
            except:
                print d
                raise


    def search(self, term):

        from whoosh.qparser import QueryParser, MultifieldParser

        with self.ix.searcher() as searcher:

            qp = MultifieldParser(search_fields, self.ix.schema)

            query = qp.parse(term)

            results = searcher.search(query, limit=None)

            bundles = {}
            stores = {}

            for hit in results:
                if hit.get('d_vid', False):
                    d_vid = hit['d_vid']

                    d = dict(score = hit.score,**hit.fields())

                    if d_vid not in bundles:
                        bundles[d_vid] = dict(score = 0, bundle = None, tables = [])

                    bundles[d_vid]['score'] += hit.score

                    if hit['type'] == 'bundle':
                        bundles[d_vid]['bundle'] = d
                    elif hit['type'] == 'table':
                        bundles[d_vid]['tables'].append(d)

                elif hit['type'] == 'store':
                    stores[hit['vid']] = dict(**hit.fields())


            # When there are a bunch of tables returned, but not the bundle, we need to re-create the bundle.
            for vid, e in bundles.items():

                if not e['bundle']:
                    cb = self.doc_cache.get_bundle(vid)
                    about = cb ['meta']['about']
                    e['bundle'] = dict(
                        title = about.get('title'),
                        summary = about.get('summary'),
                        fqname = cb['identity']['fqname'],
                        vname = cb['identity']['vname'],
                        source=cb['identity']['source'],
                        vid = cb['identity']['vid'])


            return bundles, stores


    def dump(self):

        for x in self.ix.searcher().documents():
            print x
