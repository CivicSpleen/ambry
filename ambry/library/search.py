"""

"""

from whoosh.fields import SchemaClass, TEXT, KEYWORD, ID, STORED, DATETIME, NGRAMWORDS
import os


class DatasetSchema(SchemaClass):

    vid = ID(stored=True, unique=True) # Bundle versioned id
    id = ID(stored=True, unique=False) # Unversioned id

    title = NGRAMWORDS(stored=True)
    summary = NGRAMWORDS(stored=True)
    source = NGRAMWORDS(stored=True) # Source ( domain ) of the
    name = TEXT

    doc = TEXT # Generated document for the core of the topic search

class PartitionSchema(SchemaClass):

    vid = ID(stored=True, unique=True) # Partition versioned id
    id = ID(stored=True, unique=False) # Unversioned id

    bvid = ID(stored=True, unique=True)  # Bundle versioned id

    title = TEXT(stored=True) # Title of the main table

    doc = TEXT # Generated document for the core of the topic search

    geo_coverage = ID(stored=True)
    grain = ID # vid of table or column for geo area or entity
    years = KEYWORD # Each year the dataset covers as a seperate entry
    detail = KEYWORD # age, rage, income and other common variable

search_fields = ['identity','title','summary','keywords', 'groups','text','time','space','grain']


class Search(object):

    index_name = 'search_index'

    def __init__(self, library):

        self.library = library

        self.cache = self.library._doc_cache

        self.index_dir = self.cache.path(self.index_name, propagate = False, missing_ok=True) # Return root directory

        self._dataset_index = None
        self._partition_index = None

        self._dataset_writer = None
        self._partition_writer = None

        self.all_datasets = set([x for x in self.datasets])

    def reset(self):

        if os.path.exists(self.index_dir):
            from shutil import rmtree
            rmtree(self.index_dir)

        self._dataset_index = None

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

            if not os.path.exists(self.index_dir):
                os.makedirs(self.index_dir)
                self._dataset_index = create_in(self.index_dir, DatasetSchema)

            else:
                self._dataset_index = open_dir(self.index_dir)

        return self._dataset_index

    @property
    def dataset_writer(self):
        if not self._dataset_writer:
            self._dataset_writer = self.dataset_index.writer()
        return self._dataset_writer

    def index_dataset(self, bundle ):

        if bundle.identity.vid in self.all_datasets:
            return

        self.dataset_writer.add_document(
            vid=unicode(bundle.identity.vid),
            id=unicode(bundle.identity.id_),
            name=unicode(bundle.identity.name),
            title=unicode(bundle.metadata.about.title),
            summary=unicode(bundle.metadata.about.summary),
            source=unicode(bundle.identity.source),
            doc=unicode(
                bundle.metadata.documentation.main
            )
        )

        self.all_datasets.add(bundle.identity.vid )

    def index_datasets(self):

        ds_vids = [ds.vid for ds in self.library.datasets()]

        for vid in ds_vids:

            if vid in self.all_datasets:
                continue

            bundle = self.library.bundle(vid)

            self.index_dataset(bundle)

            bundle.close()

        self.commit()

    @property
    def datasets(self):

        for x in self.dataset_index.searcher().documents():
            yield x['vid']

    def search_datasets(self, search_phrase):
        from whoosh.qparser import QueryParser

        from whoosh.qparser import QueryParser

        parser = QueryParser("title", schema = self.dataset_index.schema)

        query =  parser.parse(search_phrase)

        with self.dataset_index.searcher() as searcher:

            results = searcher.search(query, limit=None)

            for hit in results:
                vid = hit.get('vid', False)
                if vid:
                    yield vid

    ##############


    @property
    def partition_index(self):
        from whoosh.index import create_in, open_dir

        if not self._partition_index:

            if not os.path.exists(self.index_dir):
                os.makedirs(self.index_dir)
                self._partition_index = create_in(self.index_dir, PartitionSchema)

            else:
                self._partition_index = open_dir(self.index_dir)

        return self._partition_index

    @property
    def partition_writer(self):
        if not self._partition_writer:
            self._partition_writer = self.partition_index.writer()
        return self._partition_writer

    def index_partition(self, bundle):

        if bundle.indentity.vid in self.all_datsets:
            return

        self.partition_writer.add_document(
            vid=unicode(bundle.identity.vid),
            id=unicode(bundle.identity.id_),
            title=unicode(bundle.metadata.about.title),
            summary=unicode(bundle.metadata.about.summary),
            source=unicode(bundle.identity.source),
            doc=unicode(
                bundle.metadata.documentation.main
            )
        )

        self.all_partitions.add(bundle.indentity.vid)

    def index_partitions(self):

        ds_vids = [ds.vid for ds in self.library.partitions()]

        for vid in ds_vids:

            if vid in self.all_partitions:
                continue

            bundle = self.library.bundle(vid)

            self.index_partition(bundle)

            bundle.close()

        self.commit()

    @property
    def partitions(self):

        for x in self.partition_index.searcher().documents():
            yield x['vid']

    def search_partitions(self, search_phrase):
        from whoosh.qparser import QueryParser

        from whoosh.qparser import QueryParser

        parser = QueryParser("title", schema=self.partition_index.schema)

        query = parser.parse(search_phrase)

        with self.partition_index.searcher() as searcher:

            results = searcher.search(query, limit=None)

            for hit in results:
                vid = hit.get('vid', False)
                if vid:
                    yield vid


    ###########


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

