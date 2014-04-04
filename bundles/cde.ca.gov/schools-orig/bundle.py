'''
'''

from  ambry.bundle import BuildBundle
 
class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)

    def meta_get_urls(self):
        '''Get the URLS for the CSV files from the repository, so the builder
        of the package does not need an account on the repo '''
        from ambry.client.ckan import new_ckan
        import re
        import yaml
        ckan = new_ckan(self.config.config.datarepo('default'))
        package = ckan.get_package(self.config.build.private_schools.source_package)
        
        urls = []
        
        for r in package['resources']:

            m = re.search(r'(\d{4})-(\d{4})', r['name']).groups()
            year = int(m[0])

            urls.append(dict(
                name = str(r['name']),
                year = year,
                url = str(r['url'])
            ))

        with open(self.filesystem.path('meta','urls.yaml'), 'w') as f:
            f.write(yaml.dump(urls, indent=4, default_flow_style=False))


    def meta_scrape_table(self):
        from bs4 import BeautifulSoup as soup
        import requests

        url = self.config.build.public_schools.schema

        type_map = {
        'Character':'varchar', 
        'Charter':'varchar', # A spelling error in the source page. 
        'Decimal':'real',
        'Date':'datetime'}
        

        if not self.database.exists():
            self.database.create()

        with self.session:
            r = requests.get(url)

            table  = self.schema.add_table("public_schools")
            table.add_column('id',datatype='integer', is_primary_key=True)

            for tr in soup(r.content).find('table').find_all('tr')[1:]:
                cells = [td.text for td in tr.find_all('td')]

                table.add_column(cells[0].strip(),
                datatype=type_map[cells[1].strip()], 
                size=int(cells[2].strip()), 
                description=cells[3])


    def intuit_schema(self, row, types, lengths):

        if len(types) == 0:
            types = [int] * len(row)
            lengths = [0] * len(row)
            
        for i, v in enumerate(row):
            lengths[i] = max(lengths[i], len(v))
            type_ = types[i]
            
            if type_ is int:
                try:
                    int(v)
                    types[i] = int
                    continue
                except ValueError:
                    types[i] = str

            if False and type_ == float: # There are no floating point numbers in these files. 
                try:
                    float(v)
                    types[i] = float
                    continue
                except ValueError:
                    types[i] = str

        return types, lengths

    def meta_private_school_schemas(self):
        
        from ambry.client.ckan import new_ckan
        import re
        import csv
        from collections import defaultdict
        
        ckan = new_ckan(self.config.config.datarepo('default'))
        package = ckan.get_package(self.config.build.private_schools.source_package)

        years = set()
        fields = defaultdict(set)

        with self.session:
            if not self.database.exists():
                self.database.create()

        # Foreach file listed in the CKAN package ... 
        for r in package['resources']:
            self.log("Processing: {}".format(r['name']))
            m = re.search(r'(\d{4})-(\d{4})', r['name']).groups()
            year = int(m[0])

            file = self.filesystem.download(r['url'])

            self.log("    File: {}".format(file))

            # Read all of the rows and figure out the header, length and types
            with open(file) as f:
                reader = csv.reader(f)
                header = reader.next() # Skip header
                types = []
                lengths = []
                for row in reader:
                    types, lengths  = self.intuit_schema(row, types,lengths)
            
            # Now create schema entries 
            type_map = {int : "integer", float: 'real', str: 'varchar'}

            try:
                with self.session:
                    table_name = 'private_schools_'+str(year)
                    table  = self.schema.add_table(table_name)
                    table.add_column('id',datatype='integer', is_primary_key=True)

                    for i,description in enumerate(header):

                        field = self.transform_field_name(i,description)

                        try:
                            table.add_column(field,datatype=type_map[types[i]], 
                                width=int(lengths[i]), description=description)
                        except:
                            self.error("Failed to add column {}, {}.{}".format(i,table_name, field))
                            self.error("Header: {}".format(header))
                            raise
            except Exception as e:
                self.error("Aborting load for table {}: {}".format(table_name, e))
                continue
                    
    @staticmethod
    def transform_field_name(i, name):
        '''Convert the input field headings into sensible column names'''
        import re
        field = re.sub(r'[^\w\s]','_',name)
        field = re.sub(r'\s+','_', field).lower().strip()

        if not field:
            field = 'field'+str(i)

        return field

    def meta(self):
        self.meta_scrape_table() 
        self.meta_private_school_schemas()

        with open(self.filesystem.path('meta',self.SCHEMA_FILE), 'w') as f:
            self.schema.as_csv(f)
        
        return True

    def build(self):
        self.load_public_schools()
        self.load_private_schools()
        return True

    def load_public_schools(self):
        from unicodecsv import DictReader
        import dateutil.parser
        from ambry.util import lowercase_dict
        
        table_name = 'public_schools'
        
        p = self.partitions.new_partition(table=table_name)
        
        url = self.config.build.public_schools.url

        self.log("Dowloading {}".format(url))
        
        file_name = self.filesystem.download(url)

        self.log("Dowloading {} to {}".format(url, file_name))

        with open(file_name) as f:
            dr = DictReader(f, delimiter='\t', encoding='latin1')

            try: p.query("DELETE FROM {}".format(table_name))
            except: pass
        
            lr = self.init_log_rate(5000,table_name)
            with p.database.inserter(table_name, update_size=True) as ins:
                for i, row in enumerate(dr):  
                    row = lowercase_dict(row)
                    row['id'] = None
                    lr()
                   
                    ins.insert(row)

            self.log("CSVizing {}".format(p.identity))
            p.csvize(logger=self.init_log_rate(5000,'CSVizing'))

        return True

    def load_private_schools(self):
        import yaml
        import re
        import unicodecsv 
        from collections import defaultdict
        from ambry.util import lowercase_dict
        
        ENCODING='latin-1'

        print self.filesystem.path('meta','urls.yaml')

        with open(self.filesystem.path('meta','urls.yaml')) as f:
            urls = yaml.load(f)

        years = set()
        fields = defaultdict(set)


        for r in urls:
            self.log("Loading: {}".format(r['name']))
            m = re.search(r'(\d{4})-(\d{4})', r['name']).groups()
            year = int(m[0])

            file = self.filesystem.download(r['url'])
     
            self.log("    File: {}".format(file))
     
            table_name = 'private_schools_'+str(year)
            p = self.partitions.find_or_new(table = table_name, time=year)
     
            with open(file) as f:
                with p.database.inserter() as ins:
                    
                    reader = unicodecsv.reader(f,encoding=ENCODING )
                    header = reader.next()
                    
                    header = [self.transform_field_name(i,name) for i,name in enumerate(header)]

                    for row in reader:
                        
                        d = dict(zip(header, row))
                        
                        # Error in source file. 
                        if year == 2009 and d['affidavit_id'] == '82580':
                            d['cds_code'] = d['cds_code'].replace(' ','')
                        
                        ins.insert(d)
     

    def combine_fields(self):
        '''Combine and normalized fields'''

    def generate_private_schools_rows(self,file_path):
        
        import xlrd
     
        sheet = xlrd.open_workbook(file_path).sheets()[0]
    
        MIN_HEADER_COUNT = 10
        
        header = None
        in_header=False
        in_body = False
        for i in range(sheet.nrows):
            
            row = [sheet.cell_value(i,j) for j in range(sheet.row_len(i))]

            row_length = len(row)
            
            # Count how many cells aren't empty
            non_nulls = reduce(lambda a, v : a+1 if unicode(v) else a, row, 0)

            # Build a blank header list that is a wide as the table. 
            if header is None:
                header = ['']*row_length

            if in_body:
                yield(dict(zip(header, row)))

            # if we are in one of the multiple header rows, concatenate cell-by-cell
            if MIN_HEADER_COUNT <= non_nulls <= row_length and not in_body:
                in_header = True
                header = [ (unicode(header[i])+ unicode(' '+unicode(row[i]) if row[i] else '')).strip() for i in range(len(row))  ]
            
            # The last line of the header has every cell filled. 
            if in_header and non_nulls == row_length:
                in_body = True
                in_header = False
               