# -*- coding: utf-8 -*-
'''

Requirements:
    dbfpy. pip install http://downloads.sourceforge.net/project/dbfpy/dbfpy/2.2.5/dbfpy-2.2.5.tar.gz

'''

from  ambry.bundle import BuildBundle
 

class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
 
    def meta_scrape_urls(self):
        '''Scrape the URLS from the index page and build a YAML file of results. '''
        from bs4 import BeautifulSoup as soup
        import requests
        import re
        import pprint
        from collections import defaultdict
        import os
 
        url  = self.config.build.index_url
        base_url = os.path.dirname(url)
        
        r = requests.get(url)

        rec = None
        recs = defaultdict(dict)

        for a in soup(r.content).find('div', attrs={'id':'maincontent'}).find_all('a'):
 
            if not a or not a.string:
                continue
 
            abs_url = a['href'] if 'http' in a['href'] else os.path.join(base_url,a['href']) 
 
            if 'reclayout' in a['href']:
                
                
                print "REC",
                if rec:
                    recs[rec['year']][rec['type']] = rec
                    
                rec = {}
                heading = a.find_previous('h6')
                rec['title'] = str(heading.string)
                rec['layout'] = abs_url

            elif 'db.zip' in a['href']:
                print "DBM",
                rec['dbm'] = abs_url

            elif 'tx.zip' in a['href']:
                print "TXT",
                rec['txt'] = abs_url

            elif 'flag' in a['href']:
                print "FLG",
                rec['flags'] = abs_url
            else:
                 print "   ",
 
 
            if 'Base' in a.string:
                print "B", 
                year = re.match(r'(\d{4})', a.string).groups()[0]
                rec['type'] = 'base'
                rec['year'] = int(year)

            elif 'Growth' in a.string:
                print "G",
                rec['type'] = 'growth'
                if re.match(r'(\d\d\d\d)-(\d\d)', a.string):
                    first_year, next_year  = re.match(r'(\d\d\d\d)-(\d\d)', a.string).groups()
                    year = 2000+int(next_year)
                    rec['year'] = int(year)
                    rec['base_year'] = int(first_year)
                else:
                    year = re.match(r'(\d\d\d\d)', a.string).groups()[0]
                    rec['year'] = int(year)
                    rec['base_year'] = int(year) - 1

            else:
                continue
            
            print year, '||',a.string, a['href']
 
        import yaml
        with open(self.filesystem.path(self.config.build.urls_file), 'wb') as f:
            f.write(yaml.dump(dict(recs), indent=4, default_flow_style=False))


    def meta_scrape_table(self, url):
        from bs4 import BeautifulSoup as soup
        import requests

        type_map = {
        'Char':'varchar', 
        'Character':'varchar', 
        'Charter':'varchar', # A spelling error in the source page. 
        'Decimal':'real',
        'Date':'datetime'}

        with self.session:
            self.database.create()

            r = requests.get(url)

            for tr in soup(r.content).find('table').find_all('tr')[1:]:
                cells = [td.text for td in tr.find_all('td')]

                yield {
                    'field': cells[1].strip(),
                    'datatype': type_map[cells[2].strip()], 
                    'size': int(cells[3].strip()), 
                    'description': cells[4].strip()
                }

    def meta_scrape_tables(self):
        '''Scape the schema information from the record web pages for each of the datasets. '''
        import yaml
        from collections import defaultdict
        import re

        with open(self.filesystem.path(self.config.build.urls_file)) as f:
            urls = yaml.load(f)

        schema = defaultdict(lambda : {'columns': list(), 'title': None, 'year': None})
        
        for s in ('base', 'growth'):
            for k,gbv in urls.items():
                    if s in gbv:
                        url =  gbv[s]['layout']
                        self.log("--- {}, {}".format(gbv[s]['title'], url))
                        year = gbv[s]['year']
                        skey = str(s)+str(year)
                        schema[skey]['title'] = gbv[s]['title']
                        schema[skey]['year'] = int(year)
                        
                        for d in self.meta_scrape_table(url): 
                            schema[skey]['columns'].append(d)
                            
 
        with open(self.filesystem.path(self.config.build.schema_source_file), 'wb') as f:
            f.write(yaml.dump(dict(schema), indent=4, default_flow_style=False))

    def meta_compile_fields(self):
        '''Group fields by year. Not used in this bundle, should be moved to  a later stage bundle. '''
        import yaml
        from collections import defaultdict

        with open(self.filesystem.path(self.config.build.urls_file)) as f:
            urls = yaml.load(f)

        for s in ('base', 'growth'):
            fields = defaultdict(lambda : {'years':set(), 'size': 0, 'datatypes': set(), 'descriptions':set()})

            for k,gbv in urls.items():
                    if s in gbv:
                        url =  gbv[s]['layout']
                        self.log("--- {}, {}".format(gbv[s]['title'], url))
                   
                        fn = str(d['field'])
                        fields[fn]['years'].add(gbv[s]['year'])
                        fields[fn]['datatypes'].add(d['datatype'])
                        fields[fn]['size'] = max(fields[d['field']]['size'] , d['size'])
                        fields[fn]['descriptions'].add( d['description'])

            with open(self.filesystem.path(self.config.build.schema_source_file), 'wb') as f:
                f.write(yaml.dump(dict(fields), indent=4, default_flow_style=False))
        
    def meta_create_schema(self):
        '''Use the yaml schema file to generate a CSV schema file.'''
        import re
        import yaml 
        
        with open(self.filesystem.path(self.config.build.schema_source_file)) as f:
            yschema = yaml.load(f)
     
        with self.session:
            
            if not self.database.exists():
                self.database.create()
     
            for table_name, data in yschema.items():
                table  = self.schema.add_table(table_name)
                table.add_column('id',datatype='integer', is_primary_key=True)
                
                year = data['year']
                columns = data['columns']
                self.log("Loading schema for: {}".format(data['title']))
                for c in columns:
                    c['description'] = (
                        re.sub(r'\s+', ' ', c['description'])
                        .replace(u'\u2013','-')
                        .encode('ascii') )
                    c['width'] = c['size']

                    table.add_column(c['field'].lower(), **c)
                    
        sf_out = self.filesystem.path('meta',self.SCHEMA_FILE)

        with open(sf_out, 'w') as f:
            self.schema.as_csv(f)

    def meta(self):
        self.meta_scrape_urls()
        self.meta_scrape_tables()
        self.meta_create_schema()
        return True
 

    def build_fixed(self):
        '''Load each of the growth and base datasets into their own tables. '''
        import csv
        import yaml
        import re
        
        with open(self.filesystem.path(self.config.build.urls_file)) as f:
            urls = yaml.load(f)
            
        for year, datasets in urls.items():
            for ds_name, data in datasets.items():
                
                #
                # Download and unzip each URL. These two calls will cache the download
                # and the unzipped versions, so they will run much faster the second times. 
                #
                zip_file = self.filesystem.download(data['txt'])
                data_file = self.filesystem.unzip(zip_file)
          
                self.log("Read: {}".format(data_file))

                # Construct the table name and create a partition. The partitions are given
                # time and grain prameters to make them easier to locate in the set of partitions
                table = str(ds_name)+str(year)
                p = self.partitions.find_or_new(table = table, time=year, grain = ds_name)

                #if p.identity.time != str(2010):
                #    continue

                # These are fixed-field files, so we have to unpack them. 
                unpack_f, header, unpack_str, length = p.get_table().get_fixed_unpack()

                self.log("Loading: {}".format(p.identity.vname))
                with open(data_file) as f:
                    with p.database.inserter() as ins:
                        for line in f:
                            
                            # Some of the lines are short, sp this pads them out to the correct length. 
                            #
                            # WARNING! This may be a symptom of a coruption in the data. 
                            # The ACS_46 column  for the 2009 through 2012 files seems to be overlaid with the
                            # SCI column.  So, for a record  where  in the DBF format  ACS_46 is 'N/A' and SCI
                            # is '155.9600864', in the TXT format, ACS_46 is 'N155.' In the text file, the 
                            # whole  SCI value will appear, not just the first few characters, so I 
                            # suspect there is more widespread file corruption. 
                            #
                            
                            if len(line) < length:
                                pad = length-len(line)
                                line += ' '*pad
                            
                            row = dict(zip(header, unpack_f(line[:length])))
                            
                            # This name entry is screwed up in a lot of the fields, either missing
                            # the tiilde or using a characer I can't figure out the encoding of. 
                            if re.match('Do.* Merced Elementary',row['sname']):
                                #self.log(u"Substitute '{}' -> '{}'".format(row['sname'], repl))
                                row['sname'] = u'Doña Merced Elementary'
                                

                            ins.insert(row)

        return True


    def build_dbf(self):
        import yaml
        import re
        
        from dbfpy import dbf
        with open(self.filesystem.path(self.config.build.urls_file)) as f:
            urls = yaml.load(f)
            
        lr = self.init_log_rate()
            
        for year, datasets in urls.items():
            for ds_name, data in datasets.items():
                
                url = data['dbm']
                
                zip_file = self.filesystem.download(url)
                data_file = self.filesystem.unzip(zip_file)
          
                self.log("Read: {}".format(data_file))
                
                # Construct the table name and create a partition. The partitions are given
                # time and grain prameters to make them easier to locate in the set of partitions
                table = str(ds_name)+str(year)
                p = self.partitions.find_or_new(table = table, time=year, grain = ds_name)
                
                
                in_db = dbf.Dbf(data_file)
                
                header = [ field.name.lower() for field in in_db.header.fields]
                
                with p.database.inserter() as ins:
                    for rec in in_db:
                        row =  dict(zip(header,rec.fieldData))
                        lr()

                        # This name entry is screwed up in a lot of the fields, either missing
                        # the tiilde or using a characer I can't figure out the encoding of. 
                        if re.match('Do.* Merced Elementary',row['sname']):
                            #self.log(u"Substitute '{}' -> '{}'".format(row['sname'], repl))
                            row['sname'] = u'Doña Merced Elementary'

                        ins.insert(row)
            
    def build(self):
        
        self.build_dbf()
        return True
        
                      