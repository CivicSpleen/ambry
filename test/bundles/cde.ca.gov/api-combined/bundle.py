'''
API Scores, combined by year. 

This bundle will need to be built twice. The first build will have a schema that is
almost entirely varchar datatypes, and there will be  strings in some columns
that are otherwize integers. The build process will analyze the collected data
and create files that will results in an updated schema

    # Pass 1
    dbundle meta --clean # Intial schema, mostly varchars
    dbundle build --clean # Initial build, to collect all data into a single table
    
    # Pass 2
    dbundle meta --clean # Rebuild schema with improved datatypes
    dbundle build --clean # Build, converting strings to integer codes. 
    
'''

from  ambry.bundle import BuildBundle
 

class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
 
 
    def translate_name_desc(self, group, name, desc, year):
        import re
        
        year2d = "{:02d}".format(year % 100)
        yearm1 = year -1
        yearp1 =  year + 1
        yearp12d = "{:02d}".format(yearp1 % 100) # Year plus 1, 2 digits. 
        yearm12d = "{:02d}".format(yearm1 % 100) 

        orig_desc = desc

        desc  = (desc
             .replace('{}-{}'.format(yearm1, year), '[Last year Range]')
             .replace('{}-{}'.format(year, yearp1), '[Next Year Range]')
             .replace('{}-{}'.format(year, yearp12d), '[Next Year Range]')
             .replace('{}'.format(year), '[This Year]')
             .replace('{}'.format(yearp1), '[Next Year]')
             .replace('{}'.format(yearm1), '[Last Year]')
        ) if desc else None


        if group == 'growth' and ( '_ls10' not in name ):

            
            name = (name
                .replace("{}".format(year2d), '_growth')
                .replace("{}".format(yearm12d), '_base')
                ) if name else None     

            
        elif name in ('valid_num','ell'):
            name = 'valid' if name == 'valid_num' else name
            name = 'el' if name == 'ell' else name
        else:

            name = (name
                .replace("api{}b".format(year2d), 'api')
                .replace("api{}".format(year2d), 'api')

                ) if name else None          


        name = re.compile(r'91$').sub('911', name) if name else None 
     
        
        return name, desc
        

    def meta_collect_fields(self,group = 'base'):
        '''Get the schemas from all of the tables in the source, for either the
        'base' or 'growth' sets. Then organize them to determine a complete set of
        fields, and the most recent names for the fields, after making some substitutions 
        in the descriptions to make them comparable. '''
        
        from ambry.identity import Identity, NameQuery
        from collections import defaultdict, OrderedDict
        import unicodecsv as csv
        import sys
               
        self.log("Collecting fields for  {}".format(group))
                
        all_years = set()

        b = self.library.dep('orig')

        for p in b.partitions:
            bp = self.library.get(p.identity.vid)
            year = int(bp.partition.identity.time)
            all_years.add(year)
            
        all_years = list(sorted(all_years))

        sch = defaultdict(set)

        last_desc = OrderedDict()

        for year in all_years: # Get by year so most recent description is lasy
          
            p = b.partitions.find(grain = group, time = year,
                                     table=NameQuery.ANY)
            
            if not p:
                self.log("No partition for grain {}, time {}"
                            .format(group, year))
                continue
            
            bp = self.library.get(p.identity.vid)

            year = int(bp.partition.identity.time)

            for c in bp.partition.table.columns:
                name, desc = self.translate_name_desc(group, c.name,
                                                     c.description, year)
                sch[(name,desc)].add(year)
                last_desc[name] = desc

                
        rows = []
        for (name, desc), years in sch.items():
            padded_years = ['X' if year in years else '' for year in all_years ]

            rows.append([name, desc] + padded_years)

        rows = sorted(rows, key = lambda x: x[0])

        with open(self.filesystem.path('meta','all_fields_by_year_{}.csv'
                                        .format(group)), 'w') as f:
            writer = csv.writer(f, encoding='utf-8')
            writer.writerow(['name', 'description']+all_years)
            writer.writerows(rows)

        with open(self.filesystem.path('meta','most_recent_fields_{}.csv'
                                        .format(group)), 'w') as f:
            writer = csv.writer(f, encoding='utf-8')
            writer.writerow(['name', 'description'])
            
            for name, desc in last_desc.items():
                writer.writerow([name, desc])

    def meta_create_schema(self, group='base'):
        '''Build the initial schema. The field layout from the CDE website 
        has every  field listed as a character, so we will have to intuit 
        the schems from the data. '''
        import unicodecsv as csv

        self.log("Creating schema for {}".format(group))

        self.database.create()

        try:
            table_name = 'api_'+group
            datatypes = self.filesystem.read_csv(
            self.config.build.datatypes_file.format(table_name), key='column')
            
            code_set = self.filesystem.read_yaml(
            self.config.build.codes_file.format(table_name))
            
        except IOError:
            # For the first run, when the field analysis hasn't yet been done. 
            from collections import defaultdict
            datatypes = defaultdict(lambda:{'type':'varchar'})
            code_set = defaultdict(lambda:[])
            
        with self.session as s:
            table = self.schema.add_table('api_{}'.format(group))

            with open(self.filesystem.path('meta',
                        'most_recent_fields_{}.csv'.format(group))) as f:
                reader = csv.DictReader(f)
        
                for row in reader:
                    
                    if row['name'] == 'id':
                        pk = True
                    else:
                        pk = False
                    
                    datatype = datatypes[row['name']]['type']
     
                    c = self.schema.add_column(table, row['name'],
                        description=row['description'],
                        datatype=datatype if not pk else 'integer',
                        is_primary_key = pk,
                        data = {'codes':','.join(code_set[row['name']]) 
                                if row['name'] in code_set else None}
                    )
                    
                    # Add extra columns after the id
                    if pk:
                        self.schema.add_column(table, 'year', 
                        description='School year , from original dataset',
                        datatype = 'integer') 
        
                        
        with open(self.filesystem.path('meta',self.SCHEMA_FILE), 'w') as f:
            self.schema.as_csv(f)

    def meta_find_distinct(self, table='api_base'):
        '''Get the disctinct values from each field and find the most 
        common data type, filtering out the strongs that are actually 
        codes in anotherwise integer field '''
        from collections import defaultdict
        import csv
        import os
    
        self.log("Find distinct values for {} ".format(table))
    
        path = self.filesystem.path(self.config.build.codes_file.format(table))
        if os.path.exists(path):
            self.log("{} exists, not rebuilding codes file".format(path))
        
        
        p = self.partitions.find(table=table)
        
        d = defaultdict(lambda :{ float: 0, int : 0, str : 0 })
        field_codes = defaultdict(set)

        lr = self.init_log_rate()
        
        self.log('---- {}'.format(p.identity.vname))
        
        #
        # Count the numebr of entries of each datatypes, and collect all of the
        # strings that are not in one of the name columns. 
        #
        for c in p.table.columns:
            for row in p.database.query("SELECT distinct({}) from {}"
                                        .format(c.name, p.table.name)):
            
                v = row[0]
                while v and True:

                    try:
                        int(v)
                        type_ = int
                        break
                    except:
                        pass
        
                    try:
                        float(v)
                        type_ = float
                        break
                    except:
                        pass            

                    type_ = str
                    break
            
                d[c.name][type_] += 1
            
                # Exclude some fields from the codes conversion
                if (c.name not in ('sname', 'dname','cname') # Definitely names
                        and c.name not in ('rtype','stype','sped',
                            'charter', 'irg5', 
                            'size') # Codes, but don't convert them. 
                        and not c.name.endswith('_sig') # These should be yes/no
                        and v is not None and type_ == str):
 
                    field_codes[c.name].add(v.strip().lower())
                
                lr(c.name)
                
        # Find the most common datatype for the field. 
        with open(self.filesystem.path(
            self.config.build.datatypes_file.format(table)),'w') as f:
            
            type_map = {float: 'real',str:'varchar', int : 'integer'}
            writer = csv.writer(f)
            writer.writerow(['column', 'type'])
            
            for cname, types in d.items():
            
                max_count = 0
                max_type = None

                for type_, count in types.items():
                    if count > max_count:
                        max_type = type_
                        max_count = count
                    
                writer.writerow([cname, type_map[max_type]])

        self.filesystem.write_yaml({k:list(v) for k,v in field_codes.items()},
                self.config.build.codes_file.format(table))


    def meta(self):
        import os
        
        for group in ('base', 'growth'):

            table_name = 'api_'+group

            path = self.filesystem.path(
                self.config.build.codes_file.format(table_name))
            if os.path.exists(path):
                os.remove(path)
                os.remove(self.config.build.datatypes_file.format('api_'+group))

            # This step transforms field names and descriptions so they 
            # are comparble across years. 
            self.meta_collect_fields(group=group)
            
            # Rebuild the schema with the new information about data types. 
            self.meta_create_schema(group=group)
                  
            
        for group in ('base', 'growth'):     
            
            table_name = 'api_'+group
            
            # Do an initial run of the data with a simple schema, so we
            # can process the values
            self.build_load_values(group=group)
            
            # Collect code values and intuit new schema data types. 
            self.meta_find_distinct(table=table_name)


        self.schema.clean()
            
        for group in ('base', 'growth'): 
            
            # Make the final schema.     
            self.meta_create_schema(group=group)

            
        return True

    def build(self):
        
        for group in ('base', 'growth'):
            self.build_load_values(group=group)

        return True

    def build_load_values(self, group='base'):
        from ambry.identity import Identity,NameQuery
        from collections import defaultdict
        
        b = self.library.dep('orig')

        lr = self.init_log_rate()
 
        table_name = 'api_{}'.format(group)
        table = self.schema.table(table_name)
        out_p = self.partitions.find_or_new(table=table_name)
      
        #
        # Create a converter function that will apply string->integer codes, 
        # if the codes  file was created on a previous build, and map 
        # names from the name map. 
        #
        try:
            code_set = self.filesystem.read_yaml(
                self.config.build.codes_file.format(table_name))
            codes = self.config.build.field_codes
            coded_fields = [ str(x) for x in code_set.keys() ]
            
            _converters = {}
            
            coded_fields_used = []
            for c in table.columns:
                if str(c.name) in coded_fields:
                    _converters[c.name] = (lambda x: codes[str(x).lower()] 
                                        if str(x).lower() in codes else x)
                    coded_fields_used.append(c.name)
                elif str(c.name).endswith('_sig'):
                    # Should all be yes and no, but there is mixed case across files. 
                    _converters[c.name] = lambda x: str(x).title()
                else:
                    _converters[c.name] = lambda x: x
            
                
            

                code_converter = (lambda row, name_map: 
                    { name_map[k]:_converters[name_map[k]](v) 
                    for k,v in row.items() if k not in ('id', 'year') })

            self.log("Using code/name converter, for fields: {}"
                    .format(coded_fields_used))
            
        except IOError as e:

            code_converter = (lambda row, name_map: { name_map[k]:v 
                        for k,v in row.items() if k not in ('id', 'year') })
            
            self.log("Using only name converter")
            

        #
        # THe name map applies the same name translations that were used in 
        # creating the schema. 
        #
        def make_name_map(row):
            name_map = {}
            for k,v in row.items():
                name, _ = self.translate_name_desc(group, k, None, year)
                name_map[k] = name 
                
            return name_map           
 
            
        if out_p.database.exists():
            out_p.database.query("DELETE FROM {}".format(table_name))
                

        for p in  b.partitions.find_all(grain = group, 
                                        time = NameQuery.ANY, 
                                        table = NameQuery.ANY):
            year = int(p.identity.time)
            
            name_map = None

            with out_p.database.inserter() as ins:
                p = p.get() # Get from library, if the partition isn't local
                for row in p.database.query(
                            "SELECT * FROM {}".format(p.table.name)):

                    lr("Copy rows from {}".format(p.identity.vname))

                    if not name_map:
                        name_map = make_name_map(row)

                    row = code_converter(row,name_map)

                    row['year'] = year
                    
                    ins.insert(row)
                    
        return True
