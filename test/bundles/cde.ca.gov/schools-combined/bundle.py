'''
'''

from  ambry.bundle import BuildBundle
 

class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
 


    def get_name_map(self):
        import csv
        
        file_name = self.filesystem.path(self.config.build.name_transforms)
        t_map = {}
        fields = []
        with open(file_name) as f:
            reader = csv.DictReader(f)
           
            for row in reader:
                t_map[row['from_name']] = row['to_name']
                fields.append(row['from_name'])

        return t_map, fields

   

    def meta_compile_column_names(self):
        '''Collect all of the column names from private school schemas and, along with the
        name map, produce a new name transformation spreadheet that can be used both for creating a schema
        and for updating the name transformations. '''
        from collections import defaultdict, OrderedDict
        import csv
        
        b = self.library.dep('schools')
        
        fields = defaultdict(set)
        lengths = defaultdict(int)
        all_years = set()
        
        for table in b.schema.tables:
            if 'private' not in table.name:
                continue
                
            _,_,year = table.name.split('_')
            year = int(year)
            all_years.add(year)
            
            for c in table.columns:
                fields[c.name].add(year)
                lengths[c.name] = c.size
            
        all_years = list(sorted(all_years))
        rows = []
        
        # Critically, field_names is in the order that the
        # fields should appear in the final schema, although it also
        # containes the names of fields that will be mapped to other names
        # and won't appear in the final schema. 
        name_map, field_names = self.get_name_map()
        
        for i,field_name in enumerate(field_names):
            
            years =  list(all_years)
            for j,year in enumerate(all_years):
            
                if year in fields[field_name]:
                    years[j] = 'X'
                else:
                    years[j] = ''
                
            c = len(fields[field_name])
            
            tf = name_map[field_name]
            lengths[tf] = max(lengths[tf], lengths[field_name])

            rows.append([i,tf,field_name,lengths[tf], c]+years)

        # now make one more pass to ensure that all of the lengths are set. 
        for i in range(len(rows)):
            rows[i][3] = max(lengths[tf], rows[i][3])

        with open(self.filesystem.path(self.config.build.revised_name_transforms),'wb') as f:
            writer = csv.writer(f)
            writer.writerow(['i','to_name','from_name','size','years']+all_years)
            for row in rows:
                writer.writerow(row)

    def meta_create_private_school_schema(self):
        import csv
        from collections import OrderedDict
        
        if not self.database.exists():
            self.database.create()

        file_name = self.filesystem.path(self.config.build.name_transforms)

        with open(file_name) as f:
            reader = csv.reader(f) # Don't use DictReader -- need to preserve ordering. 
            header = reader.next()

            fields = OrderedDict()
            for row in reader:
                row = dict(zip(header, row))
                fields[row['to_name']] = row['size']

        table_name = "private_schools"
        
        with self.session:
            table  = self.schema.add_table(table_name)
            table.add_column('id',datatype='integer', is_primary_key=True)
  
            for field, size in fields.items():
                table.add_column( field,datatype='varchar',width=int(size), description='')

    def meta_create_public_school_schema(self):
        import csv
        from collections import OrderedDict
        

        file_name = self.filesystem.path(self.config.build.name_transforms)

        with open(file_name) as f:
            reader = csv.reader(f) # Don't use DictReader -- need to preserve ordering. 
            header = reader.next()

            fields = OrderedDict()
            for row in reader:
                row = dict(zip(header, row))
                fields[row['to_name']] = row['size']

        table_name = "public_schools"
        
        with self.session:
            table  = self.schema.add_table(table_name)
            table.add_column('id',datatype='integer', is_primary_key=True)
  
            for field, size in fields.items():
                table.add_column( field,datatype='varchar',width=int(size), description='')


    def meta(self):
        
        # Need to create the database to create the schema
        if not self.database.exists():
            self.database.create()

        self.meta_compile_column_names()
        
        self.meta_create_private_school_schema()

        b = self.library.dep('schools')
        
        self.schema.copy_table(b.schema.table('public_schools'))

        with open(self.filesystem.path('meta',self.SCHEMA_FILE), 'w') as f:
            self.schema.as_csv(f)
            
            
        return True

    def build_import_private_schools(self):
        import pprint
        
        b = self.library.dep('schools')

        lr = self.init_log_rate(1000)
        for orig_p in b.partitions:
            
            if not orig_p.identity.time:
                continue
         
            orig_p.get() # Fetch from library if it does not exist. 
            
            if not 'private' in orig_p.table.name:
                continue

            p = self.partitions.find_or_new_db(table='private_schools')

            year = int(orig_p.identity.time)


            with p.database.inserter() as ins:
                for i, orig_row in enumerate(orig_p.database.query(
                            "SELECT * FROM {}".format(orig_p.table.name))):
                    orig_row = dict(orig_row)
                    
                    del orig_row['id']

                    name_map, field_names = self.get_name_map()

                    row = { name_map[k]:v for k,v in orig_row.items() }

                    row['year'] = year
                    row['id'] = None
                    
                    #pprint.pprint(row)
                    row['name'] = row['name'].title().replace("'S","'s") if row['name'] else None
                    row['street'] = row['street'].title() if row['street'] else None
                    row['city'] = row['city'].title() if row['city'] else None
                    row['county'] = row['county'].title() if row['county'] else None
                    row['district'] = row['district'].title() if row['district'] else None
                    
                    #print "==== {:30s} {:30s}".format(row['name'], orig_row['school_name'])    
                    #for k,v in orig_row.items():
                    #    print "   {:50s}->{:30s} {}".format(k,name_map[k],v)
                                     
                    lr(orig_p.identity.vname)
                 
                    try:
                        ins.insert(row)
                    except:
                        self.error("Failed for row {}: {}".format(i,row))
                        continue
                          
        return True


    def build_import_public_schools(self):
        
        b = self.library.dep('schools')
        
        in_p = b.partitions.find_or_new_db(table='public_schools', format='db')
        
        out_p = self.partitions.find_or_new_db(table='public_schools')
        
        lr = self.init_log_rate(1000, "copy public schools")
        
        with out_p.database.inserter(cache_size=1) as ins:
            for row in in_p.database.query("SELECT * from public_schools"):
                
                ins.insert(row)
                lr()


    def build_update_schema(self):
        '''Run after the build process to intuit types and sizes and update the schema.csv file.
        Review the file after running, because it may need some adjustment'''

        self.schema.update('private_schools', 
                self.partitions.find(table='private_schools').database.query("SELECT * from private_schools"), 
                logger=self.init_log_rate(10000,'Update private_schoools schema'))

        self.schema.update('public_schools', 
                self.partitions.find(table='public_schools').database.query("SELECT * from public_schools"), 
                logger=self.init_log_rate(10000,'Update public_schools schema'))

    def build(self):
        
        self.build_import_private_schools()
        self.build_import_public_schools()
        self.build_update_schema()
        
        return True
        
        
        
        
        

