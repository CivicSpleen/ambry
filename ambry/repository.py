"""Interface to the CKAN data repository, for uploading bundle records and data extracts. 

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
from ambry.dbexceptions import ConfigurationError
import petl.fluent as petlf

class Repository(object):
    '''Interface to the CKAN data repository, for uploading bundle records and 
    data extracts. classdocs
    '''

    def __init__(self, bundle, repo_name='default'):
        '''Create a new repository interface
        '''  
        import ambry.client.ckan
        import time, datetime

        self.bundle = bundle 
        self.extracts = self.bundle.config.group('extracts')
        self.partitions = self.bundle.partitions   
        self.repo_name = repo_name
        self._api = None
        self.filestore = None
   
    @property
    def remote(self):
        if not self._api:
            self.set_api()
            
        return self._api

    def set_api(self): 
        import ambry.client.ckan
        repo_group = self.bundle.config.group('datarepo')
        
        if not repo_group.get(self.repo_name):
            raise ConfigurationError("'repository' group in configure either nonexistent"+
                                     " or missing {} sub-group ".format(self.repo_name))
        
        repo_config = repo_group.get(self.repo_name)
        
        self._api =  ambry.client.ckan.Ckan( repo_config.url, repo_config.key)
        
        # Look for an S3 filestore
        
        fs_config = repo_config.get('filestore', False)
        
        if fs_config is not False:
            raise Exception("Deprecated?")
        else:
            self.filestore = None
        
        
        return self.remote
        
   
    def _validate_for_expr(self, astr,debug=False):
        """Check that an expression is save to evaluate"""
        import os
        import ast
        try: tree=ast.parse(astr)
        except SyntaxError: raise ValueError(
                    "Could not parse code expression : \"{}\" ".format(astr)+
                    " ")
        for node in ast.walk(tree):
            if isinstance(node,(ast.Module,
                                ast.Expr,
                                ast.Dict,
                                ast.Str,
                                ast.Attribute,
                                ast.Num,
                                ast.Name,
                                ast.Load,
                                ast.BinOp,
                                ast.Compare,
                                ast.Eq,
                                ast.Import,
                                ast.alias,
                                ast.Call
                                )): 
                continue
            if (isinstance(node,ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == 'datetime'): 
                continue
            if debug:
                attrs=[attr for attr in dir(node) if not attr.startswith('__')]
                print(node)
                for attrname in attrs:
                    print('    {k} ==> {v}'.format(k=attrname,v=getattr(node,attrname)))
            raise ValueError("Bad node {} in {}. This code is not allowed to execute".format(node,astr))
        return True

    def _do_extract(self, extract_data, force=False):
        import os # For the eval @UnusedImport

        done_if = extract_data.get('done_if',False)
 
        if not force and done_if and self._validate_for_expr(done_if, True):
            path =  extract_data['path'] # for eval
            if eval(done_if): 
                self.bundle.log("For extract {}, done_if ( {} ) evaluated true"
                         .format(extract_data['_name'], done_if)) 
                return extract_data['path']

        if extract_data.get('function',False):
            file_ = self._do_function_extract(extract_data)
        if extract_data.get('file',False):
            file_ = self._do_copy_file(extract_data) 
        else:
            file_ = self._do_query_extract(extract_data)


        return file_
        
    def _do_function_extract(self, extract_data):
        '''Run a function on the build that produces a file to upload'''
        import os.path
        
        f_name = extract_data['function']
        
        f = getattr(self.bundle, f_name)
    
        file_ = f(extract_data)        

        return file_

    def _do_copy_file(self, extract_data):
        '''Run a function on the build that produces a file to upload'''
        import os.path
        import pkgutil

        f_name = extract_data['file']
   
        if extract_data['name'].endswith('.html') and f_name.endswith('.md'):
            import markdown
            
            self.bundle.log("Translate Markdown to HTML")
            
            with open(self.bundle.filesystem.path(f_name)) as f:
                html_body = markdown.markdown(f.read())

            template = pkgutil.get_data('ambry.support','extract_template.html')

            out_file = self.bundle.filesystem.path('extracts',extract_data['name'])

            with open(out_file, 'wb') as f:
                
                html_body = html_body.format(**dict(self.bundle.config.about))
                
                html  = str(template).format(
                                             body=html_body,
                                             **dict(self.bundle.config.about))
                f.write(html)
               
            return out_file
                
        else:
   
            return self.bundle.filesystem.path(f_name)

                           
    def _do_query_extract(self,  extract_data):
        """Extract a CSV file and  upload it to CKAN"""
        import tempfile
        import uuid
        import os
        import sqlite3
        import unicodecsv as csv

        p = extract_data['_partition'] # Set in _make_partition_dict

        file_name = extract_data.get('name', None)
        
        if file_name:
            file_ = self.bundle.filesystem.path('extracts', file_name)
        else:
            file_ =  os.path.join(tempfile.gettempdir(), str(uuid.uuid4()) )

        if extract_data.get('query', False):
            query = extract_data['query']
        else:

            source_table = extract_data.get('source_table', False)
            
            if not source_table:
                source_table = p.table.name
                
            extract_table = extract_data.get('extract_table', False)
            
            if not extract_table:
                extract_table = source_table
            
            query = self.bundle.schema.extract_query(source_table,extract_table )

            where = extract_data.get('extract_where', False)
            
            if where:
                query = query + " WHERE "+where

        self.bundle.log("Running CSV extract from a query")
        self.bundle.log("    Partition: {}".format(p.name))
        self.bundle.log("    Source table: {}".format(source_table))
        self.bundle.log("    Extract Table: {}".format(extract_table))
        self.bundle.log("    Query: {}".format(query.replace('\n',' ')))
        self.bundle.log("    Name: {}".format(extract_data['name']))        
        self.bundle.log("    Output: {}".format(file_))       

        #self.bundle.log(query)

        conn = sqlite3.connect(p.database.path)

        lr = self.bundle.init_log_rate(100000,"Extract to {}".format(file_name))

        with open(file_, 'w') as f:
            conn.row_factory = sqlite3.Row
            
            try:
                rows = conn.execute(query)
            except:
                print query
                raise
                
                
            first = rows.fetchone()
            
            if not first:
                raise Exception("Got no data from query: {}".format(query))
            
            writer = csv.writer(f)

            writer.writerow(first.keys())
            writer.writerow(tuple(first))
    
            for row in rows:
                lr()
                writer.writerow(tuple(row))

        return file_       
    
    def _send(self, package, extract_data, file_):
        import os
        import mimetypes
        
        _, ext = os.path.splitext(file_)
        mimetypes.init()
        content_type = mimetypes.types_map.get(ext,None)  #@UndefinedVariable
        
        try:
            _,format = content_type.split('/')
        except:
            format = None
        
        name = extract_data.get('name', os.path.basename(file_))

        #
        # If the filestore exists, write to S3 first, the upload the URL
        if self.filestore:
            from ambry.util import md5_for_file
            urlf = self.filestore.public_url_f(public=True)
            path = self.bundle.identity.path+'/'+name

            # Don't upload if  S3 has the file of the same key and md5
            md5 =  md5_for_file(file_)
            if not self.filestore.has(path, md5=md5):
                self.filestore.put(file_, path, metadata={'public':True, 'md5':md5})

            r = self.remote.add_url_resource(package, urlf(path), name,
                    description=extract_data['description'],
                    content_type = content_type, 
                    format=format,
                    hash=md5,
                    rel_path=path
                    )
        else:
            r = self.remote.add_file_resource(package, file_, 
                                name=name,
                                description=extract_data['description'],
                                content_type = content_type, 
                                format=format
                                )
        
        return r
        
    def _make_partition_dict(self, p):
        '''Return a dict that includes the fields from the extract expanded for
        the values of each and the partition'''
        
        qd = {
            'p_id' : p.identity.id_,
            'p_name' : p.identity.name,
         }
        
        try:
            # Bundles don't have these      
            qd_part = {
                'p_table' : p.identity.table,
                'p_space' : p.identity.space,
                'p_time' : p.identity.time,
                'p_grain' : p.identity.grain,              
                }
        except:
            qd_part = {'p_table' : '','p_space' : '', 'p_time' :'','p_grain' : ''}
            
        qd =  dict(qd.items()+ qd_part.items())
        qd['_partition'] = p

        return qd
    
    def _expand_each(self, each):
        '''Generate a set of dicts from the cross product of each of the
        arrays of 'each' group'''
        
        # Normalize the each group, particular for the case where there is only
        # one dimension
  
        if not isinstance(each, list):
            raise ConfigurationError("The 'each' key must have a list. Got a {} ".format(type(each)))
        
        elif len(each) == 0:
            each = [[{}]]
        if not isinstance(each[0], list):
            each = [each]
        

        # Now the top level arrays of each are dimensions, and we can do a 
        # multi dimensional iteration over them. 
        # This is essentially a cross-product, where out <- out X dim(i)

        out = []
        for i,dim in enumerate(each):
            if i == 0:
                out = dim
            else:
                o2 = []
                for i in dim:
                    for j in out:
                        o2.append(dict(i.items()+j.items()))
                out = o2

        return out
        

        
    def _expand_partitions(self, partition_name='any', for_=None):
        '''Generate a list of partitions to apply the extract process to. '''

        if partition_name == 'bundle':
            partitions = [self.bundle]
        elif partition_name == 'any':
            partitions = [p for p in self.partitions]
            partitions = [self.bundle] + partitions
        else:
            partition = self.partitions.get(partition_name)
            
            if partition:
                partitions = [partition]
            else:
                raise Exception("Didn't get a partition for name: {}".format(partition_name))

        out = []
         
        if not for_:
            for_ = 'True'
         
        for partition in partitions:
         
            try:
                self.bundle.log("Testing: {} ".format(partition.identity.name))
                if self._validate_for_expr(for_, True):
                    if eval(for_):  
                        out.append(partition)
            except Exception as e:
                self.bundle.error("Error in evaluting for '{}' : {} ".format(for_, e))
          
        return out
         
    def _sub(self, data):
        import datetime
        
        if data.get('aa', False):
            from ambry.geo.analysisarea import get_analysis_area

            aa = get_analysis_area(self.bundle.library, **data['aa'])    
        
            aa_d  = dict(aa.__dict__)
            aa_d['aa_name'] = aa_d['name']
            del  aa_d['name']
            
            data = dict(data.items() + aa_d.items())

        data['bundle_name'] = self.bundle.identity.name
        data['date'] = datetime.datetime.now().date().isoformat()

        data['query'] = data.get('query','').format(**data)
        data['extract_where'] = data.get('extract_where','').format(**data)
        data['title'] = data.get('title','').format(**data)
        data['description'] = data.get('description','').format(**data)
        data['name'] = data.get('name','').format(**data)
        data['layer_name'] = data.get('layer_name','').format(**data)
        data['path'] = self.bundle.filesystem.path('extracts',format(data['name']))
        data['done_if'] = data.get('done_if','os.path.exists(path)').format(**data)
        
  
        return data

    
    def dep_tree(self, root):
        """Return the tree of dependencies rooted in the given node name, 
        excluding all other nodes"""
        
        graph = {}
        for key,extract in self.extracts.items():
            graph[key] = set(extract.get('depends',[]))
            
        def _recurse(node):
            l = set([node])
            for n in graph[node]:
                l = l | _recurse(n)
            
            return l
            
        return  _recurse(root)
            
            
    def generate_extracts(self, root=None):
        """Generate dicts that have the data for an extract, along with the 
        partition, query, title and description
        
        :param root: The name of an extract group to use as the root of
        the dependency tree
        :type root: string
        
        If `root` is specified, it is a name of an extract group from the configuration,
        and the only extracts performed will be the named extracts and any of its
        dependencies. 
    
         """
        import collections
        from ambry.util import toposort

        
        ext_config = self.extracts

        # Order the extracts to satisfy dependencies. 
        graph = {}
        for key,extract in ext_config.items():
            graph[key] = set(extract.get('depends',[]))
     

        if graph:
            exec_list = []
            for group in toposort(graph):
                exec_list.extend(group)
        else:
            exec_list = ext_config.keys()
            
        if root:
            deps = self.dep_tree(root)
            exec_list = [ n for n in exec_list if n in deps]
         
       
        # now can iterate over the list. 
        for key in exec_list:
            extract = ext_config[key]
            extract['_name'] = key
            for_ = extract.get('for', "'True'")
            function = extract.get('function', False)
            file_ = extract.get('file', False)
            each = extract.get('each', [])
            p_id = extract.get('partition', False)
            eaches = self._expand_each(each)
  
  
            # This part is a awful hack and should be refactored
            if function:
                for data in eaches:  
                    yield self._sub(dict(extract.items() + data.items()))

            elif p_id:       
                partitions = self._expand_partitions(p_id, for_)
    
                for partition in partitions:
                    p_dict = self._make_partition_dict(partition)
                    for data in eaches:     
                        yield self._sub(dict(p_dict.items()+extract.items() + 
                                             data.items() ))
            elif file_:
                for data in eaches:
                    yield self._sub(dict(extract.items() + data.items()))
            else:
                self.bundle.error("Extract group {} should have either a function or a partition".format(key))
              
    def store_document(self, package, config):
        import re, string

        id =  re.sub('[\W_]+', '-',config['title'])
        
        r = self.remote.add_url_resource(package, 
                                        config['url'], 
                                        config['title'],
                                        description=config['description'])
        
        return r
    

    def zip(self, zips):
        import os
        from util import zipdir

        outs = {}
        
        for z in zips:
            
            if os.path.isdir(z):
                
                out = z+'.zip'
                print "zip {} to {}".format(z, out)
                zipdir(z, out)
                outs[z] = out
            elif os.path.isfile(z):
                print "zip file"
                pass
            else:
                raise Exception("Can't zip: '{}' ".format(z))
                       
        return outs
          
    def extract(self, root=None, force=False):
        import os

        zips = set()
        
        for extract_data in self.generate_extracts(root=root):
            
            zip = extract_data.get('zip', False)
            if zip == 'dir':
                zips.add(os.path.dirname(extract_data['path']))
            elif zip == 'file':
                zips.add(extract_data['path'])
            
            file_ = self._do_extract(extract_data, force=force)
            if file_ is True:
                self.bundle.log("Extract {} marked as done".format(extract_data['_name']))
            elif file_ and os.path.exists(file_):
                self.bundle.log("Extracted: {}".format(file_))
            else:
                self.bundle.error("Extracted file {} does not exist".format(file_))
       
        self.zip(zips)
       
        return True
 
                    
    def submit(self,  root=None, force=False, repo=None): 
        """Create a dataset for the bundle, then add a resource for each of the
        extracts listed in the bundle.yaml file"""
        import ambry.util as du
        
        if repo:
            self.repo_name = repo
            self.set_api()
        
        import os
        from os.path import  basename
    
        ckb = self.remote.update_or_new_bundle_extract(self.bundle)
        
        sent = set()
    
        self.remote.put_package(ckb)
        
        for doc in self.bundle.config.group('about').get('documents',[]):
            self.store_document(ckb, doc)

        zip_inputs = {}

        for extract_data in self.generate_extracts(root=root):

            zip = extract_data.get('zip', False)
            will_zip = False
            
            if zip == 'dir':
                zip_inputs[os.path.dirname(extract_data['path'])] = extract_data
                will_zip = True
            elif zip == 'file':
                zip_inputs[extract_data['path']] = extract_data
                will_zip = True

            file_ = self._do_extract(extract_data, force=force)
            
            if will_zip:
                self.bundle.log("{} will get submitted as a zip".format(file_))
            elif file_ not in sent:
                r = self._send(ckb, extract_data,file_)
                sent.add(file_)
                url = r['ckan_url']
                self.bundle.log("Submitted {} to {}".format(basename(file_), url))
            else:
                self.bundle.log("Already processed {}, not sending.".format(basename(file_)))
        
        
        zip_outputs = self.zip(zip_inputs.keys() )
        
        
        print zip_outputs
        
        for in_zf, out_zf in zip_outputs.items():
            extract_data = zip_inputs[in_zf]
            extract_data['name'] = extract_data['zipname'] if 'zipname' in extract_data else extract_data['name']
            r = self._send(ckb, extract_data,out_zf)
        
            url = r['ckan_url']
            self.bundle.log("Submitted {} to {}".format(basename(out_zf), url))
        
        
        return True