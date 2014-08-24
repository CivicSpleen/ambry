"""
Class for handling manifest files.
"""

# Copyright (c) 2014 Clarinova. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt

import os.path
import re
import sqlparse
import markdown
from ..util import memoize
from pygments import highlight
from pygments.lexers import PythonLexer
from pygments.formatters import HtmlFormatter

class null_logger(object):

    def error(self, w):
        pass


def new_manifest(ref, logger, library, base_dir, force=False, install_db = False):
    return Manifest(ref, logger, library = library, base_dir = base_dir, force=force, install_db = install_db)

class ParseError(Exception):
    pass

class ManifestSection(object):

    def __init__(self, tag, linenumber, args):
        self.linenumber = linenumber
        self.args = args
        self.tag = tag

        self.lines = []
        self.content = None
        self.html = None


class Manifest(object):

    # These tags have only a single line; revert back to 'doc' afterward
    singles = ['uid', 'title', 'extract', 'dir', 'destination', 'database', 'publish', 'author', 'url', 'access']


    def __init__(self, file_or_data, logger=None, library=None, base_dir=None,
                 pub_dest=None, force=False, install_db=False):

        from ..dbexceptions import ConfigurationError

        self.library = library
        self.base_dir = base_dir
        self.force = force
        self._install_db = install_db

        self.logger = logger if logger else null_logger()
        self.last_line = 0
        self.sections = {}

        self.file, self.data = self._extract_file_data(file_or_data)

        if self.data:
            self.sectionalize(self.data)

        if not self.base_dir:
            raise ConfigurationError("Must specify a base dir")

        self.file_installs = set()
        self.installed_partitions = list()

        self._warehouse = None

    def _extract_file_data(self, file_or_data):
        if file_or_data.startswith('http'):  # A URL
            import requests

            r = requests.get(file_or_data)
            r.raise_for_status()

            file = None

            data = r.text.splitlines()

        elif os.path.exists(file_or_data):  # A file
            with open(file_or_data, 'r') as f:
                data = f.readlines()
                file = file_or_data

        else:  # String data

            if isinstance(file_or_data, list):
                data = file_or_data
            else:
                data = file_or_data.splitlines()
            file = None

        return file, data


    @property
    def abs_work_dir(self):
        from ..dbexceptions import  ConfigurationError

        if not os.path.isdir(self.base_dir):
            os.makedirs(self.base_dir)

        work_dir = self.work_dir if self.work_dir else self.uid

        abs_work_dir = os.path.join(self.base_dir, work_dir)

        if not os.path.isdir(abs_work_dir):
            os.makedirs(abs_work_dir)

        if not self.work_dir or not os.path.isdir(abs_work_dir):
            raise ConfigurationError("Must specify  work dir ")


        return abs_work_dir



    @property
    def install_db(self):
        """Return true if both the install_db flag was set on construction, and the database is installable"""
        if  self._install_db and os.path.exists(self.warehouse.database.path):
            return os.path.basename(self.warehouse.database.path)
        else:
            return False


    @property
    def sorted_sections(self):
        for line in sorted(self.sections.keys()):
            yield (line, self.sections[line])

    def tagged_sections(self, tag):
        for line in sorted(self.sections.keys()):
            if self.sections[line].tag == tag:
                yield (line, self.sections[line])

    def single_line(self, keyword):
        for line, section in self.sections.items():
            if section.tag == keyword:
                return section.args.strip()

        return None

    def count_sections(self,tag):
        return sum( section.tag == tag for section in self.sections.values())

    # Deprecated! Use database
    @property
    def destination(self):
        return self.single_line('database')

    @property
    def database(self):
        return self.single_line('database')

    @property
    def publication(self):
        return self.single_line('publication')

    @property
    def ckan(self):
        return self.single_line('ckan')

    @property
    def work_dir(self):
        return self.single_line('dir')

    @property
    def uid(self):
        from ..dbexceptions import ConfigurationError

        uid =  self.single_line('uid')

        if not uid:
            import uuid
            raise ConfigurationError(
                "Manifest does not have a UID. Add this line to the file:\n\nUID: {}\n".format(uuid.uuid4()))

        return uid

    @property
    def title(self):
        return self.single_line('title')

    @property
    def access(self):
        acl =  self.single_line('access')

        if not acl:
            acl = 'public-read'

        return acl


    @property
    def bundles(self):
        """Metadata for bundles, each with the partitions that are installed here. """

        bundles = {}

        for p in self.partitions:
            b_ident = p['bundle']

            if not b_ident.vid in bundles:
                b = self.library.get(b_ident.vid)
                bundles[b_ident.vid] = dict(
                    partitions = [],
                    metadata = b.metadata,
                    ident = b_ident
                )

            bundles[b_ident.vid]['partitions'].append(p)

        return bundles

    @property
    def summary(self):
        """The first doc section"""

        for line, section in self.sorted_sections:

            if section.tag == 'doc':
                return section.content

        return None


    def pygmentize_sql(self,c):
        return  highlight(c, PythonLexer(), HtmlFormatter())

    @property
    def css(self):
        return HtmlFormatter(style='manni').get_style_defs('.highlight')


    def make_item(self, sections, tag, i, args):
        """Creates a new entry in sections, which will later have lines appended to it. """
        line_number = i + 1
        section = ManifestSection(tag=tag, linenumber=line_number, args=args)
        sections[line_number] = section
        return line_number, section

    def sectionalize(self, data, first_line=0):
        """Break the file into sections"""

        import re

        sections = {}

        tag = 'doc'  # Starts in the Doc section
        args = ''
        non_tag_is_doc = False # The line isn't a tag, and we're in a doc section

        section_start_line_number, section =  self.make_item(sections, tag,  first_line, args)

        for i, line in enumerate(data):

            line_number = first_line + i

            line_w_comments = line # A hack to handle '#compress' in cache specification

            if tag != 'doc':
                line = re.sub(r'#.*$','', line ) # Remove comments

            if not line.strip():
                if non_tag_is_doc:
                    non_tag_is_doc = False
                    tag = 'doc'
                    section_start_line_number, section = self.make_item(sections, tag,line_number, None)

                if tag == 'doc': # save newlines for doc sections
                    section.lines.append(line)

                continue

            rx = re.match(r'^(\w+):(.*)$', line.strip())

            if rx: # Section tag lines

                tag = rx.group(1).strip().lower()

                # The '#' is a valid char in publication URLs
                if tag == 'publication' and '#' in line_w_comments:
                    rx = re.match(r'^(\w+):(.*)$', line_w_comments.strip())

                args = rx.group(2).strip()
                section_start_line_number, section = self.make_item(sections, tag, line_number, args)

                if tag in self.singles: # Following a single line tag, the next line revers to DOC
                    non_tag_is_doc = True

                continue

            elif non_tag_is_doc:
                non_tag_is_doc = False
                tag = 'doc'
                section_start_line_number, section = self.make_item(sections, tag, line_number, None)


            section.lines.append(line)

        for line in sections.keys():

            section = sections[line]

            # clear out any empty doc sections. These tend to get created for blank lines.
            if section.tag == 'doc' and len(section.lines) == 0:
                del sections[line]
                continue

            fn = '_process_{}'.format(section.tag)
            pf = getattr(self, fn, False)

            if pf:
                section.content = pf(section)

        first_line  = i

        self.sections.update(sections)

    def _process_doc(self, section):
        import markdown
        from ..util import normalize_newlines
        import textwrap

        if section.args:
            # Table documentation
            from collections import OrderedDict
            out = OrderedDict()
            for l in section.lines:
                parts = l.strip().split(' ',2)
                name = parts.pop(0) if parts else None
                type = parts.pop(0) if parts else 'UNK'
                doc = parts.pop(0).strip() if parts else ''
                out[name.lower()] = (name, type, doc)

            return out
        else:
            t = '\n'.join(section.lines)

            # Normal markdown documentation
            return dict(text=t,html=markdown.markdown(t))

    def _process_sql(self, section):
        return sqlparse.format(''.join(section.lines), reindent=True, keyword_case='upper')

    def _process_mview(self, section):
        t = sqlparse.format(''.join(section.lines), reindent=True, keyword_case='upper')

        return dict(text=t,html=self.pygmentize_sql(t))

    def _process_view(self, section):
        t = sqlparse.format(''.join(section.lines), reindent=True, keyword_case='upper')

        return dict(text=t,html=self.pygmentize_sql(t))

    def _process_extract(self, section):

        line = section.args

        words = line.split()

        if len(words) != 5:
            raise ParseError('Extract line has wrong format; expected 5 words, got: {}'.format(line))

        table, as_w, format, to_w, rpath = words

        if not as_w.upper() == 'AS':
            raise ParseError('Extract line malformed. Expected 3rd word to be \'as\' got: {}'.format(as_w))

        if not to_w.upper() == 'TO':
            raise ParseError('Extract line malformed. Expected 5th word to be \'to\' got: {}'.format(to_w))


        return dict(table=table, format=format, rpath=rpath)


    def _process_partitions(self, section):

        def row(cell, *args):
            return "<tr>{}</tr>\n".format(''.join([ "<{}>{}</{}>".format(cell,v,cell) for v in args]))

        partitions = []
        html = '<table class="partitions table table-striped table-bordered table-condensed">\n'
        html += row('th','d_vid','p_vid','vname','time','space','grain','format','tables')
        start_line  = section.linenumber
        for i,line in enumerate(section.lines):
            try:
                d = Manifest.parse_partition_line(line)

                if self.library:
                    b = self.library.resolve(d['partition'])

                    if not b:
                        raise ParseError("Partition reference not resolved to a bundle: '{}' ".format(d['partition']))

                    if not b.partition:
                        raise ParseError("Partition reference not resolved to a partition: '{}' ".format(d['partition']))

                    d['bundle'] = b
                    d['ident']  = b.partition

                    ident = b.partition
                    d['config']=dict(
                        time=ident.time if ident.time else '',
                        space=ident.space if ident.space else '',
                        grain=ident.grain if ident.grain else '',
                        format=ident.format if ident.format else '',
                    )


                partitions.append(d)

            except ParseError as e:
                raise ParseError("Failed to parse in section at line #{}: {}".format(start_line+i, e))

        html += '</table>'

        self.partitions = partitions
        return dict(partitions=partitions, html=html)


    def _process_index(self, section):

        line = section.args

        line = re.sub('index:', '', line, flags=re.IGNORECASE).strip()

        tokens, remainder = Manifest.tokenize_line(line.strip())

        (_, index_name), tokens = Manifest.extract("NAME", tokens)

        (_, table), tokens = Manifest.extract_next('ON', 'NAME', tokens)

        columns, tokens = Manifest.coalesce_list('NAME', tokens)

        return dict(name=index_name, table=table, columns=columns)

    @property
    def documentation(self):
        pass


    def documentation_for(self, name):
        """Return documentation for a table"""

        for line, section in self.sorted_sections:

            if section.tag =='doc' and section.args == name:
                return section

        return None

    @staticmethod
    def tokenize_line(line):
        import re

        scanner = re.Scanner([
            (r"#.*$", lambda scanner, token: ("COMMENT", token)),
            (r"from", lambda scanner, token: ("FROM", token)),
            (r"as", lambda scanner, token: ("AS", token)),
            (r"to", lambda scanner, token: ("TO", token)),
            (r"on", lambda scanner, token: ("ON", token)),
            (r"prefix", lambda scanner, token: ("PREFIX", token)),
            (r"where.*", lambda scanner, token: ("WHERE", token)),
            (r"[a-z0-9\.\-_\']+", lambda scanner, token: ("NAME", token)),
            (r"\s+", None),  # None == skip token.
            (r",\s*", lambda scanner, token: ("SEP", token)),
            (r"[^\s]+", lambda scanner, token: ("OTHER", token)),
        ], re.IGNORECASE)

        results, remainder = scanner.scan(line.strip())

        return results, remainder

    @staticmethod
    def has(tp, tokens):
        return any(filter(lambda x: x[0] == tp.upper(), tokens))

    @staticmethod
    def extract(tp, tokens):
        '''Extract the first token of the named type. '''

        i = [t[0] for t in tokens].index(tp)

        return tokens[i], tokens[:i] + tokens[i + 1:]

    @staticmethod
    def extract_next(tp1, tp2, tokens):
        '''Extract the token after the named token type. '''

        try:
            i = [t[0] for t in tokens].index(tp1)
        except ValueError:
            return None, tokens

        if tokens[i+1][0] != tp2:
            raise ParseError("Expected {}, got {}".format(tp2, tokens[i+1][1]))

        return tokens[i+1], tokens[:i]+tokens[i+2:]

    @staticmethod
    def coalesce_list(tp, tokens):
        '''Extract the token types, and all after it that are seperated with SEP '''

        t, tokens = Manifest.extract(tp, tokens)

        l = [t[1]]

        while True:
            t, tokens = Manifest.extract_next('SEP', tp, tokens)

            if not t:
                break

            l.append(t[1])

        return l, tokens

    @staticmethod
    def parse_partition_line(line):
        import re
        tokens, remainder = Manifest.tokenize_line(line.strip())

        try:
            try:
                (_, partition), tokens = Manifest.extract_next('FROM', "NAME", tokens)
            except TypeError:
                partition = None

            if partition:
                tables, tokens = Manifest.coalesce_list('NAME', tokens)
            else:
                (_,partition), tokens = Manifest.extract("NAME", tokens)
                tables = None

            try:
                (_, where), tokens = Manifest.extract('WHERE', tokens)

                where = re.sub(r'^where','', where, flags =  re.IGNORECASE).strip()

            except (TypeError, ValueError):
                where = None

            try:
                (_, prefix), tokens = Manifest.extract_next('PREFIX', 'NAME', tokens)

                prefix = re.sub(r'^where', '', prefix, flags=re.IGNORECASE).strip().strip("'")


            except (TypeError, ValueError):
                prefix = None

            return dict(
                partition=partition,
                tables = tables,
                where = where,
                prefix = prefix
            )

        except Exception as e:
            raise ParseError("Failed to parse {} : {}".format(line, e))

    @property
    def warehouse(self):
        from ..dbexceptions import NotFoundError, ConfigurationError
        from . import database_config, new_warehouse

        if self._warehouse and self._warehouse.exists():
            # When the manifest is created in IPython, the warehouse ca be deleted while the
            # manifest object exists.
            return self._warehouse

        config = database_config(self.destination, self.abs_work_dir)

        w = new_warehouse(config, self.library)

        if not w.exists():
            w.create()

        self._warehouse = w

        return self._warehouse



    def install(self):
        from os import getcwd, chdir

        last_wd = getcwd()


        try:
            return self._install()
        finally:
            chdir(last_wd)

    def _install(self):

        from ..dbexceptions import NotFoundError, ConfigurationError
        from ambry.util import init_log_rate
        from ambry.warehouse.extractors import extract
        from . import  Logger
        from ..cache import new_cache
        import os

        self.logger.info("Working directory: {}".format(self.abs_work_dir))

        # legacy
        m = self
        logger = self.logger

        w = self.warehouse

        w.logger = Logger(logger, init_log_rate(logger.info, N=2000))

        if os.path.exists(w.database.path):
            self.file_installs = set([w.database.path])

        working_cache = new_cache(self.abs_work_dir)

        ## First pass
        for line in sorted(m.sections.keys()):
            section = m.sections[line]

            tag = section.tag

            if tag in ('partitions','sql','index','mview','view'):
                logger.info("== Processing manifest section {} at line {}".format(section.tag, section.linenumber))

            if tag == 'partitions':
                for pd in section.content['partitions']:
                    try:

                        tables = pd['tables']

                        if pd['where'] and len(pd['tables']) == 1:
                            tables = [ (pd['tables'][0], "WHERE ("+pd['where']+")") ]

                        w.install(pd['partition'], tables)
                        self.installed_partitions.append(pd['partition'])

                    except NotFoundError:
                        logger.error("Partition {} not found in external library".format(pd['partition']))

            elif tag == 'sql':
                sql = section.content

                if w.database.driver in sql:
                    w.run_sql(sql[w.database.driver])

            elif tag == 'index':
                c = section.content
                w.create_index(c['name'], c['table'], c['columns'])

            elif tag == 'mview':
                w.install_material_view(section.args, section.content['text'], clean=self.force)

            elif tag == 'view':
                w.install_view(section.args, section.content['text'])

        ## Second Pass. Extracts must come after everything else.
        for line in sorted(m.sections.keys()):
            section = m.sections[line]

            tag = section.tag

            if tag in ('extract'):
                logger.info("== Processing manifest section {} at line {}".format(section.tag, section.linenumber))

            if tag == 'extract':
                import os

                c = section.content
                table = c['table']
                format = c['format']

                # rpath is the realtive path
                # dlrpath is the rpath for downloads. It is different for shapefile, which must be zipped to download.

                dest = c['rpath']

                logger.info("Extracting {} to {} as {}".format(table, format, dest))
                extracted, abs_path = extract(w.database, table, format, working_cache, dest, force=self.force)

                if not extracted:
                    logger.info("Didn't extract {}; it already existed".format(abs_path))

                elif os.path.exists(abs_path + ".zip"):
                    os.remove(abs_path + ".zip") # Get rid of old zip file.


                if os.path.isfile(abs_path):
                    self.file_installs.add(abs_path)


                    c['dlrpath'] = c['rpath']

                elif os.path.isdir(abs_path):
                    import zipfile

                    zfn = abs_path + ".zip"
                    c['dlrpath'] = c['rpath'] + ".zip"

                    if os.path.exists(zfn):
                        logger.info("Zip dir for {} already exists".format(abs_path))

                    else:
                        logger.info("Zipping directory {}".format(abs_path))



                        zf = zipfile.ZipFile(zfn, 'w', zipfile.ZIP_DEFLATED)

                        for root, dirs, files in os.walk(abs_path):
                            for f in files:
                                zf.write(os.path.join(root, f), os.path.join(dest,f))

                            zf.close()

                    self.file_installs.add(zfn)

        if self.file:
            fn = 'manifest.ambry'
            working_cache.put(self.file, fn)
            self.file_installs.add(working_cache.path(fn))

        index = self.write_documentation(working_cache)

        class InstallResult(object):
            """Returned by install() to capture the results of installation, and display it for
            IPython"""

            def __init__(self, manifest, index):
                self.index = index
                self.manifest = manifest
                self.warehouse = manifest.warehouse

            def _repr_html_(self):
                return """
<table>
<tr><td>Directory</td><td>{base_dir}</td></tr>
<tr><td>Documentation</td><td>{index}</td></tr>
<tr><td>Warehouse</td><td>{warehouse}</td></tr>
</table>""".format(warehouse=self.warehouse.database.dsn, index=self.index, uid=self.manifest.uid, base_dir = self.manifest.base_dir)

        self.logger.info("Done")

        return InstallResult(self, index)


    def write_documentation(self, cache):

        fn = 'index.html'
        s = cache.put_stream(fn)
        s.write(self.html_doc())
        s.close()
        index = afn = cache.path(fn)
        self.file_installs.add(afn)

        bundles = {}
        for p_name in self.installed_partitions:
            ident = self.library.resolve(p_name).partition
            b = self.library.get(ident.vid)
            bundles[b.identity.vid] = b
            # Write the partition documentation
            p = b.partition
            fn = ident.vid+".html"
            s = cache.put_stream(fn)
            s.write(p.html_doc())
            s.close()
            afn = cache.path(fn)
            self.file_installs.add(afn)

        for b in bundles.values():
            fn = b.identity.vid + ".html"
            s = cache.put_stream(fn)
            s.write(b.html_doc())
            s.close()
            afn = cache.path(fn)
            self.file_installs.add(afn)

            b.close()


        return index

    def publish(self, run_config, dest=None):
        from ..util import md5_for_file
        from ..cache import new_cache, parse_cache_string
        import json

        if self.access == 'public' or self.access == 'private-data':
            doc_acl = 'public-read'
        else:
            doc_acl = 'private'

        if self.access == 'private' or self.access == 'private-data':
            data_acl = 'private'
        else:
            data_acl = 'public-read'


        if not dest or  dest == 'default':
            dest = self.publication

        cache_config = parse_cache_string(dest)

        # Re-write account to get login credentials
        if 'account' in cache_config:
            cache_config['account'] = run_config.account(cache_config['account'])

        if cache_config.get('prefix', False):
            cache_config['prefix'] = cache_config['prefix'] + '/' + self.uid

        pub = new_cache(cache_config)

        self.logger.info("Publishing to {}".format(pub))


        doc_url = None

        for p in self.file_installs:

            if p == self.warehouse.database.path and not self.install_db:
                self.logger.info("Not installing database, skipping : {}".format(self.warehouse.database.path))
                continue

            rel = p.replace(self.abs_work_dir, '', 1).strip('/')

            md5 = md5_for_file(p)

            if pub.has(rel):
                meta =  pub.metadata(rel)
                if meta.get('md5',False) == md5:
                    self.logger.info("Md5 match, skipping : {}".format(rel))
                    if 'index.html' in rel:
                        doc_url = pub.path(rel, public_url=True, use_cname = True)
                    continue
                else:
                    self.logger.info("Remote has, but md5's don't match : {}".format(rel))
            else:
                self.logger.info("Publishing: {}".format(rel))


            meta = {
                'md5': md5
            }

            if rel.endswith('.html'):
                meta['Content-Type'] = 'text/html'
                meta['acl'] = doc_acl
            else:
                meta['acl'] = data_acl


            pub.put(p, rel, metadata=meta)

            self.logger.info("Published: {}".format(pub.path(rel, public_url = True, use_cname = True)))

            if 'index.html' in rel:
                doc_url = pub.path(rel, public_url = True, use_cname = True)


        # Write the Metadata
        cache_config['prefix'] = ''
        cache_config['options'] = []
        root = new_cache(cache_config)
        meta = self.meta
        meta['url'] = doc_url

        rel = os.path.join('meta', self.uid + '.json')

        s = root.put_stream(rel)
        s.write(json.dumps(meta))
        s.close()

        self.logger.info("Finished publication. Documentation at: {}".format(doc_url))

    def publish_ckan(self,d):

        pass

    def html_doc(self):
        from ..text import ManifestDoc
        import os

        md = ManifestDoc(self)

        return md.render()

    def gen_doc(self):
        """Generate schema documentation for direct inclusion in the manifest. This documentation requires having
        a database and is usually hand-edited, so it can't be fully automatic"""
        import textwrap
        out = ""

        w = self.warehouse()

        def indent(lines, amount=4, ch=' '):
            padding = amount * ch
            return padding + ('\n' + padding).join(lines.split('\n'))

        print self.documentation_for('crime_demo')

        for line, section in self.sorted_sections:
            if section.tag == 'view' or section.tag == 'mview':

                doccontent = self.documentation_for(section.args.lower())

                out += "\n\nDOC: for {}\n".format(section.args)

                for cols in w.installed_table(section.args):
                    default = (cols['name'], cols['type'], '')

                    if doccontent:
                        (name, type, doc) = doccontent.content.get(cols['name'].lower(), default)
                    else:
                        (name, type, doc) = default

                    out += "    {} {} {}\n".format(name.lower(), type, doc)

            else:
                continue


        wl = w.library

        return out

        out += '\n\n### Partitions\n\n'

        return out

    @property
    def meta(self):

        return {
            'title': self.title,
            'uid': self.uid,
            'summary': self.summary,
            'url': None,
            'access': self.access
        }

    def save(self,filename=None):
        """Write the manifest to a file.
        Writes to the current base_dir if a filename is not given or if the filename is not absolute
        """
        if not filename:
            import re
            filename = re.sub(r'[\s\\\/]','_',"{}.ambry".format(self.title))


        if not os.path.isabs(filename):
            filename = os.path.join(self.abs_work_dir, filename)

        with open(filename, 'w') as f:
            f.write(str(self))

    def __str__(self):

        o = ""

        last_tag_single = False

        for l, c in self.sorted_sections:

            if l == 1 and c.tag == 'doc' and not c.lines:
                last_tag_single = True
                continue # Skip the opening doc if it doesn't exist.

            this_tag_single = (c.tag  in self.singles)

            if not ( this_tag_single and last_tag_single ):
                o += '\n'

            if not(l == 1 and c.tag == 'doc' and not c.args):  # DOC is default for start, so don't need to print it
                if c.args:
                    o += "{}: {}\n".format(c.tag.upper(), c.args)
                else:
                    o += "{}:\n".format(c.tag.upper())

            if c.lines:

                if 'text' in c.content:
                    o+= c.content['text']
                else:
                    o += '\n'.join(c.lines)

                if not ( this_tag_single and last_tag_single ):
                    o += '\n'

            last_tag_single = this_tag_single

        return o

    def _repr_html_(self):
        return "<pre>" + str(self) + "</pre>"



