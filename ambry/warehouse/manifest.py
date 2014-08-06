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


def new_manifest(ref, logger, library, base_dir, force=False):
    return Manifest(ref, logger, library = library, base_dir = base_dir, force=force)

class ParseError(Exception):
    pass

class Manifest(object):

    def __init__(self, file_or_data, logger=None, library=None, base_dir=None, pub_dest=None, force=False):
        from ..dbexceptions import NotFoundError, ConfigurationError

        self.library = library
        self.base_dir = base_dir
        self.force = force

        if file_or_data.startswith('http'):
            import requests

            r = requests.get(file_or_data)
            r.raise_for_status()

            self.file = None

            self.data = r.text.splitlines()

        elif os.path.exists(file_or_data):
            with open(file_or_data, 'r') as f:
                self.data = f.readlines()
                self.file = file_or_data
        else:

            if isinstance(file_or_data, list):
                self.data = file_or_data
            else:
                self.data = file_or_data.splitlines()
            self.file = None


        self.logger = logger if logger else null_logger()

        self.sectionalize()

        if not self.uid:
            import uuid

            raise ConfigurationError(
                "Manifest does not have a UID. Add this line to the file:\n\nUID: {}\n".format(uuid.uuid4()))

        if not self.base_dir:
            raise ConfigurationError("Must specify a base dir")

        if not os.path.isdir(self.base_dir):
            os.makedirs(self.base_dir)

        work_dir = self.work_dir if self.work_dir else self.uid

        self.abs_work_dir = os.path.join(self.base_dir, work_dir)

        self.pub_dir = self.abs_work_dir

        if not os.path.isdir(self.abs_work_dir):
            os.makedirs(self.abs_work_dir)

        if not self.work_dir or not os.path.isdir(self.abs_work_dir):
            raise ConfigurationError("Must specify  work dir ")

        self.file_installs = set()
        self.publishable = set()
        self.installed_partitions = list()

    @property
    def sorted_sections(self):
        for line in sorted(self.sections.keys()):
            yield (line, self.sections[line])

    def tagged_sections(self, tag):
        for line in sorted(self.sections.keys()):
            if self.sections[line]['tag'] == tag:
                yield (line, self.sections[line])

    def single_line(self, keyword):
        for line, section in self.sections.items():
            if section['tag'] == keyword:
                return section['args'].strip()

        return None

    def count_sections(self,tag):
        return sum( section['tag'] == tag for section in self.sections.values())

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
        return self.single_line('uid')

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

            if section['tag'] == 'doc':
                return section['content']

        return None


    def pygmentize_sql(self,c):
        return  highlight(c, PythonLexer(), HtmlFormatter())

    @property
    def css(self):
        return HtmlFormatter(style='manni').get_style_defs('.highlight')

    def sectionalize(self):
        """Break the file into sections"""

        import re

        sections = {}

        # These tags have only a single line; revert back to 'doc' afterward
        singles = ['uid', 'title','extract','dir','destination','database','publish', 'author', 'url', 'access']

        def make_item(tag, i, args):
            line_number = i + 1
            sections[line_number] = dict(tag=tag, line_number=line_number, args=args, lines=[])
            return line_number

        tag = 'doc'  # Starts in the Doc section
        args = ''
        non_tag_is_doc = False

        line_number =  make_item(tag, 0, args)

        for i, line in enumerate(self.data):

            line_w_comments = line # A hack to handle '#compress' in cache specification

            if tag != 'doc':
                line = re.sub(r'#.*$','', line ) # Remove comments

            if not line.strip():
                if non_tag_is_doc:
                    non_tag_is_doc = False
                    tag = 'doc'
                    line_number = make_item(tag, i, None)
                continue

            rx = re.match(r'^(\w+):(.*)$', line.strip())

            if rx: # Section tag lines

                tag = rx.group(1).strip().lower()

                if tag == 'publication' and '#' in line_w_comments:
                    rx = re.match(r'^(\w+):(.*)$', line_w_comments.strip())

                args = rx.group(2).strip()
                line_number = make_item(tag, i, args)

                if tag in singles:
                    non_tag_is_doc = True

                continue

            elif non_tag_is_doc:
                non_tag_is_doc = False
                tag = 'doc'
                line_number = make_item(tag, i, None)

            sections[line_number]['lines'].append(line)

        for line in sections.keys():

            section = sections[line]

            fn = '_process_{}'.format(section['tag'])
            pf = getattr(self, fn, False)

            if pf:
                section['content'] = pf(section)


        self.sections = sections

    def _process_doc(self, section):
        import markdown

        if section['args']:
            # Table documentation
            from collections import OrderedDict
            out = OrderedDict()
            for l in section['lines']:
                parts = l.strip().split(' ',2)
                name = parts.pop(0) if parts else None
                type = parts.pop(0) if parts else 'UNK'
                doc = parts.pop(0).strip() if parts else ''
                out[name.lower()] = (name, type, doc)

            return out
        else:
            t = '\n'.join(section['lines']).strip()

            # Normal markdown documentation
            return dict(text=t,html=markdown.markdown(t))

    def _process_sql(self, section):
        return sqlparse.format(''.join(section['lines']), reindent=True, keyword_case='upper')

    def _process_mview(self, section):
        t = sqlparse.format(''.join(section['lines']), reindent=True, keyword_case='upper')

        return dict(text=t,html=self.pygmentize_sql(t))

    def _process_view(self, section):
        t = sqlparse.format(''.join(section['lines']), reindent=True, keyword_case='upper')

        return dict(text=t,html=self.pygmentize_sql(t))

    def _process_extract(self, section):

        line = section['args']

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
        start_line  = section['line_number']
        for i,line in enumerate(section['lines']):
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

        line = section['args']

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

            if section['tag'] =='doc' and section['args'] == name:
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
    @memoize
    def warehouse(self):
        from ..dbexceptions import NotFoundError, ConfigurationError
        from . import database_config, new_warehouse

        config = database_config(self.destination, self.abs_work_dir)

        w = new_warehouse(config, self.library)

        if not w.exists():
            w.create()

        return w

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

        self.logger.info("Working directory: {}".format(self.abs_work_dir))

        # legacy
        m = self
        logger = self.logger

        w = self.warehouse

        w.logger = Logger(logger, init_log_rate(logger.info, N=2000))

        self.file_installs = set([w.database.path])
        self.publishable = set()

        working_cache = new_cache(self.abs_work_dir)

        ## First pass
        for line in sorted(m.sections.keys()):
            section = m.sections[line]

            tag = section['tag']

            if tag in ('partitions','sql','index','mview','view'):
                logger.info("== Processing manifest section {} at line {}".format(section['tag'], section['line_number']))

            if tag == 'partitions':
                for pd in section['content']['partitions']:
                    try:

                        tables = pd['tables']

                        if pd['where'] and len(pd['tables']) == 1:
                            tables = [ (pd['tables'][0], "WHERE ("+pd['where']+")") ]

                        w.install(pd['partition'], tables)
                        self.installed_partitions.append(pd['partition'])

                    except NotFoundError:
                        logger.error("Partition {} not found in external library".format(pd['partition']))

            elif tag == 'sql':
                sql = section['content']

                if w.database.driver in sql:
                    w.run_sql(sql[w.database.driver])

            elif tag == 'index':
                c = section['content']
                w.create_index(c['name'], c['table'], c['columns'])

            elif tag == 'mview':
                w.install_material_view(section['args'], section['content']['text'], clean=self.force)

            elif tag == 'view':
                w.install_view(section['args'], section['content']['text'])

        ## Second Pass. Extracts must come after everything else.
        for line in sorted(m.sections.keys()):
            section = m.sections[line]

            tag = section['tag']

            if tag in ('extract'):
                logger.info("== Processing manifest section {} at line {}".format(section['tag'], section['line_number']))

            if tag == 'extract':
                import os

                c = section['content']
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
                    self.publishable.add(abs_path)

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

                    self.file_installs.add(abs_path)
                    self.publishable.add(zfn)


        self.write_documentation(working_cache)


    def write_documentation(self, cache):

        fn = 'index.html'
        s = cache.put_stream(fn)
        s.write(self.html_doc())
        s.close()
        afn = cache.path(fn)
        self.file_installs.add(afn)
        self.publishable.add(afn)

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
            self.publishable.add(afn)

        for b in bundles.values():
            fn = b.identity.vid + ".html"
            s = cache.put_stream(fn)
            s.write(b.html_doc())
            s.close()
            afn = cache.path(fn)
            self.file_installs.add(afn)
            self.publishable.add(afn)

            b.close()



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

        self.publishable.add(self.file)

        doc_url = None

        for p in self.publishable:

            if p.endswith('.ambry'): # its the manifest file.
                rel = os.path.basename(p)
            else:
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
            if section['tag'] == 'view' or section['tag'] == 'mview':

                doccontent = self.documentation_for(section['args'].lower())

                out += "\n\nDOC: for {}\n".format(section['args'])

                for cols in w.installed_table(section['args']):
                    default = (cols['name'], cols['type'], '')

                    if doccontent:
                        (name, type, doc) = doccontent['content'].get(cols['name'].lower(), default)
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


