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


def new_manifest(ref, logger, library, base_dir):
    return Manifest(ref, logger, library = library, base_dir = base_dir)

class ParseError(Exception):
    pass

class Manifest(object):

    def __init__(self, file_or_data, logger=None, library=None, base_dir=None, pub_dest=None):
        from ..dbexceptions import NotFoundError, ConfigurationError

        self.library = library
        self.base_dir = base_dir

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

    @property
    def sorted_sections(self):
        for line in sorted(self.sections.keys()):
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
    def work_dir(self):
        return self.single_line('dir')

    @property
    def uid(self):
        return self.single_line('uid')

    @property
    def title(self):
        return self.single_line('title')

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
        singles = ['uid', 'title','extract','dir','destination','database','publish', 'author', 'url']

        def make_item(tag, i, args):
            line_number = i + 1
            sections[line_number] = dict(tag=tag, line_number=line_number, args=args, lines=[])
            return line_number

        tag = 'doc'  # Starts in the Doc section
        args = ''
        non_tag_is_doc = False

        line_number =  make_item(tag, 0, args)

        for i, line in enumerate(self.data):

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
                args = re.sub(r'#.*$','', rx.group(2) ).strip()
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
        md = '\n'.join(['    '+l for l in t.splitlines()])
        return dict(text=t,html=self.pygmentize_sql(t))

    def _process_view(self, section):
        t = sqlparse.format(''.join(section['lines']), reindent=True, keyword_case='upper')
        md = '\n'.join(['    ' + l for l in t.splitlines()])
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

                partitions.append(d)

                if self.library:
                    ident = self.library.resolve(d['partition']).partition

                    html +=row('td',ident.as_dataset().vid, ident.vid, ident.vname,
                               ident.time if ident.time else '',
                               ident.space if ident.space else '',
                               ident.grain if ident.grain else '',
                               ident.format if ident.format else '',
                               d['tables'] if d['tables'] else '')

            except ParseError as e:
                raise ParseError("Failed to parse line #{}: {}".format(start_line+i, e))

        html += '</table>'

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
            (r"where.*", lambda scanner, token: ("WHERE", token)),
            (r"[a-z0-9\.\-_]+", lambda scanner, token: ("NAME", token)),
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

            return dict(
                partition=partition,
                tables = tables,
                where = where
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

        w.logger = Logger(logger, init_log_rate(2000))

        self.file_installs = set([w.database.path])
        self.publishable = set()

        working_cache = new_cache(self.abs_work_dir)

        for line in sorted(m.sections.keys()):
            section = m.sections[line]

            tag = section['tag']

            logger.info("== Processing manifest section {} at line {}".format(section['tag'], section['line_number']))

            if tag == 'partitions':
                for pd in section['content']['partitions']:
                    try:

                        tables = pd['tables']

                        if pd['where'] and len(pd['tables']) == 1:
                            tables = (pd['tables'][0], pd['where'])

                        w.install(pd['partition'], tables)

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
                w.install_material_view(section['args'], section['content'])

            elif tag == 'view':
                w.install_view(section['args'], section['content'])

            elif tag == 'extract':
                c = section['content']
                table = c['table']
                format = c['format']
                dest = c['rpath']

                logger.info("Extracting {} to {} as {}".format(table, format, dest))

                abs_path = extract(w.database, table, format, working_cache, dest)
                logger.info("Extracted to {}".format(abs_path))

                self.file_installs.add(abs_path)
                self.publishable.add(abs_path)


        fn = 'documentation.html'
        working_cache.put_stream(fn).write(m.html_doc())

        self.publishable.add(working_cache.path(fn))
        self.file_installs.add(working_cache.path(fn))


    def publish(self, run_config, dest=None):

        from ..cache import new_cache, parse_cache_string

        if not dest:
            dest = self.publication

        cache_config = parse_cache_string(dest)

        # Re-write account to get login credentials
        if 'account' in cache_config:
            cache_config['account'] = run_config.account(cache_config['account'])

        pub = new_cache(cache_config)

        for p in self.publishable:

            rel = p.replace(self.abs_work_dir, '', 1).strip('/')
            meta = {}

            if rel.endswith('.html'):
                meta['Content-Type'] = 'text/html'

            self.logger.info("Publishing: {}".format(rel))
            pub.put(p, rel, metadata=meta)

            self.logger.info("Published: {}".format(pub.path(rel)))

        # Write the documentation seperately, since it will require extract paths that are relative to
        # the web files we just wrote.





    def html_doc(self):
        from ..text import ManifestDoc

        md = ManifestDoc(self)

        return md.render()


    def gen_doc(self):
        """Generate schema documentation for direct inclusion in the manifest. This documentation requires having
        a database and is usually hand-edited, so it cant be fully automatic"""
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
