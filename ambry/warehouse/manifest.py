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


    def warn(self, w):
        pass


    def info(self, w):
        pass

class ParseError(Exception):
    pass

class ManifestSection(object):

    def __init__(self, path, tag, linenumber, args):
        self.path = path
        self.linenumber = linenumber
        self.args = args
        self.tag = tag

        self.lines = []
        self.content = None
        self.doc = None

    @property
    def file_line(self):
        return "{}:{}".format(self.path, self.linenumber)

    @property
    def name(self):

        if self.content and 'name' in self.content:
            return self.content['name']

        return None

    @property
    def ref(self):
        if self.content and 'ref' in self.content:
            return self.content['ref']

        return None

    def __str__(self):
        return "Section: tag={} line={} args={} content={} doc={}".format(self.tag, self.linenumber, self.args, self.content, self.doc)

class Manifest(object):

    # These tags have only a single line; revert back to 'doc' afterward
    singles = ['uid', 'title', 'extract', 'dir',  'database', 'cache',  'author', 'url', 'access', 'index', 'include']
    multi_line = ['partitions','view','mview','sql','doc']

    partitions = None

    def __init__(self, file_or_data, logger=None):

        from ..dbexceptions import ConfigurationError

        self.logger = logger if logger else null_logger()
        self.last_line = 0
        self.sections = {}

        self.file, self.data, self.path = self._extract_file_data(file_or_data)

        if self.data:
            self.sectionalize(self.data)

        self.file_installs = set()
        self.installed_partitions = list()

    def _extract_file_data(self, file_or_data):

        if file_or_data.startswith('http'):  # A URL
            import requests

            path = file_or_data

            r = requests.get(path)
            r.raise_for_status()

            file = None

            data = r.text.splitlines()

        elif os.path.exists(file_or_data):  # A file
            with open(file_or_data, 'r') as f:
                data = f.readlines()

            file = file_or_data
            path = file_or_data

        else:  # String data

            if isinstance(file_or_data, list):
                data = file_or_data
            else:
                data = file_or_data.splitlines()
            file = None
            path = None

        return file, data, path

    @property
    def sorted_sections(self):
        for line in sorted(self.sections.keys()):
            yield (line, self.sections[line])

    def tagged_sections(self, tag):

        if isinstance(tag,basestring):
            tag = [tag]

        for line in sorted(self.sections.keys()):
            if self.sections[line].tag in tag:
                yield (line, self.sections[line])

    def single_line(self, keyword):
        for line, section in self.sections.items():
            if section.tag == keyword:
                return section.args.strip()

        return None

    def count_sections(self,tag):
        return sum( section.tag == tag for section in self.sections.values())

    @property
    def database(self):
        return self.single_line('database')

    @property
    def cache(self):
        return self.single_line('cache')

    @property
    def uid(self):
        from ..dbexceptions import ConfigurationError

        uid =  self.single_line('uid')

        if not uid:
            from ..identity import TopNumber
            tn = TopNumber('m')
            raise ConfigurationError(
                "Manifest does not have a UID. Add this line to the file:\n\nUID: {}\n".format(str(tn)))

        return uid




    @property
    def title(self):
        return self.single_line('title')

    @property
    def summary(self):

        t = self.tagged_sections('title').pop()

        print t.doc



    @property
    def summary(self):
        """The first doc section"""

        for line, section in self.sorted_sections:

            if section.tag == 'doc':
                return section.content

        return None

    def doc_for(self, section):
        """Return a doc section that referrs to a named section. """

        if not section.name:
            return None

        for line, other in self.sorted_sections:

            if other.tag == 'doc' and other.ref == section.name:
                return other

        return None


    def pygmentize_sql(self,c):
        return  highlight(c, PythonLexer(), HtmlFormatter())

    @property
    def css(self):
        return HtmlFormatter(style='manni').get_style_defs('.highlight')


    def make_item(self, sections, tag, i, args):
        """Creates a new entry in sections, which will later have lines appended to it. """
        from ..dbexceptions import  ConfigurationError

        if tag not in self.singles and tag not in self.multi_line:
            raise ConfigurationError("Unknown tag '{}'  at {}:{}".format(tag, self.path, i))

        line_number = i + 1
        section = ManifestSection(self.path, tag=tag, linenumber=line_number, args=args)
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

                # The '#' is a valid char in cache URLs
                if tag == 'cache' and '#' in line_w_comments:
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

        # Postprocessing cleanup
        for line in sections.keys():

            section = sections[line]

            non_empty_lines = len([l for l in section.lines if l.strip()])

            # clear out any empty doc sections. These tend to get created for blank lines.
            if section.tag == 'doc' and non_empty_lines == 0:
                del sections[line]
                continue

            fn = '_process_{}'.format(section.tag)
            pf = getattr(self, fn, False)

            if pf:
                section.content = pf(section)

        # Link docs to previous sections, where appropriate

        previous_section = None
        for line_no in sorted(sections.keys()):
            section = sections[line_no]

            if previous_section and section.tag == 'doc' and previous_section.tag in ['title', 'view','mview','extract']:
                section.content['ref'] = previous_section.name
                previous_section.doc = section.content

            previous_section = section

        self.sections.update(sections)

    def _extract_summary(self, t):
        """Extract the first sentence of a possiblt Markdown text"""
        from nltk import tokenize

        test = tokenize.punkt.PunktSentenceTokenizer()

        sentences = test.sentences_from_text(t)
        if sentences:
            return sentences[0].strip()

        return None

    def _process_doc(self, section):
        import markdown
        from ..util import normalize_newlines
        import textwrap

        t = '\n'.join(section.lines)

        summary = self._extract_summary(t)

        # Normal markdown documentation
        return dict(text=t.strip(),
                    summary_text = summary,
                    html=markdown.markdown(t),
                    summary_html=markdown.markdown(summary))

    def _process_sql(self, section):

        return sqlparse.format('\n'.join(section.lines), reindent=True, keyword_case='upper')

    def _process_mview(self, section):

        if not section.args.strip():
            raise ParseError('No name specified for view at {}'.format(section.file_line))

        t = sqlparse.format('\n'.join(section.lines), reindent=True, keyword_case='upper')

        if not t.strip():
            raise ParseError('No sql specified for view at {}'.format(section.file_line))

        tc_names = set()  # table and column names

        # Add table names from parsing the SQL, so we can build dependencies for views. Unfortunately,
        # it also add column names, which are removed when the template context is created.

        # The SQL parser doesn't like quotes, so remove them first
        for s in sqlparse.parse('\n'.join(section.lines).replace('"','')):
            for tok in s.flatten():
                if tok.ttype == sqlparse.tokens.Name:
                    tc_names.add(str(tok))


        return dict(text=t,html=self.pygmentize_sql(t), name = section.args.strip(), tc_names = list(tc_names))

    def _process_view(self, section):
        import sqlparse.tokens

        if not section.args.strip():
            raise ParseError('No name specified for view at {}'.format(section.file_line))

        t = sqlparse.format('\n'.join(section.lines), reindent=True, keyword_case='upper')

        if not t.strip():
            raise ParseError('No sql specified for view at {}'.format(section.file_line))

        tc_names = set() # table and column names

        for s in sqlparse.parse('\n'.join(section.lines)):

            for tok in s.flatten():

                if tok.ttype in (sqlparse.tokens.Name, sqlparse.tokens.String.Symbol):
                    tc_names.add(str(tok).strip('"'))


        return dict(text=t,html=self.pygmentize_sql(t),
                    name = section.args.strip(), tc_names = list(tc_names))

    def _process_extract(self, section):

        line = section.args

        words = line.split()

        #if len(words) != 5:
        #    raise ParseError('Extract line has wrong format; expected 5 words, got: {}'.format(line))


        table = words.pop(0)

        format = 'csv'
        rpath = None

        for i, word in enumerate(words):
            if word.upper() == 'AS':
                format = words[i+1]

            if word.upper() == 'TO':
                rpath = words[i+1]


        if not rpath:
            rpath = "{}.{}".format(table, format)


        return dict(table=table, format=format, rpath=rpath, name=rpath)

    def _process_include(self, section):
        import os.path

        path = section.args.strip()

        if not self.path and not os.path.isabs(path):
            # Can't include, but let the caller deal with that.
            return dict(path=path)

        if self.path.startswith('http'):
            raise NotImplementedError
        else:
            if not os.path.isabs(path) :
                path = os.path.join(os.path.dirname(self.path), path)

        return dict(path = path )

    def _process_partitions(self, section):

        partitions = []

        start_line  = section.linenumber
        for i,line in enumerate(section.lines):
            try:
                d = Manifest.parse_partition_line(line)

                partitions.append(d)

            except ParseError as e:
                raise ParseError("Failed to parse in section at line #{}: {}".format(start_line+i, e))

        self.partitions = partitions
        return dict(partitions=partitions)


    def add_bundles(self, library):
        """Add bundle information when a Library is available"""
        from ..bundle import LibraryDbBundle
        for line, partitions in self.tagged_sections('partitions'):

            for partition in partitions.content['partitions']:

                ident = library.resolve(partition['partition'])

                if not ident:
                    raise ParseError("Partition reference not resolved to a bundle: '{}' in manifest '{}' "
                                     " for library {}"
                                     .format(partition['partition'], self.path, library.database.dsn))

                if not ident.partition:
                    raise ParseError("Partition reference not resolved to a partition: '{}' ".format(partition['partition']))

                b = LibraryDbBundle(library.database, ident.vid)

                partition['bundle'] = ident
                partition['metadata'] = b.metadata
                partition['ident'] = ident.partition

                p = b.partitions.get(ident.partition.vid)

                # The 'tables' key is used for tables specified on the partitions line in the manifest.
                partition['table_vids'] = [ b.schema.table(t).vid for t in  p.tables]

                ident = ident.partition
                partition['config'] = dict(
                    time=ident.time if ident.time else '',
                    space=ident.space if ident.space else '',
                    grain=ident.grain if ident.grain else '',
                    format=ident.format if ident.format else '',
                )

    def _process_index(self, section):

        line = section.args

        line = re.sub('index:', '', line, flags=re.IGNORECASE).strip()

        tokens, remainder = Manifest.tokenize_line(line.strip())

        (_, index_name), tokens = Manifest.extract_token("NAME", tokens)

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
    def extract_token(tp, tokens):
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

        t, tokens = Manifest.extract_token(tp, tokens)

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
                (_,partition), tokens = Manifest.extract_token("NAME", tokens)
                tables = None

            try:
                (_, where), tokens = Manifest.extract_token('WHERE', tokens)

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
    def dict(self):
        m = self.meta
        m['sections'] =  [ s.__dict__ for l, s in self.sorted_sections ]

        return m

    @property
    def meta(self):

        return {
            'title': self.title,
            'uid': self.uid,
            'summary': self.summary,
            'url': None
        }

    def __str__(self):
        """Re-create the Manifest text from the sections. """
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

                if not ( this_tag_single and last_tag_single  ):
                    o += '\n'

            last_tag_single = this_tag_single

        return o

    def _repr_html_(self):
        return "<pre>" + str(self) + "</pre>"
