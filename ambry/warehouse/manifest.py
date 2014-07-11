"""
Class for handling manifest files.
"""

# Copyright (c) 2014 Clarinova. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt

import os.path
import re

class null_logger(object):

    def error(self, w):
        pass


class ParseError(Exception):
    pass

class Manifest(object):

    def __init__(self, file_or_data, logger=None):


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

    def single_line(self, keyword):
        import re
        for line in self.data:
            if line.lower().strip().startswith('{}:'.format(keyword)):
                return re.match(r'\w+:([^#]+)', line).group(1).strip()

        return None


    @property
    def destination(self):
        return self.single_line('destination')

    @property
    def work_dir(self):
        return self.single_line('dir')

    @property
    def documentation(self):

        in_doc_section = True # Start in doc mode

        doc_lines = []

        for line in self.data:

            if line.lower().strip().startswith('doc:'):
                in_doc_section = True
                continue

            if re.match(r'^\w+:', line.strip()):
                in_doc_section = False
                continue

            if in_doc_section and line.strip():
                doc_lines.append(line)

        return '\n'.join(doc_lines)

    @property
    def sql(self):
        ''' Collect all of the SQL entries by driver. Each block can apply to
        multiple drivers.
        '''
        from collections import defaultdict

        in_sql_section = False
        drivers = None

        sql_lines = defaultdict(list)

        for line in self.data:

            m = re.match(r'^(\w+):\s*([\w+|\s]+)\s*', line)

            if m and m.group(1).lower() == 'sql':
                in_sql_section = True
                drivers = [ d.strip() for d in m.groups()[1].split('|') ]

                continue

            if re.match(r'^\w+:', line.strip()):
                in_sql_section = False
                continue

            if in_sql_section and line.strip():
                for driver in drivers:
                    sql_lines[driver].append(line)

        return { driver:'\n'.join(lines) for driver, lines in sql_lines.items() }

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
    def partitions(self):
        import re
        in_partitions_section = False

        for line in self.data:
            line = line.strip()

            m = re.match(r'^(.*)#.*$', line)
            if m:
                line = m.groups()[0]

            if line.lower().startswith('partitions:'):
                in_partitions_section = True
                continue

            if re.match(r'^\w+:', line):
                in_partitions_section = False
                continue

            if in_partitions_section and line.strip():
                yield Manifest.parse_partition_line(line)


    @property
    def indexes(self):
        import re

        for line in self.data:
            line = line.strip()

            if line.lower().startswith('index:'):

                line = re.sub('index:','',line, flags=re.IGNORECASE).strip()

                tokens, remainder = Manifest.tokenize_line(line.strip())

                (_, index_name), tokens = Manifest.extract("NAME", tokens)

                (_, table), tokens = Manifest.extract_next('ON', 'NAME', tokens)

                columns, tokens = Manifest.coalesce_list('NAME', tokens)

                yield index_name, table, columns

    @property
    def extracts(self):
        import re


        for line in self.data:
            line = line.strip()

            if line.lower().startswith('extract:'):
                in_extracts_section = True

                words  = line.split()

                if len(words) != 6:
                    self.logger.error('Extract line has wrong format; expected 6 words, got: {}'.format(line))
                    continue;

                _, table, as_w, format, to_w, rpath = words

                if not as_w.lower() == 'as':
                    self.logger.error('Extract line malformed. Expected 3rd word to be \'as\' got: {}'.format(as_w))

                if not as_w.lower() == 'to_w':
                    self.logger.error('Extract line malformed. Expected 5th word to be \'to\' got: {}'.format(to_w))

                yield table, format, rpath


    def yield_view_lines(self, data):
        """Read all of the lines of the manifest and return the lines for views"""
        in_views_section = False
        in_view = False
        view_name = None

        for line in data:
            line = line.strip()

            if line.lower().startswith('view:') or line.lower().startswith('mview:'):  # Start of the views section

                m = re.match(r'^(m?view):\w*([^\#]+)', line, flags=re.IGNORECASE)

                if m and m.group(2):
                    view_name = m.group(2).strip()
                    if view_name:
                        in_views_section = True
                        view_type = m.group(1).lower()
                        yield 'end', None, None
                        continue

            if re.match(r'^\w+:', line):  # Start of some other section
                in_views_section = False
                yield 'end', None, None
                continue

            if not in_views_section:
                continue

            if re.match(r'create view', line.lower()):
                yield 'end', None, None

            if line.strip():
                if view_type == 'view':
                    yield 'viewline', view_name, line.strip()
                elif view_type == 'mview':
                    yield 'mviewline', view_name, line.strip()
                else:
                    raise Exception(view_type)

        yield 'end', None, None

    def _views(self, type_):

        from collections import defaultdict

        view_lines = defaultdict(list)

        for cmd, view_name, line in self.yield_view_lines(self.data):
            if cmd == type_:
                view_lines[view_name].append(line)

        for view_name, view_line in view_lines.items():
            yield view_name, ' '.join(view_line)


    @property
    def views(self):
        return self._views('viewline')

    @property
    def mviews(self):
        return self._views('mviewline')