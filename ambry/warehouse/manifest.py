"""
Class for handling manifest files.
"""

# Copyright (c) 2014 Clarinova. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt

import os.path
import re
import sqlparse

class null_logger(object):

    def error(self, w):
        pass


def new_manifest(ref, logger):
    return Manifest(ref, logger)

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

        self.sectionalize()

    def single_line(self, keyword):
        for line, section in self.sections.items():
            if section['tag'] == keyword:
                return section['args'].strip()

        return None

    def count_sections(self,tag):
        return sum( section['tag'] == tag for section in self.sections.values())

    @property
    def destination(self):
        return self.single_line('destination')

    @property
    def work_dir(self):
        return self.single_line('dir')

    @property
    def uid(self):
        return self.single_line('uid')

    def sectionalize(self):
        """Break the file into sections"""

        import re

        sections = {}

        def make_item(tag, line_number, args):
            sections[line_number] = dict(tag=tag, line_number=line_number, args=args, lines=[])

        line_number = 1
        tag = 'doc'  # Starts in the Doc section
        args = ''

        make_item(tag, line_number, args)

        for i, line in enumerate(self.data):
            line = re.sub(r'#.*$','', line ) # Remove comments

            if not line.strip():
                continue

            rx = re.match(r'^(\w+):(.*)$', line.strip())

            if rx: # Section tag lines
                line_number = i+1
                tag = rx.group(1).strip().lower()
                args = re.sub(r'#.*$','', rx.group(2) ).strip()
                make_item(tag, line_number, args)
                continue

            sections[line_number]['lines'].append(line)

        for line in sections.keys():

            section = sections[line]

            fn = '_process_{}'.format(section['tag'])
            pf = getattr(self, fn, False)

            if pf:
                section['content'] = pf(section)


        self.sections = sections

    def _process_doc(self, section):
        return '\n'.join(section['lines'])

    def _process_sql(self, section):
        return sqlparse.format(''.join(section['lines']), reindent=True, keyword_case='upper')

    def _process_mview(self, section):
        return sqlparse.format(''.join(section['lines']), reindent=True, keyword_case='upper')

    def _process_view(self, section):
        return sqlparse.format(''.join(section['lines']), reindent=True, keyword_case='upper')

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

        content = []
        start_line  = section['line_number']
        for i,line in enumerate(section['lines']):
            try:
                content.append(Manifest.parse_partition_line(line))
            except ParseError as e:
                raise ParseError("Failed to parse line #{}: {}".format(start_line+i, e))

        return content

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


    def install(self, l, base_dir):
        from os import getcwd, chdir

        last_wd = getcwd()

        try:
            self._install(l, base_dir)
        finally:
            chdir(last_wd)

    def _install(self, l, base_dir):
        import os.path
        from ..dbexceptions import NotFoundError, ConfigurationError
        from ambry.util import init_log_rate
        from ambry.warehouse.extractors import extract
        from . import database_config, new_warehouse, Logger
        from ..cache import new_cache

        # legacy
        m = self
        logger = self.logger

        destination = m.destination

        if not base_dir:
            raise ConfigurationError("Must specify a base dir")

        if not os.path.isdir(base_dir):
            os.makedirs(base_dir)

        work_dir = os.path.join(base_dir, m.work_dir if m.work_dir else None)

        pub_dir = work_dir

        if not os.path.isdir(work_dir):
            os.makedirs(work_dir)

        if not work_dir or not os.path.isdir(work_dir):
            raise ConfigurationError("Must specify either work dir ")

        os.chdir(work_dir)

        logger.info("Working directory: {}".format(work_dir))

        config = database_config(destination)

        w = new_warehouse(config, l)

        if not w.exists():
            w.create()

        w.logger = Logger(logger, init_log_rate(2000))

        if not m.uid:
            import uuid

            raise ConfigurationError(
                "Manifest does not have a UID. Add this line to the file:\n\nUID: {}\n".format(uuid.uuid4()))

        for line in sorted(m.sections.keys()):
            section = m.sections[line]

            tag = section['tag']

            logger.info("== Processing manifest section {} at line {}".format(section['tag'], section['line_number']))

            if tag == 'partitions':
                for pd in section['content']:
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
                cache = new_cache(pub_dir)
                abs_path = extract(w.database, table, format, cache, dest)
                logger.info("Extracted to {}".format(abs_path))


    def html_doc(self):
        from ..web import Web
