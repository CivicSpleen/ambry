"""
Class to read and write the sources.csv file to into and out of the metadata.
"""

from collections import OrderedDict

class SourcesFile(object):

    header = (
        ("name","name"),
        ("title" , "title"),
        ("table", "table"),
        ("segment" , "segment"),
        ("time", "time"),
        ("space", "space"),
        ("grain", "grain"),
        ("start_line", "row_spec.data_start_line"),
        ("end_line", "row_spec.data_end_line"),
        ("comment_lines", "row_spec.header_comment_lines"),
        ("header_lines", "row_spec.header_lines"),
        ("description", "description"),
        ("url", "url"),
        ("ref", "ref")
    )

    def __init__(self, path, metadata):

        self._path = path
        self._metadata = metadata

    def read(self):
        """ Read the source file and write it into the metadata

        :return:
        """
        import unicodecsv as csv

        m = OrderedDict(self.header)

        self._metadata.sources = {}

        with open(self._path) as f:
            r = csv.DictReader(f)

            for row in r:

                row = {m.get(k, k) : v for k,v in row.items()}

                s = self._metadata.sources[row['name']]

                name = row['name']
                del row['name']

                for k, v in row.items():
                    if not 'row_spec' in k:
                        k = m[k]

                        try:
                            v = int(v)
                        except ValueError:
                            pass # v = v

                        if bool(v) or v == 0:
                            s[k] = v
                        elif k in s:
                            del s[k]

                for k, v in row.items():

                    if 'row_spec' in k:

                        k = k.replace('row_spec.','')
                        if 'lines' in k:
                            v = [ int(x) for x in v.split(',') if x ]
                        else:
                            try:
                                v =  int(v)
                            except ValueError:
                                pass # v = v

                        if bool(v) or v == 0 :
                            s.row_spec[k] = v
                        elif k in s.row_spec:
                            del s.row_spec[k]

                if row.get('ref',False):
                    # If the source has a ref, then it is also a dependency
                    self._metadata.dependencies[name] = row['ref']



    def write(self):

        m = OrderedDict([ (v,k) for k, v in self.header])

        header = [x[0] for x in self.header]

        with open(self._path,'w') as f:
            import unicodecsv as csv

            r = csv.DictWriter(f,header)
            r.writeheader()

            rows = []
            for source_name, source in self._metadata.sources.items():
                row = { 'name' : source_name}
                for k,v in source.items():

                    if k == 'row_spec':
                        for rk, rv in v.items():
                            ok = m['row_spec.'+rk]
                            try:
                                ov = ','.join( str(x) for x in list(rv))
                            except TypeError as e:
                                ov = rv

                            row[ok] = ov

                    else:
                        ok, ov = m.get(k,k), v

                        row[ok] = ov

                    if not k in header and k in row:
                        del row[k]

                rows.append(row)

            rows = sorted(rows, key = lambda x : ( x.get('table'), x.get('url'), x.get('segment')))

            r.writerows(rows)
















