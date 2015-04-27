"""A Library is a local collection of bundles.

It holds a database for the configuration of the bundles that have been
installed into it.

"""

# Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt


from ambry.orm import Dataset, Partition, File
from ambry.orm import Table, Column
from ..identity import Identity, PartitionNumber, DatasetNumber


class _qc_attrdict(object):

    def __init__(self, inner, query):
        self.__dict__['inner'] = inner
        self.__dict__['query'] = query

    def __setattr__(self, key, value):
        # key = key.strip('_')
        inner = self.__dict__['inner']
        inner[key] = value

    def __getattr__(self, key):
        # key = key.strip('_')
        inner = self.__dict__['inner']

        if key not in inner:
            return None

        return inner[key]

    def __len__(self):
        return len(self.inner)

    def __iter__(self):
        return iter(self.inner)

    def items(self):
        return self.inner.items()

    def __call__(self, **kwargs):
        for k, v in kwargs.items():
            self.inner[k] = v
        return self.query


class QueryCommand(object):

    """An object that contains and transfers a query for a bundle.

    Components of the query can include.

    Identity
        id
        name
        vname
        source
        dataset
        subset
        variation
        creator
        revision


    Column
        name, altname
        description
        keywords
        datatype
        measure
        units
        universe

    Table
        name, altname
        description
        keywords

    Partition
        id
        name
        vname
        time
        space
        table
        format
        other

    When the Partition search is included, the other three components are used
    to find a bundle, then the pretition information is used to select a bundle

    All of the  values are text, except for revision, which is numeric. The text
    values are used in an SQL LIKE phtase, with '%' replaced by '*', so some
    example values are:

        word    Matches text field, that is, in it entirety, 'word'
        word*   Matches a text field that begins with 'word'
        *word   Matches a text fiels that

    """

    def __init__(self, dict_=None):

        if dict_ is None:
            dict_ = {}

        self._dict = dict_

    def to_dict(self):
        return self._dict

    def from_dict(self, dict_):
        for k, v in dict_.items():
            print "FROM DICT", k, v

    def getsubdict(self, group):
        '''Fetch a confiration group and return the contents as an
        attribute-accessible dict'''

        if group not in self._dict:
            self._dict[group] = {}

        inner = self._dict[group]
        query = self

        return _qc_attrdict(inner, query)

    class ParseError(Exception):
        pass

    @classmethod
    def parse(cls, s):

        from io import StringIO
        import tokenize
        import token

        state = 'name_start'
        n1 = None
        n2 = None
        value = None
        is_like = False

        qc = QueryCommand()

        for tt in tokenize.generate_tokens(StringIO(unicode(s)).readline):
            t_type = tt[0]
            t_string = tt[1].strip()
            pos = tt[2][0]

            line = tt[4]

            # print "{:5d} {:5d} {:15s} {:20s} {:8s}.{:8s}= {:10s} ||
            # {}".format(t_type, pos, "'"+t_string+"'", state, n1, n2, value,
            # line)

            def err(expected):
                raise cls.ParseError(
                    "Expected {} in {} at char {}, got {}, '{}' ".format(
                        expected,
                        line,
                        pos,
                        token.tok_name[t_type],
                        t_string))

            if not t_string:
                continue

            if state == 'value_continuation':
                value += t_string
                if is_like:
                    value = '%' + value + '%'
                state = 'value_continuation'
                qc.getsubdict(n1).__setattr__(n2, value.strip("'").strip('"'))

            elif state == 'name_start' or state == 'name_start_or_value':
                # First part of name
                if state == 'name_start_or_value' and t_string in ('-', '.'):
                    if is_like:
                        value = value.strip('%')

                    value += t_string
                    state = 'value_continuation'
                elif t_type == token.NAME:
                    n1 = t_string
                    state = 'name_sep'
                    is_like = False

                elif t_type == token.OP and t_string == ',':
                    state = 'name_start'

                elif t_type == token.ENDMARKER:
                    state = 'done'

                else:
                    err("NAME or ','; got: '{}'  ".format(t_string))

            elif state == 'name_sep':
                # '.' that separates names
                if t_type == token.OP and t_string == '.':
                    state = 'name_2'
                else:
                    raise err("'.'")

            elif state == 'name_2':
                # Second part of name
                if t_type == token.NAME:
                    state = 'value_sep'
                    n2 = t_string
                else:
                    raise err("NAME")

            elif state == 'value_sep':
                # The '=' that seperates name from values
                if (t_type == token.OP and t_string == '=') or (t_type == token.NAME and t_string == 'like'):
                    state = 'value'

                    if t_string == 'like':
                        is_like = True

                else:
                    raise err("'='")

            elif state == 'value':
                # The Value
                if t_type == token.NAME or t_type == token.STRING or t_type == token.NUMBER:
                    value = t_string
                    if is_like:
                        value = '%' + value + '%'

                    state = 'name_start_or_value'

                    qc.getsubdict(n1).__setattr__(
                        n2,
                        value.strip("'").strip('"'))

                else:
                    raise err("NAME or STRING")
            elif state == 'done':
                raise cls.ParseError("Got token after end")
            else:
                raise cls.ParseError(
                    "Unknown state: {} at char {}".format(state))

        return qc

    @property
    def identity(self):
        """Return an array of terms for identity searches."""
        return self.getsubdict('identity')

    @identity.setter
    def identity(self, value):
        self._dict['identity'] = value

    @property
    def table(self):
        """Return an array of terms for table searches."""
        return self.getsubdict('table')

    @property
    def column(self):
        """Return an array of terms for column searches."""
        return self.getsubdict('column')

    @property
    def partition(self):
        """Return an array of terms for partition searches."""
        return self.getsubdict('partition')

    def __str__(self):
        return str(self._dict)


class Resolver(object):

    """Find a reference to a dataset or partition based on a string, which may
    be a name or object number."""

    def __init__(self, session):

        self.session = session  # a Sqlalchemy connection

    def _resolve_ref_orm(self, ref):
        from ..identity import Locations

        ip = Identity.classify(ref)

        dqp = None  # Dataset query parts
        pqp = None  # Partition query parts

        if ip.isa == PartitionNumber:
            if ip.on.revision:
                pqp = Partition.vid == str(ip.on)
            else:
                pqp = Partition.id_ == str(ip.on)

        elif ip.isa == DatasetNumber:
            if ip.on.revision:
                dqp = Dataset.vid == str(ip.on)
            else:
                dqp = Dataset.id_ == str(ip.on)

        elif ip.vname:
            dqp = Dataset.vname == ip.vname
            pqp = Partition.vname == ip.vname

        elif ip.cache_key:
            dqp = Dataset.cache_key == ip.cache_key
            pqp = Partition.cache_key == ip.cache_key

        else:
            dqp = Dataset.name == ip.sname
            pqp = Partition.name == ip.sname

        out = []

        if dqp is not None:

            q = (self.session.query(Dataset, File)
                 .outerjoin(File, File.ref == Dataset.vid)
                 .filter(dqp)
                 .order_by(Dataset.revision.desc()))

            for row in (q.all()):
                out.append((row.Dataset, None, row.File))

        if pqp is not None:

            for row in (self.session.query(Dataset, Partition, File)
                        .join(Partition)
                        .filter(pqp)
                        .outerjoin(File, File.ref == Partition.vid)
                        .order_by(Dataset.revision.desc()).all()):

                out.append((row.Dataset, row.Partition, row.File))

        return ip, out

    def _resolve_ref(self, ref):
        """Convert the output from _resolve_ref to nested identities."""

        ip, results = self._resolve_ref_orm(ref)
        from collections import OrderedDict
        from ..identity import LocationRef

        # Convert the ORM results to identities
        out = OrderedDict()

        for d, p, f in results:

            if d.vid not in out:
                out[d.vid] = d.identity

            # Locations in the identity are set in add_file
            if f:
                if not p:
                    out[d.vid].add_file(f)
                else:
                    p.identity.add_file(f)

                    # Also need to set the location in the dataset, or the location
                    # filtering may fail later.
                    lrc = LocationRef.LOCATION
                    d_f_type = {
                        lrc.REMOTEPARTITION: lrc.REMOTE,
                        lrc.PARTITION: lrc.LIBRARY}.get(
                        f.type_,
                        None)
                    out[d.vid].locations.set(d_f_type)

            if p:
                out[d.vid].add_partition(p.identity)

        return ip, out

    def resolve_ref_all(self, ref):

        return self._resolve_ref(ref)

    def resolve_ref_one(self, ref, location=None):
        """Return the "best" result for an object specification."""
        import semantic_version
        from collections import OrderedDict

        ip, refs = self._resolve_ref(ref)

        if location:

            refs = OrderedDict(
                [(k, v) for k, v in refs.items() if v.locations.has(location)])

        if not isinstance(ip.version, semantic_version.Spec):
            return ip, refs.values().pop(0) if refs and len(
                refs.values()) else None
        else:

            versions = {
                semantic_version.Version(
                    d.name.version): d for d in refs.values()}

            best = ip.version.select(versions.keys())

            if not best:
                return ip, None
            else:
                return ip, versions[best]

    def resolve(self, ref):
        return self.resolve_ref_one(ref)[1]

    def find(self, query_command):
        """Find a bundle or partition record by a QueryCommand or Identity.

        Args:
            query_command. QueryCommand or Identity

        returns:
            A list of identities, either Identity, for datasets, or PartitionIdentity
            for partitions.

        """

        def like_or_eq(c, v):

            if v and '%' in v:
                return c.like(v)
            else:
                return c == v

        has_partition = False
        has_where = False

        if isinstance(query_command, Identity):
            raise NotImplementedError()
            out = []
            for d in self.queryByIdentity(query_command).all():
                id_ = d.identity
                d.path = os.path.join(self.cache, id_.cache_key)
                out.append(d)

        tables = [Dataset]

        if len(query_command.partition) > 0:
            tables.append(Partition)

        if len(query_command.table) > 0:
            tables.append(Table)

        if len(query_command.column) > 0:
            tables.append(Column)

        # Dataset.id_ is included to ensure result is always a tuple)
        tables.append(Dataset.id_)

        # Dataset.id_ is included to ensure result is always a tuple
        query = self.session.query(*tables)

        if len(query_command.identity) > 0:
            for k, v in query_command.identity.items():
                if k == 'id':
                    k = 'id_'
                try:
                    query = query.filter(like_or_eq(getattr(Dataset, k), v))
                except AttributeError as e:
                    # Dataset doesn't have the attribute, so ignore it.
                    pass

        if len(query_command.partition) > 0:
            query = query.join(Partition)

            for k, v in query_command.partition.items():
                if k == 'id':
                    k = 'id_'

                from sqlalchemy.sql import or_

                if k == 'any':
                    continue  # Just join the partition
                elif k == 'table':
                    # The 'table" value could be the table id
                    # or a table name
                    query = query.join(Table)
                    query = query.filter(or_(Partition.t_id == v,
                                             like_or_eq(Table.name, v)))
                elif k == 'space':
                    query = query.filter(or_(like_or_eq(Partition.space, v)))

                else:
                    query = query.filter(like_or_eq(getattr(Partition, k), v))

            if not query_command.partition.format:
                # Exclude CSV if not specified
                query = query.filter(Partition.format != 'csv')

        if len(query_command.table) > 0:
            query = query.join(Table)
            for k, v in query_command.table.items():
                query = query.filter(like_or_eq(getattr(Table, k), v))

        if len(query_command.column) > 0:
            query = query.join(Table)
            query = query.join(Column)
            for k, v in query_command.column.items():
                query = query.filter(like_or_eq(getattr(Column, k), v))

        query = query.distinct().order_by(Dataset.revision.desc())

        return query
