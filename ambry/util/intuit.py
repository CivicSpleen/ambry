"""
Intuit data types for rows of values
"""
__author__ = 'eric'


from collections import defaultdict, deque
import datetime

def test_float(v):

    if v and v[0] == '0': #Fixed-width integer codes are actually strings.
        return 0

    try:
        float(v)
        return 1
    except:
        return 0

def test_int(v):

    if v and v[0] == '0': #Fixed-width integer codes are actually strings.
        return 0

    try:
        if float(v) == int(v):
            return 1
        else:
            return 0
    except:
        return 0

def test_string(v):
    if isinstance(v, basestring):
        return 1
    else:
        return 0

def test_datetime(v):
    """Test for ISO datetime"""
    if not isinstance(v, basestring):
        return 0

    if len(v) > 22:
        # Not exactly correct; ISO8601 allows fractional seconds
        # which could result in a longer string.
        return 0

    if '-' not in v and ':' not in v:
        return 0

    for c in set(v):  # Set of Unique characters
        if not c.isdigit() and c not in 'T:-Z':
            return 0

    return 1

def test_time(v):
    if not isinstance(v, basestring):
        return 0

    if len(v) > 15:
        return 0

    if ':' not in v:
        return 0

    for c in set(v):  # Set of Unique characters
        if not c.isdigit() and c not in 'T:Z.':
            return 0

    return 1


def test_date(v):

    if not isinstance(v, basestring):
        return 0

    if len(v) > 10:
        # Not exactly correct; ISO8601 allows fractional seconds
        # which could result in a longer string.
        return 0

    if '-' not in v:
        return 0

    for c in set(v):  # Set of Unique characters
        if not c.isdigit() and c not in '-':
            return 0

    return 1

tests = [
    (int, test_int),
    (float, test_float),
    (str,test_string),
]



class Column(object):

    name = None
    type_counts = None
    type_ratios = None
    length = 0
    count = 0
    strings = None

    def __init__(self):
        self.type_counts = { k:0 for k,v in tests}
        self.type_counts[datetime.datetime] = 0
        self.type_counts[datetime.date] = 0
        self.type_counts[datetime.time] = 0
        self.type_counts[None] = 0
        self.strings = deque(maxlen=1000)
        self.count = 0
        self.length = 0
        self.date_successes = 0

    def inc_type_count(self,t):
        self.type_counts[t] +=  1

    def test(self, v):
        from dateutil import parser

        self.length = max(self.length,len(str(v)))
        self.count += 1

        if v is None:
            self.type_counts[None] += 1
            return None

        v = str(v)

        try:
            v = v.strip()
        except AttributeError:
            pass

        if v == '':
            self.type_counts[None] += 1
            return None

        type_ = None

        for test, testf in tests:
            t = testf(v)

            if t > 0:
                type_ = test

                if test == str:
                    if v not in self.strings:
                        self.strings.append(v)

                    if (self.count < 1000 or self.date_successes != 0) and any(( c in '-/:T') for c in v):
                        try:
                            maybe_dt = parser.parse(v, default=datetime.datetime.fromtimestamp(0) )
                        except (TypeError, ValueError):
                            maybe_dt = None

                        if maybe_dt:
                            # Check which parts of the default the parser didn't change to find
                            # the real type
                            # HACK The time check will be wrong for the time of the start of the epoch, 16:00.
                            if maybe_dt.time() == datetime.datetime.fromtimestamp(0).time():
                                type_ = datetime.date
                            elif maybe_dt.date() == datetime.datetime.fromtimestamp(0).date():
                                type_ =  datetime.time
                            else:
                                type_ =  datetime.datetime

                            self.date_successes += 1

                self.type_counts[type_] += 1

                return type_


    def resolved_type(self):
        "Return the type for the columns, and a flag to indicate that the column has codes"
        import datetime

        self.type_ratios = { test:float(self.type_counts[test]) / float(self.count) for test, testf in tests + [(None, None)] }

        if self.type_ratios[str] > .2:
            return str, False

        if self.type_ratios[None] > .7:
            return str, False

        if self.type_counts[datetime.datetime] > 0:
            num_type = datetime.datetime

        elif self.type_counts[datetime.date] > 0:
            num_type = datetime.date

        elif self.type_counts[datetime.time] > 0:
            num_type = datetime.time

        elif self.type_counts[float] > 0:
            num_type = float
        else:
            num_type = int

        if self.type_counts[str] > 0:
            return num_type, True
        else:
            return num_type, False

class Intuiter(object):
    """Determine the types of rows in a table """
    header = None
    counts = None

    def __init__(self, header = None, logger = None):
        self.header = header
        self._columns = defaultdict(Column)
        self.logger = logger

    def add_row(self,row):

        if self.logger:
            self.logger("Intuit Row")

        if isinstance(row, dict):
            for k,v in row.items():
                self._columns[k].test(v)
        elif isinstance(row, (list, tuple)):
            if isinstance(row[0], (tuple, list)):
                # The row is actually a tuple of (header, row)
                for k, v in enumerate(row[1]):
                    type_ = self._columns[k].test(v)
            else:
                for k, v in enumerate(row):
                    type_  = self._columns[k].test(v)

        else:
            raise TypeError("Row must be a list or a dict")

    def iterate(self, itr, max_n = None):

        for n,row in enumerate(itr):


            if max_n and n > max_n:
                return

            self.add_row(row)


    @property
    def columns(self):

        for k, v in self._columns.items():

            if self.header:
                v.name = self.header[k]
            else:
                v.name = k

            yield v

    def dump(self):

        for v in self.columns:

            print v.name, v.resolved_type()
