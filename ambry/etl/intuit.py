"""Intuit data types for rows of values."""
__author__ = 'eric'

from collections import deque
import datetime

from ambry.etl.pipeline import Pipe
from ambry.util import get_logger

logger = get_logger(__name__)


class unknown(str):

    __name__ = 'unknown'

    def __new__(cls):
        return super(unknown, cls).__new__(cls, cls.__name__)

    def __str__(self):
        return self.__name__

    def __eq__(self, other):
        return str(self) == str(other)


def test_float(v):
    # Fixed-width integer codes are actually strings.
    # if v and v[0]  == '0' and len(v) > 1:
    # return 0

    try:
        float(v)
        return 1
    except:
        return 0

def test_int(v):
    # Fixed-width integer codes are actually strings.
    # if v and v[0] == '0' and len(v) > 1:
    # return 0

    try:
        if float(v) == int(float(v)):
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
    """Test for ISO datetime."""
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
    (str, test_string),
]


class Column(object):
    position = None
    header = None
    type_counts = None
    type_ratios = None
    length = 0
    count = 0
    strings = None

    def __init__(self):
        self.type_counts = {k: 0 for k, v in tests}
        self.type_counts[datetime.datetime] = 0
        self.type_counts[datetime.date] = 0
        self.type_counts[datetime.time] = 0
        self.type_counts[None] = 0
        self.strings = deque(maxlen=1000)
        self.position = None
        self.header = None
        self.count = 0
        self.length = 0
        self.date_successes = 0
        self.description = None

    def inc_type_count(self, t):
        self.type_counts[t] += 1

    def test(self, v):
        from dateutil import parser

        self.length = max(self.length, len(str(v)))
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

        for test, testf in tests:
            t = testf(v)

            if t > 0:
                type_ = test

                if test == str:
                    if v not in self.strings:
                        self.strings.append(v)

                    if (self.count < 1000 or self.date_successes != 0) and any((c in '-/:T') for c in v):
                        try:
                            maybe_dt = parser.parse(
                                v, default=datetime.datetime.fromtimestamp(0))
                        except (TypeError, ValueError):
                            maybe_dt = None

                        if maybe_dt:
                            # Check which parts of the default the parser didn't change to find
                            # the real type
                            # HACK The time check will be wrong for the time of
                            # the start of the epoch, 16:00.
                            if maybe_dt.time() == datetime.datetime.fromtimestamp(0).time():
                                type_ = datetime.date
                            elif maybe_dt.date() == datetime.datetime.fromtimestamp(0).date():
                                type_ = datetime.time
                            else:
                                type_ = datetime.datetime

                            self.date_successes += 1

                self.type_counts[type_] += 1

                return type_


    def _resolved_type(self):
        """Return the type for the columns, and a flag to indicate that the
        column has codes."""
        import datetime

        self.type_ratios = {test: (float(self.type_counts[test]) / float(self.count)) if self.count else None
                            for test, testf in tests + [(None, None)]}

        # If it is more than 20% str, it's a str
        if self.type_ratios[str] > .2:
            return str, False

        # If more than 70% None, it's also a str, because ...
        #if self.type_ratios[None] > .7:
        #    return str, False

        if self.type_counts[datetime.datetime] > 0:
            num_type = datetime.datetime

        elif self.type_counts[datetime.date] > 0:
            num_type = datetime.date

        elif self.type_counts[datetime.time] > 0:
            num_type = datetime.time

        elif self.type_counts[float] > 0:
            num_type = float

        elif self.type_counts[int] > 0:
            num_type = int

        elif self.type_counts[str] > 0:
            num_type = str

        else:
            num_type = unknown

        if self.type_counts[str] > 0 and num_type != str:
            has_codes = True
        else:
            has_codes = False

        return num_type, has_codes

    @property
    def resolved_type(self):
        return self._resolved_type()[0]

    @property
    def resolved_type_name(self):
        try:
            return self.resolved_type.__name__
        except AttributeError:
            return self.resolved_type

    @property
    def has_codes(self):
        return self._resolved_type()[1]


class TypeIntuiter(Pipe):
    """Determine the types of rows in a table."""
    header = None
    counts = None

    def __init__(self, skip_rows=1):
        from collections import OrderedDict

        self._columns = OrderedDict()
        self.skip_rows = skip_rows

    def process_row(self, n, row):

        if n == 0:
            header = row
            for i, value in enumerate(row):
                if i not in header:
                    self._columns[i] = Column()
                    self._columns[i].position = i
                    self._columns[i].header = value

            return

        if n < self.skip_rows:
            return

        try:
            for i, value in enumerate(row):
                if i not in self._columns:
                    self._columns[i] = Column()
                    self._columns[i].position = i

                self._columns[i].test(value)

        except Exception:
            # This usually doesn't matter, since there are usually plenty of other rows to intuit from
            # print 'Failed to add row: {}: {} {}'.format(row, type(e), e)
            pass

    def __iter__(self):
        for i, row in enumerate(self.source_pipe):
            self.process_row(i, row)
            yield row

    def iterate(self, row_gen, max_n=None):
        """
        :param row_gen:
        :param max_n:
        :return:
        """

        for n, row in enumerate(row_gen):

            if max_n and n > max_n:
                return

            self.process_row(n, row)

    @property
    def columns(self):

        for k, v in self._columns.items():
            v.position = k

            yield v

    def __str__(self):
        from tabulate import tabulate
        from ..util import qualified_class_name

        # return  SingleTable([[ str(x) for x in row] for row in self.rows] ).table

        results = self.results_table()

        if len(results) > 1:
            o = '\n' + str(tabulate(results[1:], results[0], tablefmt='pipe'))
        else:
            o = ''

        return qualified_class_name(self) + o

    @staticmethod
    def promote_type(orig_type, new_type):
        """Given a table with an original type, decide whether a new determination of a new applicable type
        should overide the existing one"""

        try:
            orig_type = orig_type.__name__
        except AttributeError:
            pass

        try:
            new_type = new_type.__name__
        except AttributeError:
            pass

        type_precidence = ['unknown', 'int', 'float', 'date', 'time', 'datetime', 'str']

        # TODO This will fail for dates and times.

        if type_precidence.index(new_type) > type_precidence.index(orig_type):
            return new_type
        else:
            return orig_type

    def results_table(self):

        fields = 'position header length resolved_type has_codes count ints floats strs nones datetimes dates times '.split()

        header = list(fields)
        header[0] = '#'
        header[2] = 'size'
        header[4] = 'codes?'
        header[10] = 'd/t'

        rows = list()

        rows.append(header)

        for d in self._dump():
            rows.append([d[k] for k in fields])

        return rows

    def _dump(self):

        for v in self.columns:

            d = dict(
                position=v.position,
                header=v.header,
                length=v.length,
                resolved_type=v.resolved_type_name,
                has_codes=v.has_codes,
                count=v.count,
                ints=v.type_counts.get(int, None),
                floats=v.type_counts.get(float, None),
                strs=v.type_counts.get(str, None),
                nones=v.type_counts.get(None, None),
                datetimes=v.type_counts.get(datetime.datetime, None),
                dates=v.type_counts.get(datetime.date, None),
                times=v.type_counts.get(datetime.time, None),
                strvals=','.join(list(v.strings)[:20])
            )

            yield d

class RowIntuiter(Pipe):
    """ Separates rows to the comments, header and data.

    Note: Assuming None means empty value in the cell. Empty strings (xlrd case) have to be replaced
        with None to match header and comment patterns. # FIXME: Consult Eric.
    """

    header = None
    comments = None
    errors = {}
    FIRST_ROWS = 20  # How many rows to keep in the top rows slice while looking for the comments and header.
    LAST_ROWS = 20  # How many rows to keep in the last rows slice while looking for the last row with data.
    SAMPLE_SIZE = 200  # How many rows to keep in the sample while recognizing data pattern.

    def _matches(self, row, pattern):
        """ Returns True if given row matches given patter.

        Args:
            row (list):
            pattern (list of sets):

        Returns:
            bool: True if row matches pattern. False otherwise.

        """
        for i, e in enumerate(row):
            if self._get_type(e) not in pattern[i]:
                return False
        return True

    def _find_data_lines(self, rows, data_pattern):
        """ Finds first and last rows with data

        Note: Assuming len(rows) == len(data_pattern)

        Args:
            rows (list):
            data_pattern (list of sets):

        Returns:
            tuple of int, int: (first line index, last line index)
        """

        first_rows = rows[:self.FIRST_ROWS]  # The first 20 lines ( Header section )
        last_rows = rows[-self.LAST_ROWS:]  # The last 20 lines ( Footer section )

        first_line = None
        last_line = None

        # iterate header to find first line with data.
        for i, row in enumerate(first_rows):
            if self._matches(row, data_pattern):
                first_line = i
                break

        # iterate footer from the end to find last row with data.
        for i, row in enumerate(reversed(last_rows)):
            if self._matches(row, data_pattern):
                last_line = len(rows) - i
                break

        assert first_line is not None
        assert last_line is not None
        return first_line, last_line

    def _find_header(self, first_rows, header_pattern):

        MATCH_THRESHOLD = 0.4  # Ratio of the strings in the row to consider it as header.

        for row in first_rows:
            if self._matches(row, header_pattern):

                str_matches = 0
                for elem in row:
                    if isinstance(elem, (str, unicode)):
                        str_matches += 1
                if float(str_matches) / float(len(row)) >= MATCH_THRESHOLD:
                    return row
        self.errors['no-header'] = 'Header row was not found.'

    def _find_comments(self, rows, comments_pattern):
        """ Finds comments in the rows using comments pattern.

        Args:
            rows: rows where to look for comments.
            comments_pattern: pattern to match against to.

        Returns:
            list: list with comments or empty list if no comments found.
        """
        comments = []
        for row in rows:
            if self._matches(row, comments_pattern):
                comment = ' '.join([x for x in row if x])
                if comment:
                    comments.append(comment)
        return comments

    def _get_patterns(self, rows):
        """ Finds comments, header and data patterns in the rows.

        Args:
            row (list):

        Returns:
            tuple of comments_pattern, header_pattern, data_pattern.

        """
        assert len(rows) > self.FIRST_ROWS + self.LAST_ROWS, 'Number of rows is not enough to recognize patter.s.'
        data_rows = rows[self.FIRST_ROWS:-self.LAST_ROWS]
        data_sample = data_rows[:self.SAMPLE_SIZE]
        data_pattern = [set() for x in range(len(data_rows[0]))]

        for row in data_sample:
            for i, column in enumerate(row):
                data_pattern[i].add(self._get_type(column))

        #
        # Comments pattern - first two columns are string or None, other columns are None.
        #
        comments_pattern = [set([None]) for x in range(len(rows[0]))]
        comments_pattern[0].add(str)
        comments_pattern[1].add(str)

        #
        # Header pattern.
        #

        header_pattern = [set([str, None]) for x in data_pattern]
        return comments_pattern, header_pattern, data_pattern

    def _is_float(self, value):
        """ Returns True if value contains float number.

        """
        ret = False
        if isinstance(value, float):
            ret = True
        if isinstance(value, basestring) and value.count('.') == 1 and value.replace('.', '1').isdigit():
            ret = True
        logger.debug(
            u'Determining float: value: {}, type: {}, is_float: {}'.format(value, type(value), ret))
        return ret

    def _is_int(self, value):
        """ Returns True if value contains int. Otherwise returns False.

        """
        ret = False
        if isinstance(value, int):
            ret = True
        if isinstance(value, basestring):
            ret = value.isdigit()
        logger.debug(
            u'Determining int: value: {}, type: {}, is_int: {}'.format(value, type(value), ret))
        return ret

    def _get_type(self, value):
        """ Determines and returns type of the value.

        Args:
            value (any):

        Returns:
            type: int or float or str or unicode.
        """
        if self._is_float(value):
            return float
        if self._is_int(value):
            return int
        if test_string(value):
            return str

    def __iter__(self):
        """ Generates rows with data.

        Yields:
            list

        """

        rows = list(self._source_pipe)

        comments_pattern, header_pattern, data_pattern = self._get_patterns(rows)

        # save comments
        self.comments = self._find_comments(rows[:self.FIRST_ROWS], comments_pattern)

        # save header
        self.header = self._find_header(rows[:self.FIRST_ROWS], header_pattern)

        # find data and generate data
        first_line, end_line = self._find_data_lines(rows, data_pattern)

        for row in rows[first_line:end_line]:
            yield row

        self.finish()
