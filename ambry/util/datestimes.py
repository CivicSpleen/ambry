"""Utilities for dealing with time"""


def expand_to_years(dates):
    """
    Expand a single string or integer date, or an iterable, into a set of years. Parses strings to date times or
    durations, and extracts the year, but passes integers though if they are four digits.

    :param dates: A string, integer or iterable producing strings or ints
    :return: an array of integer years
    """
    import isodate
    import datetime

    if not dates:
        return []

    if not isinstance(dates, (basestring, int)):  # Can't EAFP since strings are iterable
        import itertools
        return sorted(set(itertools.chain(*[expand_to_years(x) for x in dates])))

    def make_year_array(d):

        return [isodate.parse_date(str(int(d))).year]

    # Ints and int-convertable strings just pass though
    try:
        return make_year_array(dates)
    except ValueError:
        pass

    try:
        dates = str(dates).upper()  # Ambry tends to lowercase things
        parts = dates.replace('E', '/').split('/')  # / is in std; ambry uses 'e' to be compat with urls.

        rparts = []

        for p in parts:
            try:
                rparts.append(isodate.parse_date(p))
            except isodate.isoerror.ISO8601Error:
                try:
                    rparts.append(isodate.parse_duration(p))
                except:
                    raise

        types = tuple(type(x) for x in rparts)

        if types == (datetime.date, isodate.duration.Duration):
            start = rparts[0].year
            end = start + int(rparts[1].years)
        elif types == (isodate.duration.Duration, datetime.date):
            end = rparts[1].year + 1
            start = end - int(rparts[0].years)
        elif types == (datetime.date, datetime.date):
            start = rparts[0].year
            end = rparts[1].year + 1

        else:
            raise ValueError()

        return sorted(range(start, end))

    except ValueError:
        pass

    try:
        return make_year_array(isodate.parse_date(dates).year)
    except isodate.isoerror.ISO8601Error:
        pass

    return []


def compress_years(dates):
    """Given a set of values that can be input for expand_to_years, expand and then
    return as a simple string in the form start/end"""

    years = expand_to_years(dates)

    if not years:
        return ''

    return "{}/{}".format(min(years), max(years))












