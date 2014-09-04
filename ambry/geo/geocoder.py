"""
Functions for geocoding using Ambry data.
"""


class Geocoder(object):

    def __init__(self,partition, city_subs=None):
        from address_parser import Parser

        self.p = partition

        self.address_cache = {}

        self.city_subs = { k.lower():v for k,v in city_subs.items() } if city_subs else {}

        self.parser =  Parser()

    def parse_and_code(self, addrstr, city=None, state=None, zip=None):


        adr = self.parser.parse(addrstr, city=city, state=state, zip=zip)

        if adr.hash in self.address_cache:
            r = self.address_cache[adr.hash]
            if r:
                address_id = r['address_id']
            else:
                address_id = None

        else:
            r = self.geocode(**adr.args)

            if r:
                address_id = r['address_id']
                self.address_cache[adr.hash] = r
            else:
                self.address_cache[adr.hash] = None
                address_id = None

        return  address_id, r, adr


    def geocode(self, number, name, direction=None,
                suffix=None, city=None, state=None, zip=None):
        '''Return a record from the geocoder table.

        This function expects a partition, p, that holds a table named 'gecoder',
        of the same structure as used in clarinova.com-geocode-casnd
        '''

        direction = direction.upper() if direction else '-'
        suffix = suffix.title() if suffix else '-'
        city = city.title() if city else '-'


        if city.lower() in self.city_subs:
            city = self.city_subs[city.lower()].title()

        if isinstance(zip, basestring ) and '-' in zip:
            zip, zsuffix = zip.split('-')

        zip = zip if zip else -1

        try:
            zip = int(zip)
        except:
            zip = -1

        suffix = suffix.lower()

        # We don't need to check for nulls in direction, b/c entries without
        # directions have the value '-'
        q = """
        SELECT
            *,
            (
                CASE WHEN city = :city THEN 10 ELSE 0 END +
                CASE WHEN zip = :zip THEN 10 ELSE 0 END +
                CASE WHEN suffix = :suffix THEN 10 ELSE 0 END
            ) AS score,
            ABS(number - :number) as ndist

        FROM geocoder
        WHERE  name = :name AND direction = :direction
        AND score >= 20
        AND number BETWEEN (:number-100) AND (:number+100)
        ORDER BY ABS(number - :number), score LIMIT 1;
        """

        r =  self.p.query(q, number=number, name=name, direction=direction,
                            suffix=suffix,city=city, state=state, zip=zip).first()

        if not r:
            return None

        r = dict(r)
        r['confidence'] = round((100.0 - ( 30.0 - r['score']) - (r['ndist'] / 2.0))/100.0,3)
        r['lat'] = float(r['lat']) / 100000000.0
        r['lon'] = float(r['lon']) / 100000000.0
        return r

    def geocode_intersection(self, street1, street2):
        pass




