"""
Functions for geocoding using Ambry data.
"""


class Geocoder(object):

    def __init__(self,partition):
        from address_parser import Parser

        self.p = partition

        self.address_cache = {}

        self.parser =  Parser()

    def parse_and_code(self, addrstr, city=None, state=None, zip=None):


        adr = self.parser.parse(addrstr, city=city, state=state, zip=zip)

        if adr.hash in self.address_cache:
            address_id = self.address_cache[adr.hash]
        else:
            r = self.geocode(**adr.args)
            if r:
                address_id = r['address_id']
                self.address_cache[adr.hash] = address_id
            else:
                self.address_cache[adr.hash] = None
                address_id = None

        return  address_id, adr


    def geocode(self, number, name, direction=None,
                suffix=None, city=None, state=None, zip=None):
        '''Return a record from the geocoder table.

        This function expects a partition, p, that holds a table named 'gecoder',
        of the same structure as used in clarinova.com-geocode-casnd
        '''

        direction = direction if direction else '-'
        suffix = suffix if suffix else '-'
        city = city if city else '-'
        zip = zip if zip else -1

        try:
            zip = int(zip)
        except:
            zip = -1

        q = """
        SELECT
            *,
            (
                CASE WHEN city = :city THEN 10 ELSE 0 END +
                CASE WHEN zip = :zip THEN 10 ELSE 0 END +
                CASE WHEN suffix = :suffix THEN 10 ELSE 0 END
            ) AS score
        FROM geocoder
        WHERE  name = :name AND direction = :direction
        AND score >= 20
        AND number BETWEEN (:number-100) AND (:number+100)
        ORDER BY ABS(number - :number) LIMIT 1;
        """

        return self.p.query(q, number=number, name=name, direction=direction, suffix=suffix,
                                       city=city.title(), state=state.upper(), zip=int(zip)).first()


    def geocode_intersection(self, street1, street2):
        pass




