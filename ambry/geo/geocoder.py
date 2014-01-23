'''

'''

from ..util import  lru_cache
import osr

class Geocoder(object):

    def __init__(self, library, **kwargs):
        """
        
        Args:
            geocoder_ds: addresses dataset dependency name. Defaults to 'addresses'
        """
        
        from ambry.geo.address import Parser
        from ambry.dbexceptions import ConfigurationError
        
        self.parser = Parser()
        
        addressesds = kwargs.get('geocoder_ds', 'geocoder')
        
        
        try:
            self.addresses = library.dep(addressesds).partition
         
        except ConfigurationError:
            raise ConfigurationError(("MISSING DEPENDENCY: To get addresses or codes, the configuration  "+
                "must specify a dependency with a set named '{0}', in build.dependencies.{0}"+
                "See https://github.com/clarinova/ambry/wiki/Error-Messages#geogeocodergeocoder__init__")
                .format(addressesds))

        self.by_scode, self.by_name = self.jur_codes()
         
    def get_srs(self):
        return self.addresses.get_srs() 
        
    @lru_cache(maxsize=2000)
    def geocode(self, street):
        """Calls either geocode_street() geocode_intersection()"""
        
        result = None
        if ' / ' in  street:
            
            s1, s2 = street.split('/',1)
            result = self.geocode_intersection(s1,s2)
       
        else:
            try:address = self.geocode_street(street)
            except Exception as e:
                raise
                address = None

            if address and address['score'] > 20:
                result = address
                
        return result
          
    def geocode_address(self, address):
        """Geocode an address"""
       
        # Current implementation just interpolates from the low to high address
       
        try: ps = self.parser.parse(address)
        except: ps = False
    
        if not ps:
            return None   
       
        r = {'address': None, 'segment': None}

        segment = self._geocode_segment(ps)

       
        if not segment:
            return None

        r['segment'] = segment

        # Try to get a specific address within the segment. 
        if ps.number <= segment['hnumber'] and ps.number >= segment['lnumber']:
           
            address = self.addresses.query("""
                SELECT * FROM addresses WHERE segment_source_id = ? 
                ORDER BY ABS(number - ?) ASC LIMIT 1""", segment['segment_source_id'], ps.number).first()
              
            if address:
                if  abs( int(address['number']) - ps.number) < (segment['hnumber'] - segment['lnumber']) :
                    r['address'] = address
                    r['gctype'] = 'cns/address'
                    x = address['x']
                    y = address['y']
                    r['gcquality'] = abs( int(address['number']) - ps.number)
                    ps.number = address['number'] # For re-coding the address later. 
                else:
                    pass
                    #print abs( int(address['number']) - ps.number), address['number'],  segment['lnumber'],ps.number, segment['hnumber']
                

        if not r['address']:
            try:
                
                number = ps.number
                number_low = segment['lnumber']
                number_high = segment['hnumber']
                x1,y1,x2,y2 = segment['x1'], segment['y1'], segment['x2'], segment['y2']
                
                m = float(y2-y1) / float(x2-x1)
                b = y1 - m*x1
                
                # Ratio to convert house number units into x units. 
                ntx = float(x2-x1) / float(  number_high - number_low )
                x = ntx*(number-number_low)+x1
                
                # Plain old linear equation
                y = m*x + b 
                
                r['gctype'] = 'cns/seginterp'
                
                #
                #  Punt and use the midpoint if the difference is too large. 
                #
                import math
                
                # Distance from the address to the midpoint
                xd = segment['xm'] - x
                yd = segment['ym'] - y
                d1 = math.sqrt(xd*xd + yd*yd)
    
                # distance from end to end. 
                xd = x1 - x2
                yd = y1 - y2
                d2 = math.sqrt(xd*xd + yd*yd)
                
                r['gcquality'] = int(d2 - d1)
                
                if d1 > d2:
                    raise Exception()
                    #print "d: ", r, d1, d2
                    #print "n: ", number, number_high, number_low
                
            except:
                x = segment['xm']
                y = segment['ym']
                r['gctype'] = 'cns/segmid'
                r['gcquality'] = 0
    
        #ps.number = segment['number']
        
        ps.city = segment['city']
        ps.street_name = segment['name']
        ps.street_type = segment['type']
        ps.street_direction = segment['dir']

        r['x'] = x
        r['y'] = y
        
        if str(ps):
            r['codedaddress'] = str(ps)
        else:
            r['codedaddress'] = None

        r = { k:v.strip() if isinstance(v, basestring) and v.strip() != '-' else v for k,v in r.items() }

        return r
        
    def geocode_street(self, street):
        """Geocode an address to a street segment"""
        try: ps = self.parser.parse(street)
        except: ps = False
    
        if not ps:
            return None       
        
        return self._geocode_segment(ps)
         
    def _geocode_segment(self, ps):
        """Geocode an address to a street segment"""

        direction = ps.street_direction
        street = ps.street_name
        street_type = ps.street_type
        number = ps.number
        
        q = """SELECT  * FROM segments WHERE name = ?"""

        # If this fails, the "city" is probably an unincorporated place, which is in the county. 
        try: in_city = self.by_name[ps.city.title()]
        except: in_city = self.by_name['NONE']
           
        max_score = 0
        winner = None
       
        for s in self.addresses.query(q, street):
            s= dict(s)
             
            s['score']  = score = self.rank_street(s, number,  direction, street_type, in_city)

            if in_city == s['rcity']:
                s['city'] = s['rcity']   
            elif in_city == s['lcity']:
                s['city'] = s['lcity']   
            else:
                s['city'] = None                 

            if not winner or score > max_score:
                winner = s
                max_score = score

        if winner:
            
            winner['lat'] = winner['latc']
            winner['lon'] = winner['lonc']
            winner['x'] = winner['xc']
            winner['y'] = winner['yc']
            winner['gctype'] = 'cns/segment'
            winner['gcquality'] = winner['score']     
            
        return winner

    def geocode_intersection(self, street1, street2):

        try: 
            ps1 = self.parser.parse(street1)
            ps2 = self.parser.parse(street2)
        except:
            return None
    
        if not ps1 or not ps2:
            return None

        q = """SELECT  * FROM nodes 
        WHERE street_1 = ? and street_2 = ?
        OR street_1 = ? and street_2 = ? LIMIT 1"""

        intr = self.addresses.query(q, ps1.street_name, ps2.street_name, ps2.street_name, ps1.street_name).first()

        if intr:
            winner = dict(intr)
            winner['gctype'] = 'cns/intersection'
            return winner
        else:
            return None
        
    def jur_codes(self):
        
        by_scode = {}
        by_name = {}
        for place in self.addresses.query("SELECT code, scode, name FROM places WHERE type = 'city'"):
            by_scode[place['scode']] = (place['code'], place['name'])
            by_name[place['name']] = place['code']
          
        by_name['County Unincorporated'] = 'SndSDO'
        by_name['Unincorporated'] = 'SndSDO'
        by_name['NONE'] = 'SndSDO'
          
        return by_scode, by_name
       
    def rank_street(self, row, number, direction, street_type, city ):
        """ Create a score for a street segment based on how well it matches the input"""
        
        score = 0
        
        if (row['dir'] or direction) or (not row['dir']  and not direction):
            if row['dir'] == direction:
                score += 10
            
        if row['type'] == street_type:
            score += 10    


        #print "Rank Street", city, row['rcity'], row['lcity']
        if city == row['rcity'] or city == row['lcity']:
            score += 20  

        if number >= row['lnumber'] and number <=row['hnumber']:
            score += 25
        elif number:
            numdist = min( abs(number-row['lnumber']), abs(number-row['hnumber']))
            
            if numdist < 1500:
                score += int((1500-numdist) / 100) # max of 15 points
        
    
        return score

    def _do_search(self, queries, number, street, street_type, city, state):
        from collections import defaultdict
        if not number:
            return []

        candidates = defaultdict(list)

        for quality, query, args in queries:

            for ar in self.addresses.query(query, *args  ):
                ar = dict(ar)
                
                city = city.title() if city else ar.get('city', ar.get('rcity', None))

                r = {
                    'quality': quality,
                    'addresses_id': ar.get('addresses_id'),
                    'segment_source_id':  ar.get('segment_source_id'),
                    'address_source_id': ar.get('addr_source_id'),
                    'zip': ar.get('zip'),
                    'street': ar['name'],
                    'street_dir': ar.get('dir',None),
                    'street_type': ar['type'],
                    'x': ar.get('x'),
                    'y': ar.get('y'),
                    'lat': ar.get('lat'),
                    'lon': ar.get('lon'),
                    'number': ar.get('number', ar.get('lnumber')),
                    'city' : city
                }
                
                r = { k:v if v != '-' else None for k,v in r.items() }
               
                candidates[(city,ar['name'],ar['type'])].append(r)

        if len(candidates) > 0:
            return candidates

        return []

    def get_street_addresses(self, segment_source_id):
        
        addresses = {}
        
        for ar in self.addresses.query("SELECT * FROM addresses WHERE segment_source_id = ?", segment_source_id):
            addresses[ar['number']] = dict(ar)
            
        return addresses

      
    def geocode_semiblock(self, street, city, state):
        """ Just parses the street,. Expects the city, state and zip to be broken out. """

        try: ps = self.parser.parse(street)
        except: ps = False
        
        if not ps:
            return  []

        number = ps.number
        street = ps.street_name
        street_type = ps.street_type

        if not number:
            return []

        city = city.title()
        street = street.title()

        queries = [
            ("""SELECT 10 as gcquality, * FROM segments WHERE  (lcity = ?  or rcity = ? )
            AND street = ? AND street_type = ? AND ? BETWEEN lnumber AND hnumber
            AND has_addresses = 1
            ORDER BY hnumber ASC""",(city,  city, street, street_type, number)),
                   
            ("""SELECT 9 as gcquality, * FROM segments WHERE (lcity = ?  or rcity = ? ) AND 
            street = ? AND ? BETWEEN lnumber AND hnumber
            AND has_addresses = 1
            ORDER BY hnumber ASC""",(city, city,  street, number)),
                   
            ("""SELECT 8 as gcquality, * FROM segments WHERE (lcity = ?  or rcity = ? ) AND 
            street = ? AND ? BETWEEN lnumber AND hnumber
            ORDER BY hnumber ASC""",(city, city,  street, number)),
                   
            ("""SELECT 7 as gcquality, * FROM segments WHERE street = ? AND ? BETWEEN lnumber AND hnumber
            AND has_addresses = 1
            ORDER BY hnumber ASC""",(street, number)),
                   
        ]

        for query, args in queries:

            candidates = {}
          
            for ar in self.addresses.query(query, *args  ):
                ar = dict(ar)
                
                candidates.setdefault(ar['segment_source_id'],[]).append(ar)

            if len(candidates) > 0:
                return candidates

        return {}
    
    def _address_geocode_parts(self, number, street, street_type, city, state):

        if not number:
            return []

        city = city.title()
        street = street.title()

        block_number = int(float(number)/100.0)*100

        queries = [
            (20, """SELECT * FROM addresses WHERE city = ? AND name = ? AND type = ? AND number = ?
            ORDER BY segment_source_id""",(city,  street, street_type, number )), 
            (19, """SELECT * FROM addresses WHERE city = ? AND name = ? AND number = ?
            ORDER BY segment_source_id""",(city,  street, number )), 
            (18, """SELECT * FROM addresses WHERE name = ? AND number = ?
            ORDER BY segment_source_id""",(street, number )),
            (17, """SELECT * FROM addresses WHERE city = ? AND name = ? AND type = ? AND number BETWEEN ? AND ?
            ORDER BY segment_source_id""",(city,  street, street_type, block_number,  str(int(block_number)+99)) ), 
            (16, """SELECT * FROM addresses WHERE city = ? AND name = ? AND number BETWEEN ? AND ?
            ORDER BY segment_source_id""",(city,  street, block_number,  str(int(block_number)+99)) ), 
            (15, """SELECT * FROM addresses WHERE name = ? AND number BETWEEN ? AND ?
            ORDER BY segment_source_id""",(street, block_number,  str(int(block_number)+99)) )
        ]

        return self._do_search(queries, number, street, street_type, city, state)

    def geocode_to_addresses(self, street):
        ''' '''
        try: ps = self.parser.parse(street)
        except: ps = False
    
        if not ps:
            return None       
        
        return self._address_geocode_parts(ps.number, ps.street_name, ps.street_type, ps.city, ps.state)

class PlaceCoder(object):
    
    def __init__(self, library, **kwargs):
        from ambry.dbexceptions import ConfigurationError
        
        placeds_name = 'places'
            
        try:
            self.places_partition = library.dep(placeds_name).partition
         
        except ConfigurationError:
            raise ConfigurationError(("MISSING DEPENDENCY: To get addresses or codes, the configuration  "+
                "must specify a dependency with a set named '{0}', in build.dependencies.{0}"+
                "See https://github.com/clarinova/ambry/wiki/Error-Messages#geogeocodergeocoder__init__")
                .format(placeds_name))


        self._load_wgs_envelope()
    
    
    def _load_wgs_envelope(self):
        from ..util.sortedcollection import SortedCollection
        from operator import itemgetter
        import json
        import pprint
        import ogr
       
        dest_srs = osr.SpatialReference()
        dest_srs.ImportFromEPSG(4326) # WGS 84
    
        source_srs = self.places_partition.get_srs()
        
        self.wgs_transform = osr.CoordinateTransformation(source_srs, dest_srs)
        
        places = {}
        self.envelopes = []
        for row in self.places_partition.query("SELECT *, AsText(geometry) AS wkt FROM places"):

            d = dict(row)

            g = ogr.CreateGeometryFromWkt(d['wkt'])
            g.Transform(self.wgs_transform)

            d['geometry'] = g

            places[row['places_id']] = d
            
            wgs_env = g.GetEnvelope()

            # This envelope is for the Analysis area, 
            # It is aligned to a bounding box that is larger than 
            # the region, because it contains the analysis area BB, 
            # which is expressed in UTM. 
            # wgs_env = json.loads(row['wgsenvelope'])

            d = (row['places_id'],) + tuple(wgs_env)
            self.envelopes.append(d)

        self.places = places

        self.lonrc = SortedCollection(self.envelopes,key=itemgetter(1)) 
        self.lonlc = SortedCollection(self.envelopes,key=itemgetter(2)) 
        self.latdc = SortedCollection(self.envelopes,key=itemgetter(3))
        self.latuc = SortedCollection(self.envelopes,key=itemgetter(4))
 
    def _is_in_wgs(self, lat, lon, place):
        import ogr

        p = ogr.Geometry(ogr.wkbPoint)
        p.SetPoint_2D(0, lon, lat)

        if place['geometry'].Contains(p):
            return True
        else:
            return False

    def lookup_wgs(self, lat, lon):

        s1 =  set([ x[0] for x in self.latdc[:self.latdc.find_le_index(lat)] ])
        s2 =  set([ x[0] for x in self.latuc[self.latuc.find_ge_index(lat):] ])

        s3 =  set([ x[0] for x in self.lonlc[self.lonlc.find_ge_index(lon):] ])
        s4 =  set([ x[0] for x in self.lonrc[:self.lonrc.find_le_index(lon)] ])

        places = []

        for p in  [ self.places[x] for x in s1&s2&s3&s4 ]:
            
            if self._is_in_wgs(lat, lon, p):
                places.append(p)
                
        return places
            
        
    