"""
Interfaces for webservice gocoders
"""

class DstkGeocoder(object):
    """A Batch geocoder interface for the DataScienceToolkit server"""
    submit_size = 100

    def __init__(self, url, address_gen):
        import dstk
        self.url = url
        self.gen = address_gen

        self.dstk_client = dstk.DSTK(self.url)

    def geocode(self):
        """A Generator that reads from the address generators and returns geocode results. """


        submit_set = []
        data_map = {}

        for address, o in self.gen:
            submit_set.append(address)
            data_map[address] = o

            if len(submit_set) >= self.submit_size:
                results = self._send(submit_set)
                submit_set = []

                for k,result in results.items():
                    o = data_map[k]
                    yield (k,result, o)


        if len(submit_set) > 0:
            results = self._send(submit_set)
            submit_set = []

            for k, result in results.items():
                o = data_map[k]
                yield (k,result, o)

    def _send(self, addr_set):
        try:
            results = self.dstk_client.street2coordinates(addr_set)
            return results

        except UnicodeDecodeError:
            # DSTK occasionally thows decode errors on the data, on the way back.
            # No idea what the problem is.
            results = {}

            for addr in addr_set:
                try:
                    result = self.dstk_client.street2coordinates(addr)
                    results[addr] = result
                except UnicodeDecodeError:
                    results[addr] = None

            return results

