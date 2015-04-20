"""Interfaces for webservice gocoders."""


class DstkGeocoder(object):

    """A Batch geocoder interface for the DataScienceToolkit server."""
    submit_size = 100

    def __init__(self, options, address_gen):
        """Batch geocode addresses using DSTK.

        :param url: URL to a DTSK server
        :param address_gen: A generator that yields tuples of (address, object), where address is an address string.
            The address is geocoded, and the object is passed thorugh to the result.
        :return:

        """
        import dstk

        if isinstance(options, basestring):
            # Single string, not an options dict
            options = {'apiBase': options}

        self.gen = address_gen

        self.dstk_client = dstk.DSTK(options)

    def geocode(self):
        """A Generator that reads from the address generators and returns
        geocode results.

        The generator yields ( address, geocode_results, object)

        """

        submit_set = []
        data_map = {}

        for address, o in self.gen:
            submit_set.append(address)
            data_map[address] = o

            if len(submit_set) >= self.submit_size:
                results = self._send(submit_set)
                submit_set = []

                for k, result in results.items():
                    o = data_map[k]
                    yield (k, result, o)

        if len(submit_set) > 0:
            results = self._send(submit_set)
            submit_set = []

            for k, result in results.items():
                o = data_map[k]
                yield (k, result, o)

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
