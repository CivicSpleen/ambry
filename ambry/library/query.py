"""A Library is a local collection of bundles.

It holds a database for the configuration of the bundles that have been
installed into it.

"""

# Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt


from ambry.orm import Dataset, Partition, File
from ambry.orm import Table, Column
from ..identity import Identity, PartitionNumber, DatasetNumber


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

            q = (self.session.query(Dataset, File).outerjoin(File, File.ref == Dataset.vid)
                 .filter(dqp).order_by(Dataset.revision.desc()))

            for row in (q.all()):
                out.append((row.Dataset, None, row.File))

        if pqp is not None:

            q = (self.session.query(Dataset, Partition, File).join(Partition)
                .filter(pqp).outerjoin(File, File.ref == Partition.vid)
                .order_by(Dataset.revision.desc()))

            for row in q.all():
                out.append((row.Dataset, row.Partition, row.File))


        return ip, out

    def _resolve_ref(self, ref):
        """Convert the output from _resolve_ref to nested identities."""

        from collections import OrderedDict
        from ..identity import LocationRef

        ip, results = self._resolve_ref_orm(ref)


        # Convert the ORM results to identities
        out = OrderedDict()

        for d, p, f in results:

            if not d.vid in out:
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
                    d_f_type = { lrc.REMOTEPARTITION: lrc.REMOTE,lrc.PARTITION: lrc.LIBRARY}.get( f.type_, None)
                    out[d.vid].locations.set(d_f_type)

            else:

                out[d.vid].locations.set(LocationRef.LOCATION.LIBRARY)


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

            refs = OrderedDict([(k, v) for k, v in refs.items() if v.locations.has(location)])

        if not isinstance(ip.version, semantic_version.Spec):
            return ip, refs.values().pop(0) if refs and len(refs.values()) else None
        else:

            versions = { semantic_version.Version(d.name.version): d for d in refs.values()}

            best = ip.version.select(versions.keys())

            if not best:
                return ip, None
            else:
                return ip, versions[best]

    def resolve(self, ref):
        return self.resolve_ref_one(ref)[1]
