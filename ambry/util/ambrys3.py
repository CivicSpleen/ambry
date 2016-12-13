"""Overides a method in the PyFS S3 object that doesn't behave well with the permissions structure
defined in the ambry-aws cli plugin.

Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from fs.s3fs import S3FS

class AmbryS3FS(S3FS):
    def _s3bukt(self):
        """ Overrides the original _s3bukt method to get the bucket without vlaidation when
        the return to the original validation is not a 404.
        :return:
        """
        from boto.exception import S3ResponseError
        import time

        try:
            (b, ctime) = self._tlocal.s3bukt
            if time.time() - ctime > 60:
                raise AttributeError
            return b
        except AttributeError:

            try:
                # Validate by listing the bucket if there is no prefix.
                # If there is a prefix, validate by listing only the prefix
                # itself, to avoid errors when an IAM policy has been applied.
                if self._prefix:
                    b = self._s3conn.get_bucket(self._bucket_name, validate=0)
                    b.get_key(self._prefix)
                else:
                    b = self._s3conn.get_bucket(self._bucket_name, validate=1)
            except S3ResponseError as e:

                if "404 Not Found" in str(e):
                    raise

                b = self._s3conn.get_bucket(self._bucket_name, validate=0)

            self._tlocal.s3bukt = (b, time.time())
            return b

    _s3bukt = property(_s3bukt)