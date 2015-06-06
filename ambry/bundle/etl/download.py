"""Download from the web, cache the results, and possibly unzip

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import sys
import zipfile
import os

# Monkey Patch!
# Need to patch zipfile.testzip b/c it doesn't close file descriptors in 2.7.3
# The bug apparently exists in several other versions of python
# http://bugs.python.org/issue16408
if sys.hexversion < int(hex('0x20706f0'), 16):
    def testzip(self):
        """Read all the files and check the CRC."""
        chunk_size = 2 ** 20
        for zinfo in self.filelist:
            try:
                # Read by chunks, to avoid an OverflowError or a
                # MemoryError with very large embedded files.
                f = self.open(zinfo.filename, "r")
                while f.read(chunk_size):  # Check CRC-32
                    pass
                f.close()
                f._fileobj.close()  # This shouldn't be necessary, but it is.
            except zipfile.BadZipfile:
                return zinfo.filename


    zipfile.ZipFile.testzip = testzip

class DownloadFailedError(Exception):
    pass


def download(self, url, test_f=None, unzip=False, force=False):
    """ Download a file

    :param url:
    :param test_f:
    :param unzip:
    :param force: If true, ignore the cache. Required to force a check for changes to the remote file.
    :return:
    """

    import tempfile
    import urlparse
    import urllib2
    import urllib
    from orm import File

    cache = self.get_cache_by_name('downloads')
    parsed = urlparse.urlparse(str(url))
    source_entry = None

    # If the URL doesn't parse as a URL, then it is a name of a source.
    if not parsed.scheme and url in self.bundle.metadata.sources:

        source_entry = self.bundle.metadata.sources.get(url)

        # If a conversion exists, load it, otherwize, get the original URL
        if source_entry.conversion:
            url = source_entry.conversion
        else:
            url = source_entry.url
        parsed = urlparse.urlparse(str(url))

    if parsed.scheme == 'file' or not parsed.scheme:
        return parsed.path

    elif parsed.scheme == 's3':
        # To keep the rest of the code simple, we'll use the S# cache to generate a signed URL, then
        # download that through the normal process.
        from ckcache import new_cache, parse_cache_string

        bucket = parsed.netloc.strip('/')

        cache_url = "s3://{}".format(bucket)

        config = parse_cache_string(cache_url)

        config['account'] = self.config.account(bucket)

        s3cache = new_cache(config)

        url = s3cache.path(urllib.unquote_plus(parsed.path.strip('/')))
        parsed = urlparse.urlparse(str(url))
        use_hash = False
    else:
        use_hash = True

    file_path = os.path.join(parsed.netloc, parsed.path.strip('/'))

    # S3 has time in the query, so it never caches
    if use_hash and parsed.query:
        import hashlib

        hash = hashlib.sha224(parsed.query).hexdigest()
        file_path = os.path.join(file_path, hash)

    file_path = file_path.strip('/')

    # We download to a temp file, then move it into place when
    # done. This allows the code to detect and correct partial
    # downloads.
    download_path = os.path.join(tempfile.gettempdir(), file_path + ".download")

    def test_zip_file(f):
        if not os.path.exists(f):
            raise Exception("Test zip file does not exist: {} ".format(f))

        try:
            with zipfile.ZipFile(f) as zf:
                return zf.testzip() is None
        except zipfile.BadZipfile:
            return False

    if test_f == 'zip':
        test_f = test_zip_file

    for attempts in range(3):

        if attempts > 0:
            self.bundle.error("Retrying download of {}".format(url))

        out_file = None
        excpt = None

        try:

            cached_file = cache.get(file_path)
            size = os.stat(cached_file).st_size if cached_file else None

            if cached_file and size and not force:

                out_file = cached_file

                if test_f and not test_f(out_file):
                    cache.remove(file_path, True)
                    raise DownloadFailedError("Cached Download didn't pass test function " + url)

                process = File.PROCESS.CACHED

            else:

                self.bundle.log("Downloading " + url)
                self.bundle.log("  --> " + cache.path(file_path, missing_ok=True))

                resp = urllib2.urlopen(url)
                # headers = resp.info()  # @UnusedVariable
                resp.info()

                if resp.getcode() is not None and resp.getcode() != 200:
                    raise DownloadFailedError("Failed to download {}: code: {} ".format(url, resp.getcode()))

                try:
                    out_file = cache.put(resp, file_path)
                except:
                    self.bundle.error("Caught exception, deleting download file")
                    cache.remove(file_path, propagate=True)
                    raise

                if test_f and not test_f(out_file):
                    cache.remove(file_path, propagate=True)
                    raise DownloadFailedError("Download didn't pass test function " + url)

                process = File.PROCESS.DOWNLOADED

            break

        except KeyboardInterrupt:
            print "\nRemoving Files! \n Wait for deletion to complete! \n"
            cache.remove(file_path, propagate=True)
            raise
        except DownloadFailedError as e:
            self.bundle.error("Failed:  " + str(e))
            excpt = e
        except IOError as e:
            self.bundle.error("Failed to download " + url + " to " + file_path + " : " + str(e))
            excpt = e
        except urllib.ContentTooShortError as e:
            self.bundle.error("Content too short for " + url)
            excpt = e
        except zipfile.BadZipfile as e:
            # Code that uses the yield value -- like th filesystem.unzip method
            # can throw exceptions that will propagate to here. Unexpected, but very useful.
            # We should probably create a FileNotValueError, but I'm lazy.
            self.bundle.error("Got an invalid zip file for " + url)
            cache.remove(file_path, propagate=True)
            excpt = e

        except Exception as e:
            self.bundle.error("Unexpected download error '" + str(e) + "' when downloading " + str(url))
            cache.remove(file_path, propagate=True)
            raise

    if download_path and os.path.exists(download_path):
        os.remove(download_path)

    if excpt:
        raise excpt

    self._record_file(url, out_file, process)

    if unzip:

        if isinstance(unzip, bool):
            return self.unzip(out_file)
        elif unzip == 'dir':
            return self.unzip_dir(out_file)
        else:
            return self.unzip_dir(out_file, regex=unzip)

    else:
        return out_file
