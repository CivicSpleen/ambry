"""Text handling and conversion utilities.

Copyright (c) 2014 Clarinova. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from six import StringIO


def generate_pdf_pages(fp, maxpages = 0, logger = None):

    from pdfminer.pdfdocument import PDFDocument
    from pdfminer.pdfparser import PDFParser
    from pdfminer.pdfdevice import PDFDevice
    from pdfminer.cmapdb import CMapDB
    from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
    from pdfminer.pdfpage import PDFPage
    from pdfminer.converter import TextConverter
    from pdfminer.layout import LAParams

    import re

    password = ''
    pagenos = set()
    imagewriter = None
    rotation = 0
    caching = True

    laparams = LAParams()

    #debug = 0
    #PDFDocument.debug = debug
    #PDFParser.debug = debug
    #CMapDB.debug = debug
    #PDFResourceManager.debug = debug
    #PDFPageInterpreter.debug = debug
    #PDFDevice.debug = debug
    #

    rsrcmgr = PDFResourceManager(caching=caching)

    pages = []

    for i, page in enumerate(PDFPage.get_pages(fp, pagenos,
                                  maxpages=maxpages, password=password,
                                  caching=caching, check_extractable=True),1):

        # page.rotate = (page.rotate + rotation) % 360

        outfp = StringIO()

        outfp.write('{} ===========================\n'.format(i))

        device = TextConverter(rsrcmgr, outfp, codec='utf-8', laparams=laparams,
                               imagewriter=imagewriter)

        interpreter = PDFPageInterpreter(rsrcmgr, device)

        interpreter.process_page(page)

        if logger:
            logger.info("Processing page: {}".format(i))

        device.close()

        r = outfp.getvalue()

        outfp.close()

        pages.append(re.sub(r'[ ]+', ' ', r) ) # Get rid of all of those damn spaces.

    fp.close()

    return  pages

def getTerminalSize():
    import os
    env = os.environ
    def ioctl_GWINSZ(fd):
        try:
            import fcntl, termios, struct, os
            cr = struct.unpack('hh', fcntl.ioctl(fd, termios.TIOCGWINSZ,
        '1234'))
        except:
            return
        return cr
    cr = ioctl_GWINSZ(0) or ioctl_GWINSZ(1) or ioctl_GWINSZ(2)
    if not cr:
        try:
            fd = os.open(os.ctermid(), os.O_RDONLY)
            cr = ioctl_GWINSZ(fd)
            os.close(fd)
        except:
            pass
    if not cr:
        cr = (env.get('LINES', 25), env.get('COLUMNS', 80))

        ### Use get(key[, default]) instead of a try/catch
        #try:
        #    cr = (env['LINES'], env['COLUMNS'])
        #except:
        #    cr = (25, 80)
    return int(cr[1]), int(cr[0])



class ansicolors:
    """Terminal Colors

        >>> print ansicolors.WARNING + "Warning!" + ansicolors.ENDC

    """
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[1;91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'