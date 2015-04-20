"""Text handling and conversion utilities

Copyright (c) 2014 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""


def generate_pdf_pages(fp):

    from pdfminer.pdfdocument import PDFDocument
    from pdfminer.pdfparser import PDFParser
    from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
    from pdfminer.pdfdevice import PDFDevice
    from pdfminer.pdfpage import PDFPage
    from pdfminer.converter import TextConverter
    from pdfminer.cmapdb import CMapDB
    from pdfminer.layout import LAParams

    import re

    debug = 0
    # input option
    password = ''
    pagenos = set()
    maxpages = 0
    imagewriter = None
    rotation = 0
    caching = True

    laparams = LAParams()

    #
    PDFDocument.debug = debug
    PDFParser.debug = debug
    CMapDB.debug = debug
    PDFResourceManager.debug = debug
    PDFPageInterpreter.debug = debug
    PDFDevice.debug = debug
    #
    rsrcmgr = PDFResourceManager(caching=caching)

    from cStringIO import StringIO

    outfp = StringIO()

    device = TextConverter(rsrcmgr, outfp, codec='utf-8', laparams=laparams,
                           imagewriter=imagewriter)

    interpreter = PDFPageInterpreter(rsrcmgr, device)
    for page in PDFPage.get_pages(fp, pagenos,
                                  maxpages=maxpages, password=password,
                                  caching=caching, check_extractable=True):
        page.rotate = (page.rotate + rotation) % 360
        interpreter.process_page(page)

    fp.close()

    device.close()

    r = outfp.getvalue()

    outfp.close()

    return re.sub(r'[ ]+', ' ', r)  # Get rid of all of those damn spaces.
