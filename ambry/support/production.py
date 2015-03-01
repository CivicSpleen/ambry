"""Production settings

This file can be copied to devel.py and altered to configure settings for development.

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""



IN_PRODUCTION = __file__.endswith('production.py')
IN_DEVELOPMENT = not IN_PRODUCTION