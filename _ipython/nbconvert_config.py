# ipython nbconvert configuration file. 
# Call with ipython nbconvert --config mycfg.py

c = get_config()
print '!!!',type(c.NbConvertApp)

import pprint

pprint.pprint(c.NbConvertApp.__dict__)
