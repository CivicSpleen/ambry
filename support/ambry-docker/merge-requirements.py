#
# Merge the requirements from the requirements.txt file into the Docker file as pip install commands.
# This reduces the changes required to the docker images on building.

import requirements
from os.path import dirname, join
import re

out = []
with open(join(dirname(__file__),'..','..','requirements.txt')) as f:
    for req in requirements.parse(f):
        out.append("RUN pip install '"+req.line.split('#',1)[0]+"'")

pips = '\n'.join(out)

pat = re.compile(r"# Start pip installs\n.*# End pip installs", re.MULTILINE)

with open('Dockerfile') as f_in:
    df = f_in.read()

print pat.sub("# Start pip installs\n{}\n# End pip installs".format(pips), df)

with open('Dockerfile','w') as f_out:
    f_out.write(pat.sub("# Start pip installs\n{}\n# End pip installs".format(pips), df))