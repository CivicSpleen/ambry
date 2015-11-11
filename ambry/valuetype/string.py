"""Math functions available for use in derivedfrom columns


Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

def uuid4():
    """Returns a UUID4 as a string"""

    from uuid import uuid4

    return str(uuid4())


def upper(v):

    return v.upper() if v else None

def lower(v):
    return v.lower() if v else None

def title(v):
    return v.title() if v else None
