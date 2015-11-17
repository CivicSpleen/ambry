"""Sqalchemy table for storing account credentials.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from sqlalchemy import Column as SAColumn, Integer, UniqueConstraint
from sqlalchemy import Text, String, ForeignKey, LargeBinary
from sqlalchemy import event
from simplecrypt import encrypt, decrypt, DecryptionException
import simplecrypt

# How many times the
simplecrypt.EXPANSION_COUNT = (100,100,100)


from . import Base, MutationDict, JSONEncodedObj

class Account(Base):

    __tablename__ = 'accounts'

    id = SAColumn('ac_id', Integer, primary_key=True)

    d_vid = SAColumn('ac_d_vid', String(20), ForeignKey('datasets.d_vid'),  index=True)

    # Foreign account identifier, often a bucket name or domain name.
    # The key used to reference the account
    account_id = SAColumn('ac_account_id', Text, unique = True)
    access_key = SAColumn('ac_access', Text)  # Access token or username

    user_id = SAColumn('ac_user_id', Text, index=True) # Ambry User
    organization_id = SAColumn('ac_org_id', Text, index=True) # Ambry Organization
    major_type = SAColumn('ac_major_type', Text)  # Major type, often name of service or account providing company
    minor_type = SAColumn('ac_minor_type', Text)  # Minor type, subtype of the major type

    _secret = SAColumn('ac_secret', Text) # Secret or password
    name = SAColumn('ac_name', Text)  # Person's name
    email = SAColumn('ac_email', Text)  # Email for foreign account
    org = SAColumn('ac_url', Text)  # Organization name
    url = SAColumn('ac_org', Text)  # General URL, maybe the login URL
    comment = SAColumn('ac_comment', Text)  # Access token or username
    data = SAColumn('ac_data', MutationDict.as_mutable(JSONEncodedObj))

    __table_args__ = (
        UniqueConstraint('ac_account_id', 'ac_access', name='_uc_account_1'),
    )

    password = None # Must be set to encrypt or decrypt secret


    @property
    def secret(self):

        if not self._secret:
            return None

        if self.password:
            try:
                return decrypt(self.password, self._secret.decode('base64'))
            except DecryptionException as e:
                raise DecryptionException("Bad password")
        else:
            raise Exception("Must have a password to get or set the secret")

    @secret.setter
    def secret(self,v):

        if self.password:
            self._secret = encrypt(self.password, v).encode('base64')
        else:
            raise Exception("Must have a password to get or set the secret")

    @staticmethod
    def before_insert(mapper, conn, target):
        Account.before_update(mapper, conn, target)

    @staticmethod
    def before_update(mapper, conn, target):
        pass

    @property
    def dict(self):
        """A dict that holds key/values for all of the properties in the
        object.

        :return:

        """
        d = {p.key: getattr(self, p.key) for p in self.__mapper__.attrs if p.key not in ('data', '_secret') }

        d['secret'] = self.secret

        for k, v in self.data.items():
            d[k] = v

        return { k:v for k,v in d.items() if v }


event.listen(Account, 'before_insert', Account.before_insert)
event.listen(Account, 'before_update', Account.before_update)