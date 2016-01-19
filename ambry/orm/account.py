"""Sqalchemy table for storing account credentials.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from sqlalchemy import Column as SAColumn, Integer, UniqueConstraint
from sqlalchemy import Text, String, ForeignKey
from sqlalchemy import event

from simplecrypt import encrypt, decrypt, DecryptionException as SC_DecryptionException
import simplecrypt

from ambry.orm.exc import OrmObjectError

from . import Base, MutationDict, JSONEncodedObj


# How many times the encryption should run. The default is 100,000 rounds, but that's horrifically
# slow, and seems to be required as a defense against bad passwords.
simplecrypt.EXPANSION_COUNT = (1000, 1000, 1000)


class AccountDecryptionError(OrmObjectError):
    pass


class MissingPasswordError(OrmObjectError):
    pass


class Account(Base):

    __tablename__ = 'accounts'

    id = SAColumn('ac_id', Integer, primary_key=True)
    d_vid = SAColumn('ac_d_vid', String(20), ForeignKey('datasets.d_vid'), index=True)
    user_id = SAColumn('ac_user_id', Text, index=True, doc='Ambry User')
    organization_id = SAColumn('ac_org_id', Text, index=True,
                               doc='Ambry Organization')

    major_type = SAColumn('ac_major_type', Text,
                          doc='Major type, often name of service or account providing company')
    minor_type = SAColumn('ac_minor_type', Text,
                          doc='Minor type, subtype of the major type')

    # Foreign account identifier, often a bucket name or domain name.
    # The key used to reference the account
    account_id = SAColumn('ac_account_id', Text, unique=True)

    url = SAColumn('ac_org', Text, doc='URL of service')

    access_key = SAColumn('ac_access', Text, doc='Access token or username')

    encrypted_secret = SAColumn('ac_secret', Text, doc='Symmetrically encrypted secret')

    encrypted_password = SAColumn('ac_password', Text, doc='Asymmetrically encrypted user password')

    name = SAColumn('ac_name', Text, doc='Person\'s name')
    email = SAColumn('ac_email', Text, doc='Email for foreign account')
    org = SAColumn('ac_url', Text, doc='Organization name')
    comment = SAColumn('ac_comment', Text)  # Access token or username
    data = SAColumn('ac_data', MutationDict.as_mutable(JSONEncodedObj))

    __table_args__ = (
        UniqueConstraint('ac_account_id', 'ac_access', name='_uc_account_1'),
    )

    secret_password = None  # Must be set to encrypt or decrypt secret

    def incver(self):
        """Increment all of the version numbers and return a new object"""
        from . import incver
        return incver(self, ['d_vid'])

    @staticmethod
    def sym_encrypt(password, v):
        return encrypt(password, v).encode('base64')

    @staticmethod
    def sym_decrypt(password, v):
        import binascii

        try:
            return decrypt(password, v.decode('base64'))
        except SC_DecryptionException as e:
            raise AccountDecryptionError('Wrong password ')
        except binascii.Error as e:
            raise AccountDecryptionError('Bad password: {}'.format(e))

    @property
    def secret(self):
        assert self.secret_password  # The encryption password
        if self.encrypted_secret:
            return self.sym_decrypt(self.secret_password, self.encrypted_secret)
        else:
            return None

    @secret.setter
    def secret(self, v):
        assert self.secret_password  # The encryption password
        self.encrypted_secret = self.sym_encrypt(self.secret_password, v)

    def decrypt_secret(self, password=None):

        if not password:
            password = self.secret_password

        if not self.encrypted_secret:
            return None

        if self.major_type == 'user':
            return None  # These can't be decrypted, only tested.

        if password:
            return self.sym_decrypt(password, self.encrypted_secret)
        else:
            raise MissingPasswordError('Must have a password to get or set the secret')

    def encrypt_secret(self, v, password=None):

        if not password:
            password = self.secret_password

        if password:
            self.encrypted_secret = self.sym_encrypt(password, v)
        else:
            raise MissingPasswordError('Must have a password to get or set the secret')

        return self.encrypted_secret

    @property
    def password(self):
        raise NotImplemented('Use test()')

    @password.setter
    def password(self, v):
        assert self.secret_password
        self.encrypted_password = self.sym_encrypt(self.secret_password, v)

    def encrypt_password(self, v):

        from passlib.hash import pbkdf2_sha512
        self.encrypted_password = pbkdf2_sha512.encrypt(v, rounds=50000, salt_size=16)

    def test(self, v):
        """Test the password against a value"""
        from passlib.hash import pbkdf2_sha512
        return pbkdf2_sha512.verify(v, self.encrypted_password)

    @staticmethod
    def before_insert(mapper, conn, target):
        Account.before_update(mapper, conn, target)

    @staticmethod
    def before_update(mapper, conn, target):
        pass

    @classmethod
    def prop_map(cls):

        prop_map = {
            'service': 'major_type',
            'host': 'url',
            'organization': 'org',
            'apikey': 'secret',

            'access': 'access_key',
            'access_key': 'access_key',
            'secret': 'secret',
            'name': 'name',
            'org': 'org',
            'url': 'url',
            'email': 'email',
        }

        for p in cls.__mapper__.attrs:
            prop_map[p.key] = p.key

        return prop_map

    @property
    def dict(self):
        """A dict that holds key/values for all of the properties in the
        object.

        :return:

        """

        d = {p.key: getattr(self, p.key) for p in self.__mapper__.attrs if p.key not in ('data')}

        d['secret'] = 'not available'

        if self.secret_password:
            try:
                d['secret'] = self.decrypt_secret()
            except AccountDecryptionError:
                pass

        if self.data:
            for k, v in self.data.items():
                d[k] = v

        return {k: v for k, v in d.items()}


event.listen(Account, 'before_insert', Account.before_insert)
event.listen(Account, 'before_update', Account.before_update)
