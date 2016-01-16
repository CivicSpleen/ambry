# -*- coding: utf-8 -*-
import smtplib
import unittest

try:
    # py2, mock is external lib.
    from mock import patch
except ImportError:
    # py3, mock is included
    from unittest.mock import patch

from ambry.util.mail import send_email


class SendEmailTest(unittest.TestCase):
    """ Tests send_email function. """

    @patch('ambry.util.mail.json.load')
    @patch('ambry.util.mail.os.path.exists')
    @patch('ambry.util.mail.smtplib.SMTP', spec=smtplib.SMTP)
    def test_uses_credentials_from_file_to_login(self, FakeSMTP, fake_exists, fake_load):
        fake_exists.return_value = True
        fake_load.return_value = {
            'server': 'example.com',
            'username': 'user1',
            'password': 'password1',
            'use_tls': True
        }

        send_email(['test1@example.com'], 'Hey', 'Hey')
        FakeSMTP().login.assert_called_once_with('user1', 'password1')
