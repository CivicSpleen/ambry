# -*- coding: utf-8 -*-

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import json
import os
import smtplib
from email.mime.base import MIMEBase

EMAIL_SETTINGS_FILE = '.email_settings.json'


def send_email(recipients, subject, message, attachments=None):
    """ Sends email.
    Args:
        recipients (list of str):
        subject (str):
        message (str):
        attachments (list of str): list containing full paths (txt files only) to attach to email.
    """
    if not attachments:
        attachments = []

    if os.path.exists(EMAIL_SETTINGS_FILE):
        email_settings = json.load(open(EMAIL_SETTINGS_FILE))
        sender = email_settings.get('sender', 'ambry@localhost')
        use_tls = email_settings.get('use_tls')
        username = email_settings['username']
        password = email_settings['password']
        server = email_settings['server']
    else:
        # use local smtp
        server = 'localhost'
        username = None
        password = None
        sender = 'ambry@localhost'

    # Create the container (outer) email message.
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ','.join(recipients)
    msg.attach(MIMEText(message, 'plain'))

    # Add attachments.
    for file_name in attachments:
        if os.path.exists(file_name):
            with open(file_name, 'r') as fp:
                attachment = MIMEBase('application', 'text')
                attachment.set_payload(fp.read())
                attachment.add_header(
                    'Content-Disposition',
                    'attachment; filename="{}"'.format(os.path.basename(file_name)))
                msg.attach(attachment)

    # The actual mail send.
    srv = smtplib.SMTP(server)
    if use_tls:
        srv.starttls()
    if username:
        srv.login(username, password)

    srv.sendmail(sender, ','.join(recipients), msg.as_string())
    srv.quit()
