""" User Forms

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt
"""


from flask_wtf import Form
from wtforms.fields import StringField, PasswordField
from wtforms.validators import Required, DataRequired, Email
from wtforms import BooleanField, TextField, PasswordField, validators


class LoginForm(Form):
    username = StringField('Username', [validators.Length(min=4, max=25)])
    password = PasswordField('New Password', [validators.DataRequired()])
