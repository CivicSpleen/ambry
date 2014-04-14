from django.contrib import admin

# Register your models here.

from django.contrib import admin
from account.models import Poll

admin.site.register(Poll)
