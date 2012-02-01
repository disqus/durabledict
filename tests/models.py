from django.conf import settings

if not settings.configured:
    settings.configure(
        DATABASE_ENGINE='sqlite3',
        DATABASE_NAME='test.db',
        INSTALLED_APPS=[
            'tests',
        ],

        DEBUG=True,
    )

from django.db import models


class Setting(models.Model):
    key = models.CharField(max_length=32, unique=True)
    value = models.CharField(max_length=32, default='')
