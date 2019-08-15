import mimetypes
import os
from pathlib import Path

from django.conf import settings
from django.core.files.storage import Storage
from django.utils.deconstruct import deconstructible
from minio import Minio
from minio.error import (InvalidXMLError, InvalidEndpointError, NoSuchKey,
                         NoSuchBucket)
from urllib3.exceptions import MaxRetryError


def setting(name, default=None):
    """
    Helper function to get a Django setting by name or (optionally) return
    a default (or else ``None``).
    """
    return getattr(settings, name, default)


@deconstructible
class MinioStorage(Storage):
    def __init__(self, *args, **kwargs):
        self.SERVER = setting('MINIO_SERVER')
        self.ACCESS_KEY = setting('MINIO_ACCESSKEY')
        self.SECRET_KEY = setting('MINIO_SECRET')
        self.BUCKET = setting('MINIO_BUCKET')
        self.SECURE = setting('MINIO_SECURE')
        self._connection = None

    @property
    def connection(self):
        if not self._connection:
            try:
                self._connection = Minio(self.SERVER,
                                         access_key=self.ACCESS_KEY,
                                         secret_key=self.SECRET_KEY,
                                         secure=self.SECURE)
            except InvalidEndpointError:
                self._connection = None
        return self._connection

    def _open(self, object_name, mode):
        return self.connection.get_object(self.BUCKET, object_name)

    def _save(self, name, content):
        content_type = (content.content_type
                        if hasattr(content, 'content_type')
                        else mimetypes.guess_type(name)[0])

        if self.connection:
            if not self.connection.bucket_exists(self.BUCKET):
                self.connection.make_bucket(self.BUCKET)
            self.connection.put_object(
                self.BUCKET,
                name,
                content,
                content.size,
                content_type=content_type,
            )
        return name

    def delete(self, name):
        object_name = Path(name).as_posix()
        self.connection.remove_object(bucket_name=self.BUCKET,
                                      object_name=object_name)

    def url(self, name):
        return f'{self.SERVER}/{self.BUCKET}/{name}'

    def exists(self, name):
        try:
            self.connection.stat_object(self.BUCKET, name)
        except (NoSuchKey, NoSuchBucket):
            return False
        except Exception as err:
            raise IOError(f'Could not stat file {name} {err}')
        else:
            return True

    def size(self, name):
        return self.connection.stat_object(self.BUCKET, name).size
