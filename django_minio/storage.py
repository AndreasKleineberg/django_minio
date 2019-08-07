import mimetypes
import os
from pathlib import Path

from django.conf import settings
from django.core.files.storage import Storage
from django.utils.deconstruct import deconstructible
from minio import Minio
from minio.error import InvalidXMLError, InvalidEndpointError, NoSuchKey, NoSuchBucket
from urllib3.exceptions import MaxRetryError


def setting(name, default=None):
    """
    Helper function to get a Django setting by name or (optionally) return
    a default (or else ``None``).
    """
    return getattr(settings, name, default)


@deconstructible
class MinioStorage(Storage):
    # TODO: Log errors caused by exceptions
    server = setting('MINIO_SERVER')
    access_key = setting('MINIO_ACCESSKEY')
    secret_key = setting('MINIO_SECRET')
    bucket = setting('MINIO_BUCKET')
    secure = setting('MINIO_SECURE')

    def __init__(self, *args, **kwargs):
        super(MinioStorage, self).__init__(*args, **kwargs)
        self._connection = None

    @property
    def connection(self):
        if not self._connection:
            try:
                self._connection = Minio(self.server,
                                         access_key=self.access_key,
                                         secret_key=self.secret_key,
                                         secure=self.secure)
            except InvalidEndpointError:
                self._connection = None
        return self._connection

    def _open(self, object_name, mode):
        return self.connection.get_object(self.bucket, object_name)

    def _save(self, name, content):
        pathname, ext = os.path.splitext(name)
        dir_path, _ = os.path.split(pathname)
        hashed_name = f'{dir_path}/{hash(content)}{ext}'

        content_type = content.content_type if hasattr(content, 'content_type') else mimetypes.guess_type(name)[0]

        if self.connection:
            if not self.connection.bucket_exists(self.bucket):
                self.connection.make_bucket(self.bucket)
            try:
                self.connection.put_object(
                    self.bucket,
                    hashed_name,
                    content,
                    content.size,
                    content_type=content_type,
                )
            except InvalidXMLError:
                pass
            except MaxRetryError:
                pass
        return hashed_name

    def delete(self, name):
        object_name = Path(name).as_posix()
        self.connection.remove_object(bucket_name=self.bucket,
                                      object_name=object_name)

    def url(self, name):
        return f'{self.server}/{self.bucket}/{name}'

    def exists(self, name):
        try:
            self.connection.stat_object(self.bucket, name)
        except (NoSuchKey, NoSuchBucket):
            return False
        except Exception as err:
            raise IOError(f'Could not stat file {name} {err}')
        else:
            return True

    def size(self, name):
        return self.connection.stat_object(self.bucket, name).size
