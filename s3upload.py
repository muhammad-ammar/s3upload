"""AWS S3 Parallel Upload Files"""

import os
import sys
import time
from queue import Queue
from threading import Thread
import threading
import mimetypes

############ boto 3
import boto3
import boto3.session
import botocore


def get_file_type(file_path):

    kwargs = {}
    if os.path.splitext(file_path)[1]:
        content_type, content_encoding = mimetypes.guess_type(file_path)

        if content_type:
            kwargs['ContentType'] = content_type

        if content_encoding:
            kwargs['ContentEncoding'] = content_encoding

    return kwargs


class UploadWorker(Thread):

    def __init__(self, queue, bucket, source, destination):
        self._queue = queue
        self._bucket = bucket
        self._source = source
        self._destination = destination
        session = boto3.session.Session()
        self.s3 = session.resource('s3')
        super(UploadWorker, self).__init__()

    def run(self):
        while True:
            file = self._queue.get()
            print('Uploading {}'.format(file))

            f_path = file[len(self._source):]
            f_path = f_path[1:] if f_path.startswith('/') else f_path

            if self._destination:
               key = self._destination + f_path
            else:
               key = f_path

            kwargs = get_file_type(file)

            self.s3.meta.client.upload_file(
                file,
                self._bucket,
                key,
                ExtraArgs=kwargs if kwargs else None
            )
            print('Upload completed for {}'.format(file))
            self._queue.task_done()


class S3(object):

    @staticmethod
    def s3_resource():
        """
        Returns s3 resource object.
        """
        return boto3.resource('s3')

    @classmethod
    def upload_data_to_bucket(cls, source, bucket_name, destination=None, threads=20):
        """
        Upload data from local system to S3 Bucket.

        Arguments:
            source (str): local system path for a directory or a file
            bucket_name (str): destination bucket address/path
            destination (str): path in S3 bucket where we upload data. NOTE: By default upload at root of bucket
            threads (int): number of threads to use for copying

        """
        if not os.path.exists(source):
            raise ValueError('{} dosen\'t exist'.format(source))

        if destination is not None and len(destination.strip()) == 0:
            raise ValueError('Incorrect value for destination')

        start_time = time.time()

        destination = cls.make_path(destination)

        __ = cls.bucket(bucket_name)
        upload_queue = Queue(maxsize=1000)

        for __ in range(threads):
            worker = UploadWorker(upload_queue, bucket_name, source, destination)
            worker.daemon = True
            worker.start()

        for file in cls.files(source):
            upload_queue.put(file)

        upload_queue.join()
        print('Uploaded all Data in {}'.format(hms_string(time.time() - start_time)))

    @classmethod
    def bucket(cls, bucket_name):
        """
        Return a bucket if exists.

        Arguments:
            bucket_name (str): name of s3 bucket

        Raises:
            ValueError: If bucket doesn't exist

        """
        s3 = cls.s3_resource()
        bucket = s3.Bucket(bucket_name)
        try:
            s3.meta.client.head_bucket(Bucket=bucket.name)
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                raise ValueError('{} bucket doesn\'t exist'.format(bucket_name))

        return bucket

    @classmethod
    def files(cls, path):
        """
        Return file(s).

        Arguments:
            path (str): path to a directory or a single file

        """
        if os.path.isdir(path):
            fs = []
            for dir_path, _, files in os.walk(path):
                fs.extend([os.path.join(dir_path, file) for file in files])
            return fs
        else:
            return [path]

    @classmethod
    def make_path(cls, path):
        """
        Insert trailing slash and remove leading slash

        Arguments:
            path (str): destination path prefix
        """
        if path is None or len(path.strip()) == 0:
            return ''

        if not path.endswith('/'):
            path = path + '/'

        if path.startswith('/'):
            path = path[1:]

        return path.strip()


def hms_string(seconds_elapsed):
    """
    Convert elapsed time in seconds to human readable form.

    Arguments:
        seconds_elapsed (float):

    """
    h = int(seconds_elapsed / (60 * 60))
    m = int((seconds_elapsed % (60 * 60)) / 60)
    s = seconds_elapsed % 60.
    return "{}h:{}m:{}s".format(h, m, int(s))
