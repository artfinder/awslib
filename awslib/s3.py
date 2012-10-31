import os
import os.path
import time
import threading
import multiprocessing
import multiprocessing.pool

from boto.s3.connection import S3Connection
from boto.s3.key import Key

### S3 UPLOAD


def upload_directory(dirname, bucketname):
    upload_directory_to_bucket_by_name(dirname, "", bucketname)

def parallel_upload_directory(dirname, bucketname):
    parallel_upload_directory_to_bucket(dirname, "", bucketname)

def upload_directory_to_bucket_by_name(base, dirname, bucketname, bucketprefix=''):
    "Upload an entire directory to S3."
    
    aws_access_key = os.environ['AWS_ACCESS_KEY']
    aws_secret_key = os.environ['AWS_SECRET_KEY']
    
    s3     = S3Connection(aws_access_key, aws_secret_key)
    bucket = s3.get_bucket(bucketname)
    upload_directory_to_bucket(base, dirname, bucket, bucketprefix)


def upload_directory_to_bucket(base, dirname, bucket, bucketprefix=''):
    for f in os.listdir(os.path.join(base, dirname)):
        fname = os.path.join(dirname, f)
        if os.path.isdir(os.path.join(base, fname)):
            upload_directory_to_bucket(base, fname, bucket, bucketprefix)
        else:
            upload_file_to_bucket(base, fname, bucket, bucketprefix=bucketprefix)

def pool_upload(args):
    bucket_conns = threading.current_thread().local.bucket_conns
    bucket = bucket_conns[os.getpid()][args[2]]
    upload_file_to_bucket(args[0], args[1], bucket, bucketprefix=args[3])

def pool_init(bucketname):
    aws_access_key = os.environ['AWS_ACCESS_KEY']
    aws_secret_key = os.environ['AWS_SECRET_KEY']
    
    s3     = S3Connection(aws_access_key, aws_secret_key)
    bucket = s3.get_bucket(bucketname)
   
    try:    
        tlocal = threading.current_thread().local
    except AttributeError:
        tlocal = threading.current_thread().local = threading.local()

    try:
        bucket_conns = tlocal.bucket_conns
    except AttributeError:
        bucket_conns = tlocal.bucket_conns = {}

    bucket_conns.setdefault(os.getpid(), {})[bucketname] = bucket

def parallel_upload_directory_to_bucket(base, dirname, bucket, bucketprefix=''):
    pool = multiprocessing.pool.ThreadPool(
            processes=32,
            initializer=pool_init,
            initargs=(bucket,))
  
    def walk_files(path):
        for root, dirs, files in os.walk(path):
            for f in files:
                yield os.path.join(root[len(path):], f)

    pool.map(pool_upload,
            ((base, f, bucket, bucketprefix) for f in walk_files(os.path.join(base, dirname))),
            10)

def upload_file_to_bucket_by_name(base, fname, bucketname, keyname=None, bucketprefix='', public=True, aws_access_key=None, aws_secret_key=None):
    "Upload a single file to S3."
    
    if aws_access_key is None:
        aws_access_key = os.environ['AWS_ACCESS_KEY']
    if aws_secret_key is None:
        aws_secret_key = os.environ['AWS_SECRET_KEY']
    
    s3     = S3Connection(aws_access_key, aws_secret_key)
    bucket = s3.get_bucket(bucketname)
    upload_file_to_bucket(
        base, fname, bucket, keyname=keyname, bucketprefix=bucketprefix,
        public=public)


def upload_file_to_bucket(base, fname, bucket, keyname=None, bucketprefix='', public=True):
    "Upload a single file to S3."
    
    TTL = 31536000      # one year
    
    print "Uploading %40.40s" % fname,
    while True:
        try:
            k = Key(bucket)
            if keyname is None:
                k.key = os.path.join(bucketprefix, fname)
            else:
                k.key = keyname
            k.set_contents_from_filename(
                os.path.join(base, fname),
                replace=False,
                headers={
                    'Cache-Control': 'max-age=%s' % TTL
                },
            )
            if public:
                k.set_acl('public-read')
            print "done."
            break
        except Exception, e:
            import traceback
            print "FAILED, retrying in 10."
            traceback.print_exc()
            time.sleep(10)
