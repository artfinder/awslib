import os
import os.path
import time
from boto.s3.connection import S3Connection
from boto.s3.key import Key

### S3 UPLOAD


def upload_directory(dirname, bucketname):
    upload_directory_to_bucket_by_name(dirname, "", bucketname)


def upload_directory_to_bucket_by_name(base, dirname, bucketname, bucketprefix=''):
    "Upload an entire directory to S3."
    
    aws_access_key = os.environ['AWS_ACCESS_KEY']
    aws_secret_key = os.environ['AWS_SECRET_KEY']
    
    s3     = S3Connection(aws_access_key, aws_secret_key)
    bucket = s3.create_bucket(bucketname)
    upload_directory_to_bucket(base, dirname, bucket, bucketprefix)


def upload_directory_to_bucket(base, dirname, bucket, bucketprefix):
    for f in os.listdir(os.path.join(base, dirname)):
        fname = os.path.join(dirname, f)
        if os.path.isdir(os.path.join(base, fname)):
            upload_directory_to_bucket(base, fname, bucket, bucketprefix)
        else:
            upload_file_to_bucket(base, fname, bucket, bucketprefix)


def upload_file_to_bucket_by_name(base, fname, bucketname, bucketprefix='', public=True):
    "Upload a single file to S3."
    
    aws_access_key = os.environ['AWS_ACCESS_KEY']
    aws_secret_key = os.environ['AWS_SECRET_KEY']
    
    s3     = S3Connection(aws_access_key, aws_secret_key)
    bucket = s3.create_bucket(bucketname)
    upload_file_to_bucket(base, fname, bucket, bucketprefix, public)


def upload_file_to_bucket(base, fname, bucket, bucketprefix='', public=True):
    "Upload a single file to S3."
    
    TTL = 31536000      # one year
    
    print "Uploading %40.40s" % fname,
    while True:
        try:
            k = Key(bucket)
            k.key = os.path.join(bucketprefix, fname)
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
