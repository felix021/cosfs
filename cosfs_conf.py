#coding=utf-8

bucket_id      = 1000000
bucket_key     = u'hello'
bucket_secret  = u'world'
bucket_name    = u'bucket'
region         = u'sh'

try:
    from cosfs_conf_local import *
except:
    pass
