#!/usr/bin/env python
#coding=utf-8

import os
import sys
import traceback
import time
import requests

import qcloud_cos

from qcloud_cos import CosClient
from qcloud_cos import UploadFileRequest
from qcloud_cos import DelFileRequest
from qcloud_cos import MoveFileRequest
from qcloud_cos import DelFolderRequest
from qcloud_cos import CreateFolderRequest
from qcloud_cos import StatFileRequest
from qcloud_cos import ListFolderRequest

SIGN_EXPIRE = 86400 #seconds
SLEEP_INTERVAL = 1.0 #seconds

CODE_SAME_FILE  = -4018
CODE_EXISTED    = -177

CONFLICT_ERROR      = 1
CONFLICT_SKIP       = 2
CONFLICT_OVERWRITE  = 3

def to_unicode(x):
    if type(x) is str:
        return x.decode('utf-8')
    return x

RETRY_COUNT = 3

def retry(func, *args, **kwargs):
    for i in range(RETRY_COUNT):
        try:
            return func(*args, **kwargs)
        except Exception, e:
            print >>sys.stderr, '[retry] %s(%s, %s) failed @ round %d' % (func, str(args), str(kwargs), i)
            print >>sys.stderr, traceback.format_exc()
            if i == RETRY_COUNT - 1:
                raise
            time.sleep(SLEEP_INTERVAL)

def download_file(url, filename, headers=None):
    r = requests.get(url, headers=headers, stream=True)
    with open(filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024): 
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
        f.flush()

class CosFSException(Exception):
    pass

class CosFS(object):
    def __init__(self, appid, secret_id, secret_key, bucket):
        self.appid = appid
        self.secret_id = secret_id
        self.secret_key = secret_key
        self.bucket = bucket
        self.cos_client = CosClient(appid, secret_id, secret_key)

    def list_dir(self, path=u'/'):
        if not path.endswith(u'/'):
            path += u'/'
        request = ListFolderRequest(self.bucket, to_unicode(path))
        result = self.cos_client.list_folder(request)
        if result['code'] != 0:
            raise CosFSException(result['code'], result['message'] + ': ' + path)

        data = result['data']
        if data['has_more']:
            args = (data['dircount'], data['filecount'], path.encode('utf-8'), len(data['infos']))
            print >>sys.stderr, "[warning] there are %d directories and %d files in %s, while COS only returned %d entries." % args
        return data

    def ls(self, path=u'/', detail=False, recursive=False):
        content = self.list_dir(path)
        if recursive:
            print '%s:' % (path.encode('utf-8'))

        for entry in content['infos']:
            isFile = self.isFile(entry)
            name = entry['name'].encode('utf-8')
            if detail:
                ctime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(entry['ctime'])))
                print './%s%s: [size:%d] [created_at:%s]' % (name, '' if isFile else '/', entry['filesize'] if isFile else 0, ctime)
            else:
                print name + ('' if isFile else '/')

        if recursive and content['dircount'] > 0:
            print ''
            for info in content['infos']:
                if 'sha' not in info:
                    self.ls(path.rstrip(u'/') + u'/' + info['name'], detail, recursive)
                    print ''

    def mv(self, src, dest, overwrite=False):
        src = to_unicode(src)
        dest = to_unicode(dest)
        if dest.endswith(u'/'):
            dest += os.path.basename(src)
        request = MoveFileRequest(self.bucket, src, dest)
        result = self.cos_client.move_file(request)
        if result['code'] != 0:
            raise CosFSException(result['code'], result['message'])
    
    def download(self, remote, local, overwrite=False):
        local = to_unicode(local)
        remote = to_unicode(remote)

        if local == u'.':
            local += u'/'

        if local.endswith(u'/'):
            local += os.path.basename(remote)

        if os.path.exists(local):
            if overwrite:
                os.unlink(local)
            else:
                raise CosFSException(-1, 'local file %s exists' % local)

        fileattr = self.stat(remote)
        url = fileattr['source_url'] + '?sign=' + fileattr['sign']
        download_file(url, local)

    def cat(self, path):
        fileattr = self.stat(to_unicode(path))
        url = fileattr['source_url'] + '?sign=' + fileattr['sign']
        download_file(url, '/dev/stdout')

    def upload(self, local, remote, overwrite=False, silent=False):
        local = to_unicode(local)
        remote = to_unicode(remote)
        if remote.endswith(u'/'):
            remote += os.path.basename(local)

        request = UploadFileRequest(self.bucket, remote, local)
        if overwrite:
            request.set_insert_only(0)
        result = self.cos_client.upload_file(request)
        if result['code'] != 0:
            if result['code'] == CODE_SAME_FILE:
                if not silent:
                    print >>sys.stderr, "skipped: same file on COS"
                return
            raise CosFSException(result['code'], result['message'])

    def cp(self, src, dest, overwrite=False):
        if src.startswith('cos:') and dest.startswith('cos:'): #move in cos
            raise CosFSException(-1, "not supported yet, use mv please")
        elif src.startswith('cos:'): #download
            self.download(src[4:], dest, overwrite)
        elif dest.startswith('cos:'): #upload
            self.upload(src, dest[4:], overwrite)
        else:
            raise CosFSException(-1, "at least one of src/dest should start with `cos:`")

    def cpdir(self, src, dest, conflict=CONFLICT_ERROR):
        src = to_unicode(src)
        dest = to_unicode(dest)
        if src.startswith(u'cos:') and dest.startswith(u'cos:'):
            raise CosFSException(-1, "not supported")
        elif src.startswith(u'cos:'): #download
            self.downloadDir(src[4:], dest, conflict)
        elif dest.startswith(u'cos:'): #upload
            self.uploadDir(src, dest[4:], conflict)
        else:
            raise CosFSException(-1, "at least one of src/dest should start with `cos:`")

    def downloadDir(self, remote, local, conflict):
        raise CosFSException(-1, "not supported yet")

    def uploadDir(self, local, remote, conflict):
        if not remote.endswith(u'/'):
            remote += u'/'

        if not local.endswith(u'/'):
            remote += os.path.basename(local) + u'/'

        local = os.path.abspath(local) + u'/'
        if not os.path.isdir(local):
            raise CosFSException(-1, "please specify a local directory")

        retry(self.mkdir, remote)

        def uploadFile(localfile, remotefile):
            try:
                self.upload(localfile, remotefile, silent=True)
                print >>sys.stderr, 'done: new'
            except Exception, e:
                if e[0] == CODE_EXISTED: #ERROR_CMD_COS_FILE_EXIST
                    if conflict == CONFLICT_SKIP:
                        print >>sys.stderr, 'skipped: existed'
                    elif conflict == CONFLICT_OVERWRITE:
                        self.upload(localfile, remotefile, overwrite=True)
                        print >>sys.stderr, 'done: overwrite'
                    else:
                        raise
                    return
                raise

        def doUpload(arg, dirname, filelist):
            dir_suffix = dirname[len(local):]
            remote_dir = remote + dir_suffix
            retry(self.mkdir, remote_dir)
            print >>sys.stderr, '[doUpload] mkdir %s' % remote_dir
            for filename in filelist:
                localfile = dirname.rstrip(u'/') + u'/' + filename
                if os.path.isdir(localfile):
                    continue
                if os.path.islink(localfile):
                    print >>sys.stderr, '[doUpload] skip symlink %s' % (localfile)
                    continue
                remotefile = remote_dir.rstrip(u'/') + u'/' + filename

                print >>sys.stderr, '[doUpload] upload file %s ...' % remotefile,
                retry(uploadFile, localfile, remotefile)

        os.path.walk(local, doUpload, None)


    def stat(self, path):
        request = StatFileRequest(self.bucket, to_unicode(path))
        result = self.cos_client.stat_file(request)
        if result['code'] != 0:
            raise CosFSException(result['code'], result['message'])

        ret = result['data']
        auth = qcloud_cos.cos_auth.Auth(self.cos_client.get_cred())
        ret['sign'] = auth.sign_download(self.bucket, path, int(time.time()) + SIGN_EXPIRE)
        return ret

    def rm(self, path):
        request = DelFileRequest(self.bucket, to_unicode(path))
        result = self.cos_client.del_file(request)
        if result['code'] != 0:
            raise CosFSException(result['code'], result['message'])

    def mkdir(self, path):
        path = to_unicode(path)
        if not path.endswith(u'/'):
            path += u'/'
        request = CreateFolderRequest(self.bucket, path)
        result = self.cos_client.create_folder(request)
        if result['code'] not in [0, -178]: #ok or already exists
            raise CosFSException(result['code'], result['message'])

    def rmdir(self, path, recursive=False):
        path = to_unicode(path)
        if not path.endswith(u'/'):
            path += u'/'

        if recursive:
            content = self.list_dir(path)
            for entry in content['infos']:
                name = path.rstrip(u'/') + '/' + entry['name']
                if self.isFile(entry):
                    self.rm(name)
                else:
                    self.rmdir(name, True)

        request = DelFolderRequest(self.bucket, to_unicode(path))
        result = self.cos_client.del_folder(request)
        if result['code'] != 0:
            raise CosFSException(result['code'], result['message'])

    def isFile(self, entry):
        return 'sha' in entry


if __name__ == '__main__':
    #pass
    from cosfs_conf import *
    fs = CosFS(bucket_id, bucket_key, bucket_secret, bucket_name)
    #retry(fs.list_dir, '/store3/backup/db/10.237.228.61/')
    fs.list_dir('/store3/backup/db/10.237.228.61/')
