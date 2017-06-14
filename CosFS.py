#!/usr/bin/env python
#coding=utf-8

#author: felix021@gmail.com

import os
import errno
import sys
import traceback
import time
import requests

import Queue
import thread
import threading

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

CODE_SAME_FILE          = -4018
CODE_EXISTED            = -177
CODE_ERR_OFF_GOBACK     = -4024

#并发线程数量 Concurrency Thread Number
NR_THREAD           = 10

#失败重试次数
RETRY_COUNT         = 6


CONFLICT_ERROR      = 1
CONFLICT_SKIP       = 2
CONFLICT_OVERWRITE  = 3

def localMkdir(path):
    try:
        os.mkdir(path)
    except OSError, e:
        if e.errno == errno.EEXIST and os.path.isdir(path): #file exists
            return
        raise e

def to_unicode(x):
    if type(x) is str:
        return x.decode('utf-8')
    return x

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

class CosThread(threading.Thread):
    def __init__(self, tid, queue):
        threading.Thread.__init__(self)
        self.tid    = tid
        self.t_queue= queue
        self.fail_list = []

    def run(self):
        print >>sys.stderr, 'Thread %d starts' % (self.tid)
        while not self.t_queue.empty():
            func, arg = self.t_queue.get(block=True, timeout=1)
            try:
                retry(func, *arg)
            except Exception, exc:
                self.fail_list.append([func, arg, exc])
        print >>sys.stderr, 'Thread %d ends' % (self.tid)

    @classmethod
    def start_new(cls, tid, queue):
        t = cls(tid, queue)
        t.start()
        return t

    @classmethod
    def execute(cls, queue, nr_thread=NR_THREAD):
        threads = []
        for i in range(nr_thread):
            threads.append(CosThread.start_new(i + 1, queue))

        fail_list = []
        for t in threads:
            fail_list += t.fail_list
            t.join()

        if fail_list:
            print >>sys.stderr, "=== FAILED LIST ==="
            for func, arg, exc in fail_list:
                print >>sys.stderr, " %s => %s" % (str(arg), str(exc))
            raise Exception("%d entries failed" % len(fail_list))


class CosFSException(Exception):
    pass

class CosFS(object):
    def __init__(self, appid, secret_id, secret_key, bucket, region = None):
        self.appid = appid
        self.secret_id = secret_id
        self.secret_key = secret_key
        self.bucket = bucket
        if region:
            self.cos_client = CosClient(appid, secret_id, secret_key, region=region)
        else:
            self.cos_client = CosClient(appid, secret_id, secret_key)

    def list_dir(self, path=u'/'):
        path = to_unicode(path)

        prefix = u''
        if path.endswith(u'*'):
            prefix = os.path.basename(path)[:-1]
            path = os.path.dirname(path)

        if not path.endswith(u'/'):
            path += u'/'

        context=u''
        arr_data = []
        while True:
            request = ListFolderRequest(self.bucket, path, prefix=prefix, context=context)
            result = self.cos_client.list_folder(request)
            if result['code'] != 0:
                raise CosFSException(result['code'], result['message'] + ': ' + path)

            data = result['data']
            arr_data.append(data)
            has_more = False
            if 'has_more' in data:
                has_more = data['has_more']
            if 'listover' in data: #cos v4
                has_more = not data['listover']

            if has_more:
                context = data['context']
            else:
                break

        dataset = {'dircount': 0, 'filecount': 0, 'infos': []}
        for data in arr_data:
            if 'filecount' not in data:
                data['filecount'] = 0
                data['dircount'] = 0
                for entry in data['infos']:
                    if self.isFile(entry):
                        data['filecount'] += 1
                    else:
                        entry['name'] = entry['name'].rstrip(u'/')
                        data['dircount'] += 1
            dataset['dircount'] += data['dircount']
            dataset['filecount'] += data['filecount']
            dataset['infos'] += data['infos']
        return dataset

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
            if overwrite and result['code'] == CODE_ERR_OFF_GOBACK:
                print >>sys.stderr, "fix tencent bug: remove remote file when ErrOffGoBack occurs"
                """
                上传出现这个4024的错误之后，覆盖（insertonly=0）参数也不能成功，只能删除文件后重新上传
                建议您将分片大小改为1M 分片上传之间sleep 100ms 出现错误的概率会小很多
                抱歉，这个问题暂时无法彻底解决，给您带来了不便。
                """
                self.rm(remote)
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
        begin_at = time.time()
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
        usage = time.time() - begin_at
        #print '[Time Usage: %.6f]' % usage

    #将[cos上remote目录里]的内容下载到[本地local目录里]，如果local目录不存在，会被创建
    def downloadDir(self, remote, local, conflict):
        remote = remote.rstrip(u'/')
        local = local.rstrip(u'/')

        file_queue = Queue.Queue()
        def walk(path = u'/', level = 0):
            print '[mkdir] ' + ' ' * level + local + path
            localMkdir(local + path)

            content = self.list_dir(remote + path)

            for entry in content['infos']:
                name = path + entry['name'].encode('utf-8')
                if self.isFile(entry):
                    print '[copy]  ' + ' ' * level + local + name
                    overwrite = conflict == CONFLICT_OVERWRITE
                    file_queue.put([self.download, (remote + name, local + name, overwrite)])
                else:
                    walk(name + '/', level + 1)

        walk()
        CosThread.execute(file_queue)
        print >>sys.stderr, "[download finished]"

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
                print >>sys.stderr, '[uploadFile] %s => %s' % (localfile, remotefile)
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

        file_queue = Queue.Queue()

        def process_dir(arg, dirname, filelist):
            dir_suffix = dirname[len(local):]
            remote_dir = remote + dir_suffix
            file_queue.put([self.mkdir, (remote_dir,)])
            print >>sys.stderr, '[doUpload] queue for mkdir %s' % remote_dir
            for filename in filelist:
                localfile = dirname.rstrip(u'/') + u'/' + filename
                if os.path.isdir(localfile):
                    continue
                if os.path.islink(localfile):
                    print >>sys.stderr, '[doUpload] skip symlink %s' % (localfile)
                    continue
                remotefile = remote_dir.rstrip(u'/') + u'/' + filename
                file_queue.put([uploadFile, (localfile, remotefile)])
                print >>sys.stderr, '[doUpload] queue for upload file %s ...' % remotefile

        os.path.walk(local, process_dir, None)

        CosThread.execute(file_queue)

        print >>sys.stderr, "[upload finished]"

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

    def delFolder(self, path):
        print >>sys.stderr, '[delFolder] %s' % (path)
        request = DelFolderRequest(self.bucket, to_unicode(path))
        result = self.cos_client.del_folder(request)
        if result['code'] not in [0, -197]:
            raise CosFSException(result['code'], result['message'])

    def rmdir(self, path, recursive=False):
        path = to_unicode(path)
        if not path.endswith(u'/'):
            path += u'/'

        if recursive:
            file_queue = Queue.Queue()
            dir_list = []
            def walk_dir(dirname):
                print >>sys.stderr, '[walk_dir] dir %s' % (dirname)
                dir_list.append(dirname)

                content = self.list_dir(dirname)
                for entry in content['infos']:
                    name = dirname.rstrip('/') + u'/' + entry['name']
                    if self.isFile(entry):
                        print >>sys.stderr, '[walk_dir] file %s' % (name)
                        file_queue.put([self.rm, (name,)])
                    else:
                        walk_dir(name + '/')

            walk_dir(path)
            CosThread.execute(file_queue)

            while dir_list:
                self.delFolder(dir_list.pop())

        else:
            self.delFolder(path)

        print >>sys.stderr, "[rmdir finished]"

    def isFile(self, entry):
        return 'sha' in entry


if __name__ == '__main__':
    #pass
    from cosfs_conf import *
    fs = CosFS(bucket_id, bucket_key, bucket_secret, bucket_name, region)
    #retry(fs.list_dir, '/store3/backup/db/10.237.228.61/')
    #result = fs.list_dir('/store3/backup/db/10.237.228.61/')
    #del result['infos']

    result = fs.list_dir('/store3/backup/db/10.6*')
    print result
