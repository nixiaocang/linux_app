#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
不落盘方式传输
批量创建
断点续传
批量追加
"""

import os
import sys
import json
import time
import Queue
import math
import zipfile
import requests
import threading
import traceback
from config import Configuration

class FuploadHelper(object):
    def __init__(self, domain, username, password):
        self.domain = domain
        self.username = username
        self.password = password
        self.conf = Configuration()
        self.user_id = self.login()
        self.flag = False
        self.total = 0
        self.chunksize = int(self.conf.get('server', 'chunksize'))* 1024*1024

    def login(self):
        data = dict(
                domain=self.domain,
                username=self.username,
                password=self.password
                )
        login_url = Configuration().get('server', 'login_url')
        res = requests.post(login_url, data)
        result = json.loads(res.content)
        if int(result.get('status')) != 0:
            raise Exception("用户信息错误")
        user_url = Configuration().get('server', 'Ezio') + '/api/user'
        res = requests.post(user_url, data)
        result = json.loads(res.content)
        if int(result.get('status')) != 0:
            raise Exception("用户信息错误")
        else:
            return result['result']['user_id']

    def create(self, local_path):

        # 第一步 获取schema信息 创建数据源
        ds_name, tbs = self.get_dsinfo(local_path)

        # 第二步 通过Ezio服务创建工作表和数据源 并返回ds_id和tb_id
        bag = dict(
                user_id=self.user_id,
                ds_name=ds_name,
                tbs=json.dumps(tbs)
                )
        ezio_create = self.conf.get('server', 'Ezio') + '/api/batchcreate'
        res = requests.post(ezio_create, bag)
        print res.content
        result = json.loads(res.content)['result']
        ds_id = result['ds_id']
        db_id = result['db_id']
        tb_res = result['tbs']

        # 第三步 将路径下的数据全部分割 
        for tbinfo in tb_res:
            path = os.path.join(local_path, tbinfo['tbname'])
            print path
            self.do_action(ds_id, tbinfo['tb_id'], tbinfo['schema'], tbinfo['separator'], tbinfo['null_holder'], path)

    def append(self, local_path, ds_name, tbname):
        bag = dict(
                user_id=self.user_id,
                ds_name=ds_name,
                tbname=tbname)
        ezio_info = self.conf.get('server', 'Ezio') + '/new/info'
        res = requests.post(ezio_info, bag)
        print res.content
        result = json.loads(res.content)['result']
        ds_id = result['ds_id']
        tb_id = result['tb_id']
        separator, null_holder, schema = self.get_schema(local_path)
        self.do_action(ds_id, tb_id, schema, separator, null_holder, local_path)

    def do_action(self, ds_id, tb_id, schema, separator, null_holder,sub_path, err=None):
            thread = []
            self.flag = False
            queue = Queue.Queue()
            remotepath = '/tmp/%s/%s' % (ds_id, tb_id)

            # 边读取边发送
            # queue (num, total, data)
            th0 = threading.Thread(target=self.split_file, args=(queue, sub_path, err))
            thread.append(th0)

            # 第四步 多线程实现文件上传
            # 创建文件夹
            for i in range(5):
                th = threading.Thread(target=self.send_file, args=(queue, remotepath))
                thread.append(th)
            for t in thread:
                t.setDaemon(True)
                t.start()
            for t in thread:
                t.join()
            print '发送完毕 进行校验'
            cres =  self.check(ds_id, tb_id)
            if cres:
                error_bag = {
                        'user_id':self.user_id,
                        'ds_id':ds_id,
                        'tb_id':tb_id,
                        'err':cres,
                        'schema':schema,
                        'separator':separator,
                        'null_holder':null_holder
                        }
                with open('%s/err.log' % sub_path , 'wb') as  fo:
                    fo.write('%s' % json.dumps(error_bag))
                print '文件发送失败:%s' % json.dumps(cres)
            else:
                task_id = self.merge(ds_id, tb_id, schema, separator, null_holder)
                print '生成任务task_id:%s' % str(task_id)
                self.delete(sub_path)
            return True

    def merge(self, ds_id, tb_id, schema, separator, null_holder):
        bag = dict(
                user_id=self.user_id,
                ds_id=ds_id,
                tb_id=tb_id,
                total=self.total,
                schema=json.dumps(schema),
                separator=separator,
                null_holder=null_holder
                )
        ezio_merge = self.conf.get('server', 'Ezio') + '/new/merge'
        res = requests.post(ezio_merge, bag)
        res = json.loads(res.content)
        task_id = res['result']
        return task_id


    def check(self, ds_id, tb_id):
        bag = dict(
                ds_id=ds_id,
                tb_id=tb_id,
                total=self.total
                )
        ezio_check = self.conf.get('server', 'Ezio') + '/new/check'
        res = requests.post(ezio_check, bag)
        res = json.loads(res.content)['result']
        return res

    def split_file(self, queue, sub_path, err=None):
        print sub_path
        zipname = os.path.join(sub_path, 'all.zip')
        print zipname
        if err is None:
            res = os.listdir(sub_path)
            # 过滤压缩无关文件
            if "schema.info" in res:
                res.remove('schema.info')
            if "err.log" in res:
                res.remove('err.log')
            if "all.zip" in res:
                res.remove('all.zip')
                os.remove(zipname)
            # 压缩该文件下所有需要上传的文件
            f = zipfile.ZipFile(zipname,'w',zipfile.ZIP_DEFLATED)
            for fname in res:
                fromfile = os.path.join(sub_path, fname)
                if os.path.isdir(fromfile):
                    continue
                f.write(fromfile)
            f.close()
        size = os.path.getsize(zipname)
        total = int(math.ceil(float(size)/self.chunksize))
        self.total = total
        md5sum = os.popen('md5 %s' % zipname).read()
        md5 = md5sum.split('= ')[1].split('\n')[0]
        partnum = 0
        if err is None:
            err = [i+1  for i in range(total)]
        input = open(zipname, 'rb')
        while 1:
            chunk = input.read(self.chunksize)
            if not chunk:
                break
            partnum += 1
            if partnum in err:
                bag = {}
                bag['fname'] = 'all.zip'
                bag['partnum'] = partnum
                bag['total'] = total
                bag['size'] = size
                bag['das'] = chunk
                bag['md5'] = md5
                queue.put(bag)

        while True:
            if queue.qsize() == 0:
                self.flag = True
                break
            else:
                time.sleep(5)
        return True

    def send_file(self, queue, remotepath):
        ezio_upload  = Configuration().get('server', 'Ezio') + '/new/upload'
        while True:
            info = None
            try:
                info = queue.get(block=0)
            except Exception, e:
                if self.flag == True:
                    break
                else:
                    time.sleep(1)
            if info:
                info['path'] = remotepath
                res = requests.post(ezio_upload, data=info)
        return True

    def get_schema(self, local_path):
        fo = open('%s/schema.info' % local_path, 'rb').readlines()
        res = []
        separator = None
        null_holder = None
        count = 0
        for line in fo:
            count +=  1
            if count == 1:
                separator =  line.split('separator=')[1].split('\n')[0]
            elif count == 2:
                null_holder =  line.split('null_holder=')[1].split('\n')[0]
            else:
                temp = line.split(',')
                bag={}
                bag['title'] = temp[0]
                bag['name'] = temp[0]
                bag['type'] = int(temp[1].split('\n')[0])
                res.append(bag)
        return separator, null_holder, res

    def delete(self, path):
        print path
        res = os.listdir(path)
        for item in res:
            fpname = os.path.join(path, item)
            os.remove(fpname)

    def get_dsinfo(self, path):
        res = os.listdir(path)
        ds_name = path.split('/')[-1]
        tbs = []
        for item in res:
            sub_path = os.path.join(path, item)
            if os.path.isdir(sub_path):
                sub_res = os.listdir(sub_path)
                if 'schema.info' not in sub_res:
                    break
                else:
                    bag = {}
                    separator, null_holder, schema = self.get_schema(sub_path)
                    bag['separator'] = separator
                    bag['null_holder'] = null_holder
                    bag['tbname'] = item
                    bag['schema'] = schema
                    tbs.append(bag)
        return ds_name, tbs

    def retry(self, path):
        res = os.listdir(path)
        retry_list = []
        for item in res:
            sub_path = os.path.join(path, item)
            if os.path.isdir(sub_path):
                sub_res = os.listdir(sub_path)
                if 'schema.info' not in sub_res:
                    continue
                if 'err.log' not in sub_res:
                    continue
                if "all.zip" not in sub_res:
                    continue
                retry_list.append(sub_path)

        for sub_path in retry_list:
            err_log = os.path.join(sub_path, 'err.log')
            fo = open(err_log, 'rb').readlines()
            bag = {}
            for line in fo:
                temp = line.split('\n')[0]
                bag = json.loads(temp)
                break
            self.do_action(bag['ds_id'], bag['tb_id'], bag['schema'], bag['separator'], bag['null_holder'], sub_path, bag['err'])
        return True

if __name__=='__main__':
    domain = "haizhi"
    username = "jiaoguofu"
    password = "jiao1993"
    local_path = "/Users/jiaoguofu/Desktop/fsplit"
    ds_name = "fsplit"
    tbname = "tb"
    import datetime
    print datetime.datetime.today()
    #FuploadHelper(domain, username, password).create(local_path)
    #FuploadHelper(domain, username, password).append(local_path, ds_name, tbname)
    FuploadHelper(domain, username, password).retry(local_path)
    print datetime.datetime.today()


