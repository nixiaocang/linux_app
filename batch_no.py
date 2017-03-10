#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
import json
import time
import Queue
import math
import requests
import threading
import traceback
from split_file import fsplit, new_fsplit
from ssh_test import BdpSftpClient
from config import Configuration

class FuploadHelper(object):
    def __init__(self, domain, username, password):
        self.domain = domain
        self.username = username
        self.password = password
        self.conf = Configuration()
        self.user_id = self.login()
        self.flag = False

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

    def do_action(self, local_path):

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
        user_id = result['user_id']
        ds_id = result['ds_id']
        db_id = result['db_id']
        tb_res = result['tbs']

        # 第三步 将路径下的数据全部分割 
        for tbinfo in tb_res:
            tb_id = tbinfo['tb_id']
            schema = tbinfo['schema']
            separator = tbinfo['separator']
            null_holder = tbinfo['null_holder']
            chunksize = 5 * 1024 * 1024
            sub_path = os.path.join(local_path, tbinfo['tbname'])
            thread = []
            self.flag = False
            queue = Queue.Queue()
            remotepath = '/tmp/%s/%s' % (ds_id, tb_id)

            # 边读取边发送
            # queue (num, total, data)
            th0 = threading.Thread(target=self.split_file, args=(queue, sub_path, chunksize))
            thread.append(th0)

            # 第四步 多线程实现文件上传
            # 创建文件夹
            for i in range(5):
                th = threading.Thread(target=self.send_file, args=(queue, remotepath, user_id, ds_id, tb_id, schema, separator, null_holder))
                thread.append(th)
            for t in thread:
                t.setDaemon(True)
                t.start()
            for t in thread:
                t.join()
        return True

    def split_file(self, queue, sub_path, chunksize):
        res = os.listdir(sub_path)
        if "schema.info" in res:
            res.remove('schema.info')
        if "err.log" in res:
            res.remove('err.log')
        for fname in res:
            fromfile = os.path.join(sub_path, fname)
            size = os.path.getsize(fromfile)
            total = int(math.ceil(float(size)/chunksize))
            md5sum = os.popen('md5 %s' % fromfile).read()
            md5 = md5sum.split('= ')[1].split('\n')[0]

            partnum = 0
            input = open(fromfile, 'rb')
            while 1:
                chunk = input.read(chunksize)
                if not chunk:
                    break
                partnum += 1
                bag = {}
                bag['fname'] = fname
                bag['partnum'] = partnum
                bag['total'] = total
                bag['size'] = size
                bag['data'] = chunk
                bag['md5'] = md5
                queue.put(bag)
        while True:
            if queue.qsize() == 0:
                self.flag = True
                break
            else:
                time.sleep(5)
        return

    def send_file(self, queue, remotepath, user_id, ds_id, tb_id, schema, separator, null_holder):
        ezio_upload  = Configuration().get('server', 'Ezio') + '/api/newupload'
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
                info['user_id'] = user_id
                info['ds_id'] = ds_id
                info['tb_id'] = tb_id
                info['schema'] = json.dumps(schema)
                info['separator'] = separator
                info['null_holder'] = null_holder
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
        res = os.listdir(path)
        for item in res:
            os.remove(item)

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

if __name__=='__main__':
    domain = "haizhi"
    username = "jiaoguofu"
    password = "jiao1993"
    local_path = "/Users/jiaoguofu/Desktop/fsplit"
    import datetime
    print datetime.datetime.today()
    FuploadHelper(domain, username, password).do_action(local_path)
    print datetime.datetime.today()


