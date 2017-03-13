#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
import json
import time
import math
import zipfile
import Queue
import requests
import threading
import traceback
from split_file import fsplit, new_fsplit
from ssh_test import BdpSftpClient
from config import Configuration

class ApendHelper(object):
    def __init__(self, domain, username, password):
        self.domain = domain
        self.username = username
        self.password = password
        self.conf = Configuration()
        self.user_id = self.login()

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


    def do_action(self, local_path, ds_name, tbname):
        user_id = self.user_id
        # 第一步 通过Ezio服务获取数据源和工作表信息, 返回ds_id, tb_id
        bag = dict(
                user_id=self.user_id,
                ds_name=ds_name,
                tbname=tbname)
        ezio_info = self.conf.get('server', 'Ezio') + '/new/info'
        res = requests.post(ezio_info, bag)
        print res.content
        result = json.loads(res.content)['result']
        user_id = result['user_id']
        ds_id = result['ds_id']
        tb_id = result['tb_id']

        # 第二步 获取schema信息
        separator, null_holder, schema = self.get_schema(local_path)

        # 第三步 将路径下的数据全部分割 
        remotepath = '/tmp/%s/%s' % (ds_id, tb_id)
        chunksize = 5 * 1024 * 1024
        thread = []
        self.flag = False
        queue = Queue.Queue()
        th0 = threading.Thread(target=self.split_file, args=(queue, local_path, chunksize))
        thread.append(th0)

        for i in range(5):
            th = threading.Thread(target=self.send_file, args=(queue, remotepath))
            thread.append(th)
        for t in thread:
            t.setDaemon(True)
            t.start()
        for t in thread:
            t.join()

        cres = self.check(ds_id, tb_id)
        if cres:
            error_bag = {
                    'user_id':user_id,
                    'ds_id':ds_id,
                    'tb_id':tb_id,
                    'err':cres,
                    'schema':schema,
                    'separator':separator,
                    'null_holder':null_holder
                    }
            with open('%s/err.log' % local_path, 'wb') as  fo:
                fo.write('%s' % json.dumps(error_bag))
            print '文件发送失败:%s' % json.dumps(cres)
        else:
            task_id = self.merge(user_id, ds_id, tb_id,schema, separator, null_holder)
            print '生成任务task_id:%s' % str(task_id)
        return True

    def check(self, ds_id, tb_id):
        bag = dict(
                ds_id=ds_id,
                tb_id=tb_id,
                total=self.total,
                )
        ezio_check = self.conf.get('server', 'Ezio') + '/new/check'
        res = requests.post(ezio_check, bag)
        res = json.loads(res.content)['result']
        return res

    def merge(self, user_id, ds_id, tb_id, schema, separator, null_holder):
        bag = dict(
                user_id=user_id,
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

    def split_file(self, queue, sub_path, chunksize):
        res = os.listdir(sub_path)
        zipname = os.path.join(sub_path, 'all.zip')
        if "schema.info" in res:
            res.remove('schema.info')
        if "err.log" in res:
            res.remove('err.log')
        if "all.zip" in res:
            res.remove('all.zip')
            os.remove(zipname)
        f = zipfile.ZipFile(zipname,'w',zipfile.ZIP_DEFLATED)
        for fname in res:
            fromfile = os.path.join(sub_path, fname)
            if os.path.isdir(fromfile):
                continue
            f.write(fromfile)
        f.close()
        size = os.path.getsize(zipname)
        total = int(math.ceil(float(size)/chunksize))
        self.total = total
        md5sum = os.popen('md5 %s' % zipname).read()
        md5 = md5sum.split('= ')[1].split('\n')[0]
        partnum = 0
        input = open(zipname, 'rb')
        while 1:
            chunk = input.read(chunksize)
            if not chunk:
                break
            partnum += 1
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

if __name__=='__main__':
    domain = "haizhi"
    username = "jiaoguofu"
    password = "jiao1993"
    local_path = "/Users/jiaoguofu/Desktop/fsplit/tb"
    ds_name = "fsplit"
    tbname = "tb"
    import datetime
    print datetime.datetime.today()
    ApendHelper(domain, username, password).do_action(local_path, ds_name, tbname)
    print datetime.datetime.today()


