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
        #self.total = 0
        self.total = 1

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
                th = threading.Thread(target=self.send_file, args=(queue, remotepath))
                thread.append(th)
            for t in thread:
                t.setDaemon(True)
                t.start()
            for t in thread:
                t.join()
            cres =  self.check(ds_id, tb_id)
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
                with open('%s/err.log' % sub_path , 'wb') as  fo:
                    fo.write('%s' % json.dumps(error_bag))
                print '文件发送失败:%s' % json.dumps(cres)
                continue
            else:
                task_id = self.merge(user_id, ds_id, tb_id, schema, separator, null_holder)
                print '生成任务task_id:%s' % str(task_id)
        return True

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

    def split_file(self, queue, sub_path, chunksize):
        res = os.listdir(sub_path)
        zipname = os.path.join(sub_path, 'all.zip')
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
        total = int(math.ceil(float(size)/chunksize))
        self.total = total
        md5sum = os.popen('md5 %s' % fromfile).read()
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
    user_id = '319d170b76123c08e7bca229958e0d3f'
    import datetime
    print datetime.datetime.today()
    ds_id = 'ds_e4a2087b021d41ee8dbe4c26e9ae7107'
    tb_id = 'tb_74ac5b338d78420c9ac9d950bedab9f7'
    separator = ','
    null_holder = 'null'
    schema = [{"type": 2, "name": "field1", "title": "field1"}, {"type": 2, "name": "field2", "title": "field2"}, {"type": 2, "name": "field3", "title": "field3"}, {"type": 2, "name": "field4", "title": "field4"}, {"type": 2, "name": "field5", "title": "field5"}, {"type": 2, "name": "field6", "title": "field6"}, {"type": 2, "name": "field7", "title": "field7"}, {"type": 2, "name": "field8", "title": "field8"}, {"type": 2, "name": "field9", "title": "field9"}, {"type": 2, "name": "field10", "title": "field10"}]
    FuploadHelper(domain, username, password).do_action(local_path)
    #print FuploadHelper(domain, username, password).merge(user_id, ds_id, tb_id, schema, separator, null_holder)
    print datetime.datetime.today()


