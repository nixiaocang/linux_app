#!/usr/bin/env python
# -*- coding:utf-8 -*-

import sys
from  bdp_batch_up import FuploadHelper
from bdp_batch_up_sftp import FuploadHelper2

if __name__=='__main__':
    print """
    Usage: python way op confile
    way: 0|1 文件落盘与否 0 不落盘 1 落盘
    op : create | append | retry
    confile:
        op:create
        confile include:
            domain=xxx
            username=xxx
            password=xxx
            path=xxxx   # 数据文件夹上层路径
        op:append
        confile include:
            domain=xxx
            username=xxx
            password=xxx
            ds_name=xxx
            tbname=xxx
            path=xxxx   # 数据文件夹路径
        op:retry
        confile include:
        domain=xxx
        username=xxx
        password=xxx
        path=xxx # 数据文件夹上层路径
    """
    if len(sys.argv) == 1:
        exit()
    way = sys.argv[1]
    op = sys.argv[2]
    conf = sys.argv[3]
    if str(way) == '0':
        upload_helper = FuploadHelper
    elif str(way) == "1":
        upload_helper = FuploadHelper2
    else:
        print '不支持的方式'
        sys.exit()

    if op == 'create':
        params = ['domain', 'username', 'password', 'path']
    elif op == 'append':
        params = ['domain', 'username', 'password', 'path', 'tbname', 'ds_name']
    elif op == 'retry':
        params = ['domain', 'username', 'password', 'path']
    else:
        print 'error op type'
        sys.exit()
    fo = open('%s' % conf, 'rb').readlines()
    bag = {}
    for line in fo:
        print line
        line = line.split('\n')[0]
        for  param in params:
            if line.find('%s=' % param) == 0:
                bag[param] = line.split('%s=' % param)[1]
                break
    if op == 'append':
        tbname = bag['tbname']
        ds_name = bag['ds_name']
    path = bag['path']
    if op == 'create':
        upload_helper(bag['domain'], bag['username'], bag['password']).create(path)
    elif op == 'append':
        upload_helper(bag['domain'], bag['username'], bag['password']).append(path, ds_name, tbname)
    elif op == 'retry':
        upload_helper(bag['domain'], bag['username'], bag['password']).retry(path)

