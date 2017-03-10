#!/usr/bin/env python
# -*- coding:utf-8 -*-

import sys
from fupload import FuploadHelper
from append import ApendHelper

if __name__=='__main__':
    print """
    Usage: python op confile
    op : create | append | retry
    confile:
        op:create
        confile include:
            domain=xxx
            username=xxx
            password=xxx
            ds_name=xxx
            tbname=xxx
            separator=x
            null_holder=xxx
            path=xxxx
        op:append
        confile include:
            domain=xxx
            username=xxx
            password=xxx
            ds_id=xxx
            tb_id=xxx
            separator=x
            null_holder=xxx
            path=xxxx
        op:retry
        confile include nothing
    """
    if len(sys.argv) == 1:
        exit()
    op = sys.argv[1]
    conf = sys.argv[2]
    if op == 'create':
        params = ['domain', 'username', 'password', 'path', 'tbname', 'ds_name', 'separator', 'null_holder']
    elif op == 'append':
        params = ['domain', 'username', 'password', 'path', 'tb_id', 'ds_id', 'separator', 'null_holder']
    elif op == 'retry':
        pass
    else:
        print 'error op type'
        exit()
    fo = open('%s' % conf, 'rb').readlines()
    bag = {}
    for line in fo:
        print line
        line = line.split('\n')[0]
        for  param in params:
            if line.find('%s=' % param) == 0:
                bag[param] = line.split('%s=' % param)[1]
                break
    if op in ('create', 'append'):
        path = bag['path']
        separator = bag['separator']
        null_holder = bag['null_holder']
        ds_id = bag.get('ds_id')
        tb_id = bag.get('tb_id')
        ds_name = bag.get('ds_name')
        tbname = bag.get('tbname')
    if op == 'create':
        FuploadHelper(bag['domain'], bag['username'], bag['password']).do_action(path, tbname, ds_name, separator, null_holder)
    elif op == 'append':
        ApendHelper(bag['domain'], bag['username'], bag['password']).do_action(path, separator, null_holder, ds_id, tb_id)
    elif op == 'retry':
        FuploadHelper(bag['domain'], bag['username'], bag['password']).retry(path)

