#!/usr/bin/env python
# -*- coding:utf-8 -*-


import sys, os
import math

def fsplit(fromfile, todir, chunksize, fn):
    fromfile = fromfile + '/' + fn
    size = os.path.getsize(fromfile)
    total = int(math.ceil(float(size)/chunksize))
    if not os.path.exists(todir):
        os.mkdir(todir)
    else:
        for fname in os.listdir(todir):
            os.remove(os.path.join(todir, fname))
    partnum = 0
    input = open(fromfile, 'rb')
    while 1:
        chunk = input.read(chunksize)
        if not chunk:
            break
        partnum = partnum+1
        filename = os.path.join(todir, ('%s_%s_part_%s.temp' % (fn, total, partnum)))
        fileobj = open(filename, 'wb')
        fileobj.write(chunk)
        fileobj.close()
    input.close()
    return partnum

def new_fsplit(fromdir, todir, chunksize):
    # 第一步 获取路径下的所有的文件名称 除去schema.info
    res = os.listdir(fromdir)
    if "schema.info" in res:
        res.remove('schema.info')
    if "err.log" in res:
        res.remove("err.log")
    log = []
    for fname in res:
        fromfile = fromdir + '/' + fname
        size = os.path.getsize(fromfile)
        total = int(math.ceil(float(size)/chunksize))
        if not os.path.exists(todir):
            os.makedirs(todir)
        else:
            for name in os.listdir(todir):
                if name.find('BDP_%s' % fname) == 0:
                    os.remove(os.path.join(todir, name))
        partnum = 0
        input = open(fromfile, 'rb')
        while 1:
            chunk = input.read(chunksize)
            if not chunk:
                break
            partnum = partnum + 1
            filename = os.path.join(todir, ('BDP_%s_%s_part_%s.temp' % (fname, total, partnum)))
            fileobj = open(filename, 'wb')
            fileobj.write(chunk)
            fileobj.close()
        input.close()
        bag = {}
        md5sum = os.popen('md5 %s' % fromfile).read()
        md5 = md5sum.split('= ')[1].split('\n')[0]
        bag['md5'] = md5
        bag['partnum'] = partnum
        bag['fname'] = fname
        log.append(bag)
    return log

def merge_file(targetdir, total, fname):
    if fname in os.listdir(targetdir):
        os.remove(os.path.join(targetdir, fname))
    outfile = open(os.path.join(targetdir,fname),'wb')
    for i in range(1, total+1):
        filepath = os.path.join(targetdir,"%s_%s_part_%s.temp" % (fname, total, i))
        infile = open(filepath,'rb')
        data = infile.read()
        outfile.write(data)
        infile.close()
    outfile.close()

