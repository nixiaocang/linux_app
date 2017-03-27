#!/usr/bin/env python
# -*- coding:utf-8 -*-

import paramiko
class BdpSftpClient(object):
    def __init__(self, host, port, username, password):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self._ssh_fd = self.ssh_connect()
        self._sftp_fd = self.sftp_open()

    def ssh_connect(self):
        try:
            _ssh_fd = paramiko.SSHClient()
            _ssh_fd.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            _ssh_fd.connect(self.host, username=self.username, password=self.password)
        except Exception, e:
            print( 'ssh %s@%s: %s' % (self.username,self.host, e) )
        return _ssh_fd

    def ssh_exec_command(self, command):
        stdin, stdout, stderr = self._ssh_fd.exec_command(command)
        return True

    def sftp_open(self):
        return self._ssh_fd.open_sftp()

    def sftp_put(self, localpath, remotepath):
        return self._sftp_fd.put(localpath, remotepath)

    def sftp_get(self, localpath, remotepath):
        return self._sftp_fd.get(remotepath, localpath)

    def close(self):
        self._sftp_fd.close()
        self._ssh_fd.close()


if __name__=='__main__':
    remotepath='/root/big.csv'
    localpath='/Users/jiaoguofu/Desktop/big.csv'
    bsftp = BdpSftpClient('115.28.9.49', 22, "root", "Jiao1993")
    #bsftp.sftp_put(localpath, remotepath)
    stdin, stdout, stderr = bsftp._ssh_fd.exec_command("mkdir -p %s" % '/temp/ds_id/test')
    print stdout.readlines()
    bsftp.close()
