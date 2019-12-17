import socket
import os
from common import *
import rpyc
import pandas as pd
import time
from io import StringIO
import sys
import json

from config import *


class Client(object):
    def __init__(self):
        self._name_node_host = name_node_host
        self._name_node_port = name_node_port
        self._job_tracker_host = job_tracker_host
        self._job_tracker_port = job_tracker_port

    def connect(self):
        self._name_node_conn = rpyc.connect(self._name_node_host, self._name_node_port)
   
    def disconnect(self):
        self._name_node_conn.close()
   
    def ls(self, dfs_path):
        self.connect()
        rev_msg = self._name_node_conn.root.ls(dfs_path)
        print(rev_msg)
        self.disconnect()

    def copy_from_local(self, local_path, dfs_path):
        self.connect()
        file_size = os.path.getsize(local_path)
        fat_pd = self._name_node_conn.root.new_fat_item(dfs_path, file_size)
        print('Fat: \n{}'.format(fat_pd))
        fat = pd.read_csv(StringIO(fat_pd))
        fp = open(local_path)
        flag = dfs_replication
        for idx, row in fat.iterrows():
            if flag == 0:
                flag = dfs_replication
            if flag >= dfs_replication:
                data = fp.read(int(row['blk_size']))
            data_node_conn = rpyc.connect(row['host_name'], data_node_port)
            blk_path = dfs_path + '.blk{}'.format(row['blk_no'])
            rev_msg = data_node_conn.root.store(blk_path, data)
            print(rev_msg)
            data_node_conn.close()
            flag -= 1
        fp.close()
        self.disconnect()

    def copy_to_local(self, dfs_path, local_path):
        self.connect()
        fat_pd = self._name_node_conn.root.get_fat_item(dfs_path)
        print('Fat: \n{}'.format(fat_pd))
        fat = pd.read_csv(StringIO(fat_pd))
        fat = fat.drop_duplicates(['blk_no'])
        fp = open(local_path, 'w')
        for idx, row in fat.iterrows():
            data_node_conn = rpyc.connect(row['host_name'], data_node_port)
            blk_path = dfs_path + '.blk{}'.format(row['blk_no'])
            rev_msg = data_node_conn.root.load(blk_path)
            fp.write(rev_msg)
            data_node_conn.close()

        fp.close()
        self.disconnect()

    def rm(self, dfs_path):
        self.connect()
        fat_pd = self._name_node_conn.root.rm_fat_item(dfs_path)
        print('Fat: \n{}'.format(fat_pd))
        fat = pd.read_csv(StringIO(fat_pd))
        for idx, row in fat.iterrows():
            data_node_conn = rpyc.connect(row['host_name'], data_node_port)
            blk_path = dfs_path + '.blk{}'.format(row['blk_no'])
            rev_msg = data_node_conn.root.rm(blk_path)
            print(rev_msg)
            data_node_conn.close()
        self.disconnect()

    def cat(self, dfs_path):
        self.connect()
        fat_pd = self._name_node_conn.root.get_fat_item(dfs_path)
        print('Fat: \n{}'.format(fat_pd))
        fat = pd.read_csv(StringIO(fat_pd))
        fat = fat.drop_duplicates(['blk_no'])
        for idx, row in fat.iterrows():
            data_node_conn = rpyc.connect(row['host_name'], data_node_port)
            blk_path = dfs_path + '.blk{}'.format(row['blk_no'])
            rev_msg = data_node_conn.root.load(blk_path)
            print(rev_msg)
            data_node_conn.close()
        self.disconnect()
        return rev_msg

    def map_reduce(self, input_file, output_file, mapper_file_path, mapper_name, reducer_file_path, reducer_name):
        fd = open(mapper_file_path, 'r')
        mapper_file = fd.readlines()
        fd.close()
        fd = open(reducer_file_path, 'r')
        reducer_file = fd.readlines()
        fd.close()
        cfg = json.dumps(configuration)
        job_track_conn = rpyc.connect(self._job_tracker_host, self._job_tracker_port)
        job_track_conn.root.load_task(input_file, output_file, mapper_file, mapper_name, reducer_file, reducer_name, cfg)
        job_track_conn.close()
        print('job finished')

    def spark(self, input_file, output_file, mapper_file_path, mapper_name):
        fd = open(mapper_file_path, 'r')
        mapper_file = fd.readlines()
        fd.close()
        cfg = json.dumps(configuration)
        job_track_conn = rpyc.connect(self._job_tracker_host, self._job_tracker_port)
        job_track_conn.root.load_task(input_file, output_file, mapper_file, mapper_name, cfg)
        job_track_conn.close()
        print('job finished')

    def format(self):
        self.connect()
        request = 'format'
        self._name_node_socket.send(request.encode('utf-8'))
        rev_msg = self._name_node_socket.recv(BUF_SIZE)
        print(str(rev_msg, encoding='utf-8'))
        for host in host_lists:
            data_node_conn = rpyc.connect(row['host_name'], data_node_port)
            rev_msg = data_node_conn.root.format()
            data_node_socket.close()
        self.disconnect()


if __name__ == '__main__':
    client = Client()
    arguments = sys.argv
    arguments = dict(enumerate(arguments))
    if len(arguments) == 0:
        print('please input the command')
    else:
        cmd_type = arguments.get(1, ' ')
        if '-ls' in cmd_type:
            dfs_path = arguments.get(2, '.')
            client.ls(dfs_path)
        elif '-copyToLocal' in cmd_type:
            dfs_path = arguments.get(2, ' ')
            local_path = arguments.get(3, ' ')
            client.copy_to_local(dfs_path, local_path)
        elif '-copyFromLocal' in cmd_type:
            local_path = arguments.get(2, ' ')
            dfs_path = arguments.get(3, ' ')
            client.copy_from_local(local_path, dfs_path)
        elif '-rm' in cmd_type:
            dfs_path = arguments.get(2, ' ')
            client.rm(dfs_path)
        elif '-cat' in cmd_type:
            dfs_path = arguments.get(2, ' ')
            client.cat((dfs_path))
        elif '-map_reduce' in cmd_type:
            print('begin')
            input_file = arguments.get(2, ' ')
            out_file = arguments.get(3, ' ')
            mapper_file_path = arguments.get(4, ' ')
            mapper_name = arguments.get(5, ' ')
            reducer_file_path = arguments.get(6, ' ')
            reducer_name = arguments.get(7, ' ')
            client.map_reduce(input_file, out_file, mapper_file_path, mapper_name, reducer_file_path, reducer_name)
        elif '-spark' in cmd_type:
            print("begin")
            input_file = arguments.get(2, ' ')
            out_file = arguments.get(3, ' ')
            mapper_file_path = arguments.get(4, ' ')
            mapper_name = arguments.get(5, ' ')
            client.spark(input_file, out_file, mapper_file_path, mapper_name)

        elif '-statistics' in cmd_type:
            print('begin')
            input_file = arguments.get(2, ' ')
            output_file = arguments.get(3, ' ')
            client.statistics(input_file, output_file)
        elif '-test' in cmd_type:
            client.test()
        else:
            print('do not find the command')





