#!/usr/bin/python
# -*- coding: UTF-8 -*-

from rpyc import Service
from rpyc.utils.server import ThreadedServer
import os
from common import *
import time 

class DataNode(Service):
    def __init__(self):
        pass

    def exposed_format(self):
        format_command = 'rm -rf {}'.format(data_node_dir)
        os.system(format_command)
        return 'format successfully at {}'.format(data_node_dir)

    def exposed_store(self, dfs_path, data):
        dfs_path = os.path.join(data_node_dir, dfs_path)
        if not os.path.exists(dfs_path):
            os.system('mkdir -p {}'.format(os.path.dirname(dfs_path)))
        fp = open(dfs_path, 'w')
        fp.write(data)
        fp.close()
        return 'store successfully at {}'.format(dfs_path)

    def exposed_load(self, dfs_path, start_index=0, end_index=dfs_blk_size-1):
        file_size = min(end_index - start_index + 1, dfs_blk_size)
        dfs_path = os.path.join(data_node_dir, dfs_path)
        fp = open(dfs_path, 'r')
        fp.seek(0, start_index)
        data = fp.read(file_size)
        fp.close()
        return data

    

    def exposed_rm(self, dfs_path):
        dfs_path = os.path.join(data_node_dir, dfs_path)
        os.system('rm ' + dfs_path)
        return 'remove successsfully at {}'.format(dfs_path)

    

if __name__ == '__main__':
    t = ThreadedServer(DataNode, port=data_node_port)
    t.start()



