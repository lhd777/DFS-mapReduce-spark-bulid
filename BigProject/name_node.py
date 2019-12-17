
from rpyc import Service
from rpyc.utils.server import ThreadedServer
from common import *
import os
import math
import pandas as pd
import numpy as np
class NameNode(Service):
    def __init__(self):
        super().__init__()

    def exposed_format(self):
        format_command = 'rm -rf {}'.format(name_node_dir)
        os.system(format_command)
        return 'format successfully at {}'.format(name_node_dir)

    def exposed_new_fat_item(self, dfs_path, file_size):
        blk_nums = int(math.ceil(file_size / dfs_blk_size))
        data_pd = pd.DataFrame(columns=['blk_no', 'host_name', 'blk_size'])
        idx = 0
        for i in range(blk_nums):
            blk_no = i
            hosts_name = np.random.choice(host_lists, dfs_replication, replace = True)
            print(hosts_name)
            blk_size = min(dfs_blk_size, file_size - i * dfs_blk_size)
            for host_name in hosts_name:
                data_pd.loc[idx] = [blk_no, host_name, blk_size]
                idx = idx + 1
        local_path = os.path.join(name_node_dir, dfs_path)
        os.system(' mkdir -p {}'.format(os.path.dirname(local_path)))
        data_pd.to_csv(local_path, index=False)
        return data_pd.to_csv(index=False)

    def exposed_rm_fat_item(self, dfs_path):
        local_path = os.path.join(name_node_dir, dfs_path)
        response = pd.read_csv(local_path)
        os.system('rm ' + local_path)
        return response.to_csv(index=False)

    def exposed_get_fat_item(self, dfs_path):
        local_path = os.path.join(name_node_dir, dfs_path)
        response = pd.read_csv(local_path)
        return response.to_csv(index=False)

    def exposed_ls(self, dfs_path):
        local_path = os.path.join(name_node_dir, dfs_path)
        if not os.path.exists(local_path):
            response = 'No such file or directory at {}'.format(dfs_path)
        elif os.path.isdir(local_path):
            files = os.listdir(local_path)
            response = str(files)
        else:
            response = pd.read_csv(local_path).to_csv(index=False)
        return response
    
    

if __name__ == '__main__':
    t = ThreadedServer(NameNode, port=name_node_port)
    t.start()
