import os
from rpyc import Service
from rpyc.utils.server import ThreadedServer

import socket
from common import  *
import rpyc

import time
from utils import * 

class TaskTracker(Service):
    def __init__(self):
        pass

    def exposed_load_mapper(self, mapper_file, mapper_name, share, job):
        if not os.path.exists(task_node_dir):
            os.system('mkdir -p {}'.format(task_node_dir))
        
        # split no
        split_no = share['split_no']
        # mapper function
        mapper = load_module(mapper_file, mapper_name)
        # get the share from dfs
        share = self.read_dfs_file(share)
        # mapper the dict
        mapper_data = mapper(share)
        # partition the dict
        partitions_path = self.partition(job, split_no, mapper_data) 
        return partitions_path

    def exposed_load_reducer(self, reducer_file, reducer_name, partition_infos, job):
        lines = []
        for partition_info in partition_infos:
            host_name = partition_info['host_name']
            port = partition_info['port']
            task_track_conn = rpyc.connect(host_name, port)
            partition_file = task_track_conn.root.read_file(partition_info['partition'])
            lines.extend(partition_file)
            task_track_conn.root.delete_file(partition_info['partition'])
            task_track_conn.close()
        tmp = {}
       
        for line in lines:
            words = line.split(' ')
            key, value = words[0:2]
            value_list = tmp.setdefault(key,[])
            value_list.append(value)
        reducer = load_module(reducer_file, reducer_name)
        results = []
        for key, values in tmp.items():
            result = reducer(key, values)
            for key,value in result.items():
                results.append((key,value))
        return results

    def exposed_read_file(self, file_path):
        fd = open(file_path, 'r')
        content = fd.readlines()
        return content
    
    def exposed_delete_file(self, file_path):
        os.remove(file_path)

    def read_dfs_file(self, share):
        splits = share['split_data']
        input_file = share['input_file']
        data = ''
        for split in splits:
            host_name = split['host_name']
            blk_no = split['blk_no']
            blk_path = input_file + '.blk{}'.format(int(blk_no))
            if socket.gethostname() == 'localhost' or socket.gethostname() == host_name or host_name == 'localhost':
                print('local')
                print(split['end_inds'], int(split['begin_inds']))
                file_size = min(int(split['end_inds']) - int(split['begin_inds']) + 1, dfs_blk_size)
                dfs_path = os.path.join(data_node_dir, blk_path)
                fp = open(dfs_path, 'r')
                fp.seek(0, int(split['begin_inds']))
                rev_msg = fp.read(file_size)
                data += rev_msg 
                #print(data)
                fp.close()
            else:
                data_node_conn = rpyc.connect(host_name, data_node_port)
                rev_msg = data_node_conn.root.load(blk_path, int(split['begin_inds']), int(split['end_inds']))
                data += rev_msg 
                print(len(data))
                data_node_conn.close()
        return data


    def partition(self, job, split_no, dicts):
        partitions = []
        partitions_path = []
        num = job['reduce_num']
        
        for i in range(num):
            partition_path = os.path.join(task_node_dir,'{}_partition_{}_{}'.format(job['input_file'], split_no, i))
            partition = open(partition_path,'w')
            partitions.append(partition)
            partitions_path.append(partition_path)
        for key,value in dicts.items():
            partition_no = hash_partition(key, num)
            partitions[partition_no].write('{} {}\n'.format(key,value))
        for i in range(num):
            partitions[i].close()
        return partitions_path

    

if __name__ == "__main__":
    t = ThreadedServer(TaskTracker, port=task_tracker_ports[0])
    t.start()