import os
from rpyc import Service
from rpyc.utils.server import ThreadedServer

import socket
import json
from common import *
import rpyc
import pandas as pd
from io import StringIO
import math
import random
import time
from config import *


class JobTracker(Service):
    def __init__(self):
        self._task_tracker_hosts = task_tracker_hosts
        self._task_tracker_ports = task_tracker_ports

        self._name_node_host = name_node_host
        self._name_node_port = name_node_port

        self._job_id = 0

    def exposed_load_task(self, input_file, output_file, mapper_file, mapper_name, cfg):
        print('begin')
        job = {}
        job['job_id'] = self.get_job_id()
        job['cfg'] = json.loads(cfg)
        job['mapper_num'] = job['cfg']['mapper_num']
        job['input_file'] = input_file
        job['output_file'] = output_file
        job['mapper_file'] = mapper_file
        job['mapper_name'] = mapper_name
        job['status'] = 'waiting'
        job['finished_mapper'] = 0

        self.process_task(job)
        print('end')

    def process_task(self, job):
        fat = self.get_fat_item(job['input_file'])
        file_size = fat.sum()['blk_size']
        split_nums = int(math.ceil(file_size / split_size))
        fat_no = 0
        begin_inds = 0
        end_inds = 0
        shares = []
        print('begin')
        for i in range(split_nums):
            task_mapper = {}
            split_no = i
            current_split_size = min(split_size, file_size - i * split_size)
            split_data = []
            host = fat.loc[fat_no]['host_name']
            while current_split_size > 0:
                end_inds = min(fat.loc[fat_no]['blk_size'], begin_inds + current_split_size)
                current_split_size -= (end_inds - begin_inds)
                split_data.append({'blk_no': fat.loc[fat_no]['blk_no'], 'blk_size': fat.loc[fat_no]['blk_size'],
                                   'host_name': fat.loc[fat_no]['host_name'], 'begin_inds': begin_inds,
                                   'end_inds': end_inds})
                if end_inds == fat.loc[fat_no]['blk_size']:
                    begin_inds = 0
                    end_inds = 0
                    fat_no += 1
                else:
                    begin_inds = end_inds

            if host in task_tracker_hosts:
                shares.append(
                    {'job_id': job['job_id'], 'input_file': job['input_file'], 'host_name': host, 'split_no': i,
                     'split_data': split_data})
            else:
                task_track_random_inds = random.randint(0, len(self._task_tracker_hosts) - 1)
                shares.append({'job_id': job['job_id'], 'input_file': job['input_file'],
                               'host_name': self.task_tracker_hosts[task_track_random_inds], 'split_no': i,
                               'split_data': split_data})

        print("load the map")
        mapper = {'connections': [], 'responses': []}
        for share in shares:
            task_track_conn = rpyc.connect(share['host_name'], self._task_tracker_ports[0])
            load_mapper = rpyc.async_(task_track_conn.root.load_mapper)
            response = load_mapper(job['mapper_file'], job['mapper_name'], share, job)
            mapper['connections'].append(task_track_conn)
            mapper['responses'].append(response)
        connections = mapper['connections']
        responses = mapper['responses']
        result_file = os.path.join(task_node_dir, 'tmp_file.csv')
        fd = open(result_file, 'w')
        for connection, response in zip(connections, responses):
            results = response.value
            fd.write(str(results))
            connection.close()
        fd.close()
        self.save(result_file, job['output_file'])

    def get_fat_item(self, dfs_path):
        name_node_conn = rpyc.connect(self._name_node_host, self._name_node_port)
        fat_pd = name_node_conn.root.get_fat_item(dfs_path)
        print('Fat: \n{}'.format(fat_pd))
        fat = pd.read_csv(StringIO(fat_pd))
        fat = fat.drop_duplicates(['blk_no'])
        fat = fat.reset_index()
        name_node_conn.close()
        return fat

    def get_job_id(self):
        job_id = self._job_id
        self._job_id += 1
        return job_id

    def save(self, local_path, dfs_path):
        name_node_conn = rpyc.connect(self._name_node_host, self._name_node_port)
        file_size = os.path.getsize(local_path)
        fat_pd = name_node_conn.root.new_fat_item(dfs_path, file_size)
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
        name_node_conn.close()


if __name__ == "__main__":
    # mapper_file = open('mapper.py')
    # mapper_lines = mapper_file.readlines()
    # reducer_file = open('reducer.py')
    # reducer_lines = reducer_file.readlines()
    # split_file = open('split.py')
    # split_lines = split_file.readlines()

    # task_track_conn.root.upload_mapper(python_lines, "Client")
    # task_track_conn.close()
    # job_tracker = JobTracker()
    t = ThreadedServer(JobTracker, port=job_tracker_port)
    t.start()
    # job_tracker.get_fat_item('client.csv')
    # job = {}
    # job['job_id']= 0
    # job['input_file'] = 'test15.csv'
    # job['output_file'] = 'output1.csv'
    # job['mapper_file'] = mapper_lines
    # job['mapper_name'] = 'mapper'
    # job['reducer_file'] = reducer_lines
    # job['reducer_name'] = 'reducer'
    # job['reduce_num'] = 2

    # job_tracker.process_task(job)

