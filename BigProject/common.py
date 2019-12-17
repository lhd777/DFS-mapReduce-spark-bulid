dfs_blk_size = 8196 * 1000
dfs_replication = 3
BUF_SIZE = dfs_blk_size * 2

data_node_dir = './data_node/data'
# data_node_host = ['thumm01', 'thumm02', 'thumm03', 'thumm04', 'thumm05']
data_node_host = 'localhost'
data_node_port = 20005  # 数据节点的端口
data_node_listening_num = 100
name_node_dir = './name_node/data'
name_node_host = 'localhost'
name_node_port = 20001  # 管理节点的端口
name_node_listening_num = 100

# host_lists =  ['thumm01', 'thumm02', 'thumm03', 'thumm04', 'thumm05']

host_lists = ['localhost']
job_tracker_dir = './job_track/data'
job_tracker_port = 20002
job_tracker_host = 'localhost'
job_tracker_listenning_num = 100


# task_tracker_hosts = ['thumm01', 'thumm02', 'thumm03', 'thumm04', 'thumm05']
task_tracker_hosts = 'localhost'
task_tracker_ports = [20003]
task_tracker_status = ['ready']
task_node_dir = './task_node/data'
split_size = 8196 * 1000
