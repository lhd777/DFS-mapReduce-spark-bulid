import importlib
from common import * 
import os
import sys
def load_module(module_file, module_name):
    module_path = os.path.join(task_node_dir, module_name + '.py')
    output = open(module_path, 'w')
    for line in module_file:
        output.write(line)
    output.close()
    sys.path.append(task_node_dir)
    dirs = task_node_dir
    module = importlib.import_module(module_name)
    module = importlib.reload(module)
    module = getattr(module, module_name)
    return module

def hash_partition(key, num):
    return hash(key)%num
