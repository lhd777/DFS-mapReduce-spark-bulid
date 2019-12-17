from pysparkling import *
import re
data_dir = './data_node/data/result.csv.blk0'
with open(data_dir) as f:
    data = f.readlines()
words = re.findall(r"[(](.*?)[)]", data[0][1:-1])
data = []
for word in words:
    word, number = word.split(',')
    data.append((word, int(number)))


sc = Context()
sc = sc.parallelize(data, 1)
result = sc.reduceByKey(lambda x,y: x+y).collect()
result = sorted(result, key=lambda x: x[1], reverse=True)
print(result)
