

def mapper_square(share):
    lines = share.split('\n')
    map_data = {}
    
    while '' in lines:
        lines.remove('')
    for line in lines:
        words = line.split(' ')
        for word in words:
            #map_data[word] = map_data.get(word, 0) + 1 
            map_data[1] = map_data.get(1, 0) +  int(word) * int(word) 
    return map_data


def mapper_count(share):
    lines = share.split('\n')
    map_data = {}
    while '' in lines:
        lines.remove('')
    for line in lines:
        words = line.split(' ')
        for word in words:
            map_data[1] = map_data.get(1, 0) + 1
    return map_data


def mapper_sum(share):
    lines = share.split('\n')
    map_data = {}
    
    while '' in lines:
        lines.remove('')
    for line in lines:
        words = line.split(' ')
        for word in words:
            map_data[1] = map_data.get(1, 0) + int(word)
    return map_data


def mapper(share):
    lines = share.split('\n')
    map_data = {}
    
    while '' in lines:
        lines.remove('')
    for line in lines:
        words = line.split(' ')
        for word in words:
            map_data[word] = 1
    return map_data
