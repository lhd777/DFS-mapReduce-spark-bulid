

def reducer(key, values):
    total = 0
    result = {}
    for value in values:
        total = total + int(value)
    result[key] = total
    return result