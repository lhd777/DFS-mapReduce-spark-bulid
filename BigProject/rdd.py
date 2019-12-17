def rdd(share):
    counts = (
    Context()
    .textFile(share)
    .flatMap(lambda line: line.split(' '))
    .Map(lambda word,: (word, 1))
    .reduceByKey(lambda a, b: a + b)
    .collect()
    )
    return counts
