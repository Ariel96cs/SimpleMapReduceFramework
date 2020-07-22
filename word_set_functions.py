def mapper(k, v):
    res = []
    for word in v.split():
        res.append((word, 1))
    return res


def reducer(k, l):
    return k, True


def combiner(k, l):
    return reducer(k, l)
