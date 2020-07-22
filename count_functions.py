def mapper(k,v):
    words = v.split()
    result = []
    for word in words:
        result.append((word,1))
    return result


def reducer(k,iterator):
    result = 0
    for i in iterator:
        result += i
    return k,result


def combiner(k,iterator):
    return reducer(k,iterator)
