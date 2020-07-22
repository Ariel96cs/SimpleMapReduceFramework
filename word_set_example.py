from client import Client

import os
from get_ip_tools import get_ip

FILE_PATH = os.path.abspath(os.path.dirname(__file__))

s = set()

if __name__ == '__main__':
    c = Client(os.path.join(FILE_PATH, 'data.txt'),
               os.path.join(FILE_PATH, 'word_set_functions.py'),client_ip=get_ip(),
               workers=[('10.0.0.3','7998'),('10.0.0.4','7998'),('10.0.0.5','7998')])

    with open(os.path.join(FILE_PATH, 'data.txt')) as fl:
        while True:
            line = fl.readline()
            if line == '':
                break
            l = line.split()
            for word in l:
                s.add(word)

    res = c.execute()
    l = []
    for x,y in res:
        if x in l:
            print(x)
            assert False,"Se partiio"
        l.append(x)

    print("myresult: ",len(l),' el otro: ',len(s))
    print("los que no estan en el mio: ",[x for x in l if x not in s])
    print('Este es s: ',s)


