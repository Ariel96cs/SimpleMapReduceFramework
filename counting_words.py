from client import Client

import os
from get_ip_tools import get_ip

FILE_PATH = os.path.abspath(os.path.dirname(__file__))

s = {}
if __name__ == '__main__':
    c = Client(os.path.join(FILE_PATH, 'data.txt'),
               os.path.join(FILE_PATH, 'count_functions.py'),
               client_ip=get_ip(),
               workers=[('10.0.0.3','7998'),('10.0.0.4','7998'),('10.0.0.5','7998')])

    with open(os.path.join(FILE_PATH, 'data.txt')) as fl:
        while True:
            line = fl.readline()
            if line == '':
                break
            l = line.split()
            for word in l:
                try:
                    s[word] = s[word]+1
                except KeyError:
                    s[word] = 1
    res = c.execute()
    if len(s) != len(res):

        print("Malll: len de solution es : ",len(s),' y la solucion mia es: ',len(res))
    else:
        print('mismo len')
    for i in range(len(res)):
        x,y = res[i]
        if s[x] != y:
            print('el bateo en:',x)
    print('ver bateo si hay')



