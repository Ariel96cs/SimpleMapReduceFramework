import os

FILE_PATH = os.path.abspath(os.path.dirname(__file__))

s = set()

with open(os.path.join(FILE_PATH, 'data.txt')) as fl:
    while True:
        line = fl.readline()
        if line == '':
            break
        l = line.split()
        for word in l:
            s.add(word)

print(s)
print(len(s))
