import os
import re

FILE_PATH = os.path.abspath(os.path.dirname(__file__))

content = ''

with open(os.path.join(FILE_PATH, 'data.txt')) as fl:
    content = ''.join(fl.readlines())

s = set(re.findall(r'\w+', content))

print(s)
print(len(s))
