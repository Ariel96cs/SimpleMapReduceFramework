from client import Client

import os

FILE_PATH = os.path.abspath(os.path.dirname(__file__))


if __name__ == '__main__':
    c = Client(os.path.join(FILE_PATH, 'data.txt'),
               os.path.join(FILE_PATH, 'functions.py'),register_addr=('0.0.0.0','9091'))

    res = c.execute()
    print("res", len(res))
