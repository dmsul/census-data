import os
import socket

# Check which machine we're on
HOST = socket.gethostname()
if HOST in ('sullivan-7d', 'sullivan-10d'):
    data_root = "D:\\data"
else:
    data_root = r'\\Sullivan-10d\data'

DATA_PATH = os.path.join(data_root, 'census')
SRC_PATH = os.path.join(DATA_PATH, 'src')


def data_path(*args):
    return os.path.join(DATA_PATH, *args)


def src_path(*args):
    return os.path.join(SRC_PATH, *args)
