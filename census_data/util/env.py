import os
import socket

PROJECT_NAME = 'census'

# Check which machine we're on
HOST = socket.gethostname()
if HOST in ('sullivan-7d', 'sullivan-10d', 'ThinkPad-PC', 'DESKTOP-HOME'):
    data_root = "D:\\data"
else:
    data_root = "D:\\data"

DATA_PATH = os.path.join(data_root, PROJECT_NAME)
SRC_PATH = os.path.join(DATA_PATH, 'src')


def data_path(*args):
    return os.path.join(DATA_PATH, *args)


def src_path(*args):
    return os.path.join(SRC_PATH, *args)
