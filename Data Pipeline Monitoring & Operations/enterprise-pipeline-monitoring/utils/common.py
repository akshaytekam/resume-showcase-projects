import os
from datetime import datetime


def create_folder(path):

    if not os.path.exists(path):

        os.makedirs(path)


def current_timestamp():

    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
