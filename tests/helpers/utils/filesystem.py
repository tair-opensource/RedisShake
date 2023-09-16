import os
import shutil


def file_size(file):
    return os.stat(file).st_size


def file_truncate(file, size):
    with open(file, "rb+") as f:
        content = f.read()
        f.truncate(size)
        return content[size:]


def file_append(file, content):
    with open(file, "ab") as f:
        f.write(content)


def create_empty_dir(path):
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path)

