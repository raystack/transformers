import os


class FileSystem:
    def read(self, file):
        if file is not None:
            with open(file, 'r') as f:
                return f.read()

    def exist(self,path):
        return os.path.exists(path)

    def basename(self,path):
        return os.path.basename(path)