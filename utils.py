import os

class MyLogger(object):
    def debug(self, msg):
        print(msg)
        pass

    def warning(self, msg):
        print(msg)
        pass

    def error(self, msg):
        print(msg)
        
def file_exists(filename):
    return os.path.exists(filename)    
