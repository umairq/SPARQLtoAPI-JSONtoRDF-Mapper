from EndpointDriver import *
from CrossRefRepository import * 

if __name__ == "__main__":
    ed = EndpointDriver('CrossRef', 8000, CrossRefRepository('map_file.txt'))
    ed.runEndpoint()
