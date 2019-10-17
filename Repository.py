from ROTree import *
from APICall import APICall

class ConnectionWrapper:
    def makeConnection(self):
        pass
    def getProjectResult(self,projectInfo):
        pass
    def getSelectResult(self,selectInfo):
        pass
    def getProjectSelectResult(self,projectInfo,selectInfo):
        pass

class Repository:
    def __init__(self,mapping_file_directory):
        self.m_f_directory = mapping_file_directory
        self.mapping_table = {}

        self.loadRepositoryData()

    def loadRepositoryData(self):
        map_fd = open(self.m_f_directory,'r')

        lines = map_fd.readlines()
        mapping_table = {}
        for line in lines:
            line = line.replace('\n','')
            tmps = line.split(' ')
            key = tmps[0]
            mapping_table[key] = SOClass(tmps[1],tmps[2])

        self.mapping_table = mapping_table

        map_fd.close()

    def getConnectionInstance(self):
        pass
    
class SOClass:
    def __init__(self,subject_column, object_column):
        self.subject_column = subject_column
        self.object_column = object_column
    def __str__():
        return self.subject_column + "-" +self.object_column

