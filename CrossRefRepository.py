from Repository import *
from crossref.restful import Works

class CrossRefConnectionWrapper(ConnectionWrapper):
    def makeConnection(self):
        pass
    def getProjectResult(self,projectInfo):
        #to do
        pass
    def getSelectResult(self,selectInfo):
        #to do
        pass
    def getProjectSelectResult(self,projectInfo,selectInfo):
        #mapping for Project target name to Select target name
        if 'DOI' in selectInfo:
            selectInfo['doi'] = selectInfo.pop('DOI')
     
        results = Works().filter(**selectInfo).select(projectInfo)

        dictResultTable = {}

        for col in projectInfo:
            dictResultTable[col] = []
      
        for result in results:
            isAllExist = True
            for col in projectInfo:
                if not col in result:
                    isAllExist = False

            if isAllExist is True:
                for col in projectInfo:
                    dictResultTable[col].append(result[col])
            else:
                pass
        
        return dictResultTable

class CrossRefRepository(Repository):
    def __init__(self,mapping_file_directory):
        super(CrossRefRepository, self).__init__(mapping_file_directory)
    
    def getConnectionInstance(self):
        return CrossRefConnectionWrapper()

