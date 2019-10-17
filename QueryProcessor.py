from ROTree import *
import copy

class QueryProcessor:
    def __init__(self,repository):
        self.repository = repository

    def operate(self,roTree):
        orders = roTree.traverse_in_order()
        for order in orders:
            op = order.op
            self.evaluateOperator(op)

        return roTree.root.op.output.data
            
    def evaluateOperator(self,op):
        if isinstance(op,RESTProjectOp):
            self.operateRESTProject(op)
        elif isinstance(op,RESTProjectSelectOp):
            self.operateRESTProjectSelectOp(op)
        elif isinstance(op,ProjectOp):
            self.operateProject(op)
        elif isinstance(op,JoinOp):
            self.operateJoin(op)
        elif isinstance(op,CartesianOp):
            self.operateCartesian(op)
            
    def operateRESTProject(self,op):
        print("operation not supported")
        pass

    def operateRESTSelect(self,op):
        print("operation not supported")
        pass

    def operateRESTProjectSelectOp(self,op):
        connection = self.repository.getConnectionInstance()
        RESTCallResult = connection.getProjectSelectResult(op.target_columns,op.filters)

        #change project name to variable name
        projectCols = op.target_columns
        for col in projectCols:
            RESTCallResult[op.alias[col]] = RESTCallResult.pop(col)
        
        op.output.data = copy.deepcopy(RESTCallResult)

    def operateProject(self,op):
        repoResultDict = {}
        repoResultDict = copy.deepcopy(op.input.data)
        target_cols = op.target_columns

        dictProjectResult = {}

        for col in target_cols:
            if col in repoResultDict:
                dictProjectResult[col] = repoResultDict[col]

        op.output.data = copy.deepcopy(dictProjectResult)

    def operateJoin(self,op):
        left_table = op.input.data
        right_table = op.resultset2.data
        left_scm = list(left_table.keys())
        right_scm = list(right_table.keys())
        left_row_cnt = len(left_table[(left_scm[0])])
        right_row_cnt = len(right_table[(right_scm[0])])

        joinedSchema = list(set().union(left_scm,right_scm))

        print("opjoin ",joinedSchema)
        joinTargetCols = []
        for left_col in left_scm:
            for right_col in right_scm:
                if left_col == right_col:
                    joinTargetCols.append(left_col)

        joinedIndex = []

        for left_idx in range(left_row_cnt):
            for right_idx in range(right_row_cnt):
                isAllMatch = True
                for key in joinedSchema:
                    if left_table[key][left_idx] != right_table[key][right_idx]:
                        isAllMatch = False
                        break
                if isAllMatch == True:
                    joinedIndex.append([left_idx,right_idx])
        joinResultDict = {}
        for col in joinedSchema:
            joinResultDict[col] = []
            

        for idx in joinedIndex:
            for col in joinedSchema:
                if col in joinTargetCols:
                    joinResultDict[col].append(left_table[col][idx[0]])
                elif col in left_scm:
                    joinResultDict[col].append(left_table[col][idx[0]])
                elif col in right_scm:
                    joinResultDict[col].append(right_table[col][idx[1]])

        op.output.data = copy.deepcopy(joinResultDict)
        
    def operateCartesian(self,op):
        left_table = op.input.data
        right_table = op.resultset2.data
        left_scm = list(left_table.keys())
        right_scm = list(right_table.keys())

        left_row_cnt = len(left_table[left_scm[0]])
        right_row_cnt = len(right_table[right_scm[0]])

        joinedSchema = list(set().union(left_scm,right_scm))
        print("opCarte ",joinedSchema)
        joinResultDict = {}
        for col in joinedSchema:
            joinResultDict[col] = []

        for left_idx in range(left_row_cnt):
            for right_idx in range(right_row_cnt):
                for col in joinedSchema:
                    if col in left_scm:
                        joinResultDict[col].append(left_table[col][left_idx])
                    elif col in right_scm:
                        joinResultDict[col].append(right_table[col][right_idx])

        op.output.data = copy.deepcopy(joinResultDict)

