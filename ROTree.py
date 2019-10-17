from itertools import combinations


class ResultSet:
    def __init__(self):
        # key : attribute name
        # value : [data]
        self.data = {}

class OpNode:
    def __init__(self,op):
        self.op = op
        self.children = []

class Operator:
    def __init__(self,input_data, output_data, schema):
        self.input = input_data
        self.output = output_data
        self.schema = schema

class RESTProjectSelectOp(Operator):
    def __init__(self,repository, target_columns, filters, alias, schema):
        super(RESTProjectSelectOp, self).__init__(repository,ResultSet(), schema)
        self.target_columns = target_columns
        self.filters = filters
        self.alias = alias

class RESTProjectOp(Operator):
    def __init__(self,repository,target_columns, schema):
        super(RESTProjectOp, self).__init__(repository,ResultSet(),schema)
        self.target_columns = target_columns

class ProjectOp(Operator):
    def __init__(self,resultset, target_columns, schema):
        super(ProjectOp, self).__init__(resultset,ResultSet(),schema)
        self.target_columns = target_columns

class JoinOp(Operator):
    def __init__(self, resultset, resultset2, schema):
        super(JoinOp, self).__init__(resultset,ResultSet(),schema)
        self.resultset2 = resultset2

class CartesianOp(Operator):
    def __init__(self, resultset, resultset2, schema):
        super(CartesianOp, self).__init__(resultset,ResultSet(),schema)
        self.resultset2 = resultset2

class ROTree:
    def __init__(self):
        self.root = None

    def traverse_in_order_intern(self, node, orders):

        for child in node.children:
            self.traverse_in_order_intern(child,orders)
            orders.append(child)

    def traverse_in_order(self):
        orders = []

        self.traverse_in_order_intern(self.root, orders)
        orders.append(self.root)
        
        return orders

class Rule:
    def __init__(self):
        pass

    def buildOp(self,repository,triple):
        pass

#v v v
class Rule1(Rule):
    def __init__(self):
        pass
#v v l, l v l
class Rule2(Rule):
    def __init__(self):
        pass
#v l v, l l v, v l l
class Rule3(Rule):
    def __init__(self):
        pass
    def buildOp(self,repository,triple):
        soclass = repository.mapping_table[str(triple.predicate)]
        filters = {}
        alias = {}
        target_columns = []

        if triple.subject.isVariable == False:
            filters[soclass.subject_column] = str(triple.subject)
        else :
            target_columns.append(soclass.subject_column)
            alias[soclass.subject_column] = str(triple.subject)

        if triple.object.isVariable == False:
            filters[soclass.object_column] = str(triple.object)
        else :
            target_columns.append(soclass.object_column)
            alias[soclass.object_column] = str(triple.object)
        #proj_op = RESTProjectOp(repository, target_columns)

        schema = []
        for col in target_columns:
            schema.append(alias[col])

        proj_sel_op = RESTProjectSelectOp(repository, target_columns, filters, alias, schema)
        return proj_sel_op

#l l 
class Rule4(Rule):
    def __init__(self):
        pass

class RuleCoordinator:
    def determineRule(self,triple):
        if(not triple.predicate.isVariable and not triple.object.isVariable):
            return Rule3()
        else:
            print("Not supported triple type!, response with http 500")
            return None
        '''
        if(triple.predicate.isVariable and not triple.object.isVariable):
            #do exception
            sys.exit()
            #return Rule1()
        elif(triple.predicate.isVariable and not triple.object.isVariable):
            #do exception
            pass
            #return Rule2()
        elif(not triple.predicate.isVariable and not triple.object.isVariable):
            return Rule3()
        elif(not triple.predicate.isVariable and not triple.object.isVariable):
            #do exception
            pass
            #return Rule4()
        else:
            return None
        '''

def match_columns(left_op, right_op):
    sch_left = left_op.schema
    sch_right = right_op.schema
    has_match = False

    matched_cols = []

    for col_left in sch_left:
        for col_right in sch_right:
            if col_left == col_right:
                has_match = True
                break
        if has_match == True:
            break
    matched_cols = list(set().union(sch_left, sch_right))

    return has_match, matched_cols

class ROTreeBuilder:
    @staticmethod
    def create(repository,query):
        tree = ROTree()
        rc = RuleCoordinator()
        if len(query.triples) == 1:
            triple = query.triples[0]
            rule = rc.determineRule(triple)
            if rule == None:
                return None        
            node = OpNode(rule.buildOp(repository,triple))

            cols = []
            for projVar in query.projectionVars:
                cols.append(str(projVar.name))

            tree.root = OpNode(ProjectOp(node.op.output, cols, cols))
            tree.root.children.append(node)                        
        else:
            #make leaves
            leaves = []
            for triple in query.triples:
                rule = rc.determineRule(triple)
                if rule == None:
                    return None
                leaves.append(OpNode(rule.buildOp(repository, triple)))

            while(len(leaves) != 1):
                preCnt = len(leaves)
                
                idx_list = range((len(leaves)))
                comb = combinations(idx_list,2)
                has_match = False
                for idx_tuple in list(comb):
                    left_op = leaves[idx_tuple[0]].op
                    right_op = leaves[idx_tuple[1]].op

                    has_match, matched_cols = match_columns(left_op,right_op)

                    if has_match == True:
                        joinedSchema = list(set().union(left_op.schema,right_op.schema))
                        newJoinOp = OpNode(JoinOp(leaves[idx_tuple[0]].op.output, leaves[idx_tuple[1]].op.output, matched_cols))
                        newJoinOp.children.append(leaves[idx_tuple[0]])
                        newJoinOp.children.append(leaves[idx_tuple[1]])
                        
                        del leaves[idx_tuple[0]]
                        del leaves[idx_tuple[1]-1]

                        leaves.append(newJoinOp)
                        break

                if has_match == False:
                    break

                postCnt = len(leaves)
                if preCnt == postCnt:
                    break
                       
            topNode = leaves[0]
            #cartesian
            for i in range(len(leaves)-1):
                sch_left = topNode.op.schema
                sch_right = leaves[i+1].op.schema
                joinedSchema = list(set().union(sch_left, sch_right))
                newNode = OpNode(CartesianOp(topNode.op.output, leaves[i+1].op.output, joinedSchema))
                newNode.children.append(topNode)
                newNode.children.append(leaves[i+1])
                topNode = newNode

            cols = []
            for projVar in query.projectionVars:
                cols.append(str(projVar.name))

            tree.root = OpNode(ProjectOp(topNode.op.output, cols, cols))
            tree.root.children.append(topNode)

        return tree

