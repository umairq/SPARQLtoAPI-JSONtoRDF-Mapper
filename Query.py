from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.parserutils import CompValue, Variable


class Node:
    def __init__(self,name,prefix,isVariable):
        self.name = name
        self.prefix = prefix
        self.isVariable = isVariable

        
    def __str__(self):
        ret_str = ''
        #if self.isVariable == True:
        #    ret_str += "?" 
        if self.prefix !=None:
            ret_str += self.prefix+":"
        ret_str += self.name
        return ret_str

class NodeBuilder:
    
    @staticmethod
    def parse_part(node_parsed):
        if 'PathAlternative' == node_parsed.name:
            return NodeBuilder.parse_part(node_parsed['part'][0])
        elif 'PathSequence' == node_parsed.name:
            return NodeBuilder.parse_part(node_parsed['part'][0])
        elif 'PathElt' == node_parsed.name:
            return NodeBuilder.parse_part(node_parsed['part'])
        elif 'vars' == node_parsed.name:
            return NodeBuilder.parse_variable(node_parsed['var'])
        elif 'pname' == node_parsed.name:
            return node_parsed['prefix'],node_parsed['localname'], False
        elif 'literal' == node_parsed.name:
            return None, node_parsed['string'], False
        else:
            print(node_parsed.name)

    @staticmethod
    def parse_variable(node_parsed):
        return None, str(node_parsed), True       

    @staticmethod
    def create(node_parsed):
        prefix = None
        name = None
        isVariable = False

        if isinstance(node_parsed,CompValue):
            prefix, name, isVariable = NodeBuilder.parse_part(node_parsed)
        elif isinstance(node_parsed,Variable):
            prefix, name, isVariable = NodeBuilder.parse_variable(node_parsed)
        else:
            return None

        return Node(name,prefix,isVariable)



class Triple:
    def __init__(self, triple_parsed):
        self.buildTriple(triple_parsed)

    def buildTriple(self,triple_parsed):
        subj = triple_parsed[0]
        pred = triple_parsed[1]
        obj = triple_parsed[2]

        self.subject = NodeBuilder.create(subj)
        self.predicate = NodeBuilder.create(pred)
        self.object = NodeBuilder.create(obj)

class Query:
    def __init__(self):
        self.triples = []
        self.projectionVars = []

class QueryBuilder:
    @staticmethod
    def create(query_str):
        q = Query()        
        parsed = parseQuery(query_str)

        projVars = parsed[1]['projection']
        for projVar in projVars:
            prefix, name, isVariable = NodeBuilder.parse_part(projVar)
            q.projectionVars.append(Node(name,prefix,isVariable))

        triples = parsed[1]['where']['part'][0]['triples']
        for triple_parsed in triples:
            triple = Triple(triple_parsed)
            q.triples.append(triple)
        return q

'''
if __name__ == '__main__':
    query = QueryBuilder.create('select ?s ?o where{?s dc:Title ?o}')
    for triple in query.triples:
        print(triple.subject)
        print(triple.predicate)
        print(triple.object)
'''

