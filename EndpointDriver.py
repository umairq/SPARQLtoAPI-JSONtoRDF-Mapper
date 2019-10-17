from flask import Flask, Response, jsonify
from flask import request
from flask_cors import CORS

from subprocess import Popen, PIPE
from xml.etree import ElementTree

from QueryProcessor import QueryProcessor
from Query import QueryBuilder
from ROTree import ROTreeBuilder
from RDFFormatter import RDFFormatter

import base64
import struct
import os
import json

app = Flask(__name__)
CORS(app)

def genVirtualResult(self):
    sparql = ElementTree.Element("sparql")
    sparql.attrib["xmlns"] = "http://www.w3.org/2005/sparql-results#"
    sparql.attrib["xmlns:xsi"] = "http://www.w3.org/2001/XMLSchema-instance"
    sparql.attrib["xsi:schemaLocation"] = "http://www.w3.org/2001/sw/DataAccess/rf1/result2.xsd"

    head = ElementTree.Element("head")
    s_variable = ElementTree.Element("variable")
    s_variable.attrib["name"] = "s"
    p_variable = ElementTree.Element("variable")
    p_variable.attrib["name"] = "p"

    o_variable = ElementTree.Element("variable")
    o_variable.attrib["name"] = "o"

    head.append(s_variable)
    head.append(p_variable)
    head.append(o_variable)
    
    results = ElementTree.Element("results")
    results.attrib["distinct"] = "false"
    results.attrib["ordered"] = "true"

    result = ElementTree.Element("result")

    s_binding = ElementTree.Element("binding")
    s_binding.attrib["name"] = "s"
    s_uri = ElementTree.Element("uri")
    s_uri.text ="http://abc"
    s_binding.append(s_uri)

    p_binding = ElementTree.Element("binding")
    p_binding.attrib["name"] = "p"
    p_uri = ElementTree.Element("uri")
    p_uri.text = "http://abc"

    p_binding.append(p_uri)

    o_binding = ElementTree.Element("binding")
    o_binding.attrib["name"] = "o"
    o_uri = ElementTree.Element("uri")
    o_uri.text = "http://abc"
    o_binding.append(o_uri)

    result.append(s_binding)
    result.append(p_binding)
    result.append(o_binding)

    results.append(result)

    sparql.append(head)
    sparql.append(results)
    return sparql

repository = None

@app.route("/sparql", methods=['GET'])
def runQuery():
    query_str = request.args.get('query')
    
    rdf = None 
    query = None
    roTree = None
    processor = None
    rs = None

    if 'LIMIT 1' in query_str:
        rdf = genVirtualResult()
        response = app.response_class(
            response=ElementTree.tostring(rdf).decode(),
            mimetype='application/sparql-results+xml'
            )
        return response
    else :
        query = QueryBuilder.create(query_str)
        roTree = ROTreeBuilder.create(repository, query)
        if roTree == None:
            return ('',500)

        processor = QueryProcessor(repository)
        rs = processor.operate(roTree)
        rdf = RDFFormatter.format(rs)
        #elif dataSourceName == "others"
                    
    resultCnt = len(rs[list(rs.keys())[0]])
    print("resultCnt:", resultCnt)
    print(ElementTree.tostring(rdf).decode())
    print("done")

    response = app.response_class(
            response=ElementTree.tostring(rdf).decode(),
            mimetype='application/sparql-results+xml'
            )

    return response

    #return run_query(query)

class EndpointDriver:
    def __init__(self, name, portNumber, _repository):
        global repository
        self.endpointName = name
        self.portNumber = portNumber
        repository = _repository

    def runEndpoint(self):
        appParams = {}
        appParams['host'] = "0.0.0.0"
        appParams['port'] = self.portNumber
        app.run(**appParams)
        print("endpoint started")

