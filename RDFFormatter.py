import json
from xml.etree import ElementTree

class RDFFormatter:
    @staticmethod
    def format(resultSet):
        sparql = ElementTree.Element("sparql")
        sparql.attrib["xmlns"] = "http://www.w3.org/2005/sparql-results#"
        sparql.attrib["xmlns:xsi"] = "http://www.w3.org/2001/XMLSchema-instance"
        sparql.attrib["xsi:schemaLocation"] = "http://www.w3.org/2001/sw/DataAccess/rf1/result2.xsd"

        head = ElementTree.Element("head")

        resultSetKey = resultSet.keys()
        rec_cnt = 0
        for key in resultSetKey:
            variable = ElementTree.Element("variable")
            variable.attrib["name"] = key
            head.append(variable)
            rec_cnt = len(resultSet[key])

        results = ElementTree.Element("results")
        results.attrib["distinct"] = "false"
        results.attrib["ordered"] = "false"

        for i in range(rec_cnt):
            result = ElementTree.Element("result")
            for key in resultSetKey:
                binding = ElementTree.Element("binding")
                binding.attrib["name"] = key
                uri = ElementTree.Element("uri")
                uri.text = str(resultSet[key][i])
                binding.append(uri)
                result.append(binding)
            results.append(result)

        sparql.append(head)
        sparql.append(results)
        return sparql

