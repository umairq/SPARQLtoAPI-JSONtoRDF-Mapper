import json
import requests

import http.client, urllib.parse



class APICall:

    def __init__(self):
        self.s = requests.Session()
        self.conn = http.client.HTTPConnection('api.crossref.org',timeout=1000)
        self.conn.connect()
        #response = self.conn.getresponse()

    def evaluate(self,params):
        headers = {'Content-type': 'application/json'} 
        print(params)
        try:
            self.conn.request("GET","/works?"+params,headers=headers)
            response = self.conn.getresponse()
            data = json.loads(response.read().decode())
            #resp = self.s.get(url=url, headers=headers,stream=True)
            #print(resp.text)
            #data = json.loads(resp.text,strict=False)
            return data
        except e:
            return None

