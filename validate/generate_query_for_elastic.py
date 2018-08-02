import pandas as pd
import json
import requests
from elastic.elastic_queries_new_logic import Base_Elastic
#загрузить список search_uid удачных поисков

#

#main_frame = pd.read_csv('out.csv')
def query_make4(list_of_args):
    if len(list_of_args)==1:
        gt = list_of_args[0]  # базовый timestamp будет добавлять
        range = {"timestamp": {"gt": gt}}
    else:
        gt = list_of_args[0]#это будет когда уже в цикле идет запрос
        range = {"timestamp": {"gt": gt}}

    query = {
         "bool": {
             "must": [
                 {
                     "range": range
                 },

                 {"range": {"timestamp": {"lte": 1533081600}}},
                 {
                     "match": {
                        "event": "checkout"
                     }
                 },
                 {
                     "match": {
                         "region": "Новосибирск"
                     }
                 }
             ]
         }
    }
    return query

#s = main_frame.query("Search_result == 1")
class Sales_db_worker(Base_Elastic):

    def __init__(self,t):
        self.from_timestamp = t
        self.list_of_sales_ids={}
        self.list_of_sales_ids2={}
        self.u = 0
        self.u2 = 0

    def get_query(self,q,list_of_args=list()):

        list_of_args.append(str(self.from_timestamp))

        query = q(list_of_args)
        return query

    def do_logic(self, list_of_elements):
        length = len(list_of_elements)
        for i in range(0, length, 1):
            sale = list_of_elements[i]

            if sale["_source"]["search_uid"] not in self.list_of_sales_ids:
                self.list_of_sales_ids[sale["_source"]["search_uid"]]=1
                self.u = 0
            else:
                self.list_of_sales_ids2[sale["_source"]["search_uid"]] = 1#дубликат

    def do_logic_short(self, list_of_elements):
        self.do_logic(list_of_elements)

    def json_begin_file(self):
        return

    def json_end_file(self):
        return


s = Sales_db_worker(1530403200)
q = query_make4
index = 'cart_item_event'
timestampe_field_name = "timestamp"
s.get_data(q, index, timestampe_field_name)


exit(0)
query = {
       "bool": {
              "must": [

                {
                    "match": {
                        "is_internal_user": False
                    }
                },
                {
                    "match": {
                        "event": "checkout"
                    }
                },
                {
                     "match": {
                          "region": "Новосибирск"
                     }
                }



              ]

       }
    }

body = {
    "size": 10,
    "query": query

}

headers = {'Accept': 'application/json', 'Content-type': 'application/json'}
elastic_url = 'http://roesportal.rossko.local:80/cart_item_event-*/_search/?size=10&pretty'
query = json.dumps(body)

while True:
    response = requests.post(elastic_url, data=query, verify=False, headers=headers)
    if (response.status_code == 502):  # , response.text.find("502 Bad Gateway") > 0
        print("Bad Gateway... REPEAT")
        continue
    else:

        break


