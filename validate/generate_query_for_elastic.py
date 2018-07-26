import pandas as pd
import json
import requests
#загрузить список search_uid удачных поисков

#

main_frame = pd.read_csv('out.csv')

s = main_frame.query("Search_result == 1")




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
                },
                {
                    "terms" : {
                        "search_uid" : list(s["Search_uid"].unique())
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


