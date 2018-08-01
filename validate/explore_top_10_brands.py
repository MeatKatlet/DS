from elastic.elastic_queries_new_logic import Base_Elastic
import pandas as pd
import json
from  frame_partial_reader.frame_partial_reader import Frame_partial_reader



class Searches_db_worker(Base_Elastic):

    def __init__(self):

        pr = Frame_partial_reader()
        pr.read("../searches/parts_with_sg_mg.csv")

        self.goods_classifier = pr.parse_result
        #with open('goods_classifier.json', 'w') as fp:
        #    json.dump(self.goods_classifier, fp)

        self.brands_dict = list(pr.brands)
        self.groups_dict = pr.groups
        self.not_in_q = 0
        self.from_timestamp = 1515903152

        self.result = {}


    def get_query(self,q,list_of_args=list()):

        list_of_args.append(str(self.from_timestamp))

        query = q(list_of_args)
        return query

    def do_logic(self, list_of_elements):
        length = len(list_of_elements)
        for i in range(0, length, 1):
            q = list_of_elements[i]["_source"]

            if q["search_query"] in self.goods_classifier:
                #l = len(self.goods_classifier[q["search_query"]])
                brands = {}
                for key in self.goods_classifier[q["search_query"]]:
                    #(self.brands[brand], self.groups[gs])
                    brand_key = key[0]
                    group_key = key[1]
                    brand_name = self.brands_dict[brand_key]
                    if brand_name not in brands:
                        brands[brand_name] = 1
                    else:
                        #это не наш случай - брендов >2
                        break

                if len(brands)==1:
                    brand_name = list(brands.keys())[0]
                    if brand_name not in self.result:
                        self.result[brand_name] = 1
                    else:
                        self.result[brand_name] +=1#к поиску этого бренда

            else:
                self.not_in_q +=1


    def query_for_searches(self,list_of_args):


        if len(list_of_args)==1:
            gt = list_of_args[0]
        else:
            gt = list_of_args[0]
        query = {
            "bool": {
                "must": [
                    {"range": {"timestamp": {"gt": gt}}},
                    {
                        "match": {
                            "is_internal_user": False
                        }
                    },
                    {
                        "match": {
                            "event": "autocomplete_select"
                        }
                    },



                ]

            }
        }

        return query


    def do_logic_short(self, list_of_elements):
        self.do_logic(list_of_elements)

    def json_begin_file(self):
        return

    def json_end_file(self):
        return



index = 'cart_item_event'
timestampe_field_name = "timestamp"
search_results = Searches_db_worker()
q = search_results.query_for_searches
search_results.get_data(q, index, timestampe_field_name,size=1000)



sorted_by_value_key_value_pairs = sorted(search_results.result.items(), key=lambda x:x[1], reverse=True)

#получаем 10 топ брендов


