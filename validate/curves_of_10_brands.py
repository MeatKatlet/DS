from elastic.elastic_queries_new_logic import Base_Elastic
import pandas as pd
import json
from  frame_partial_reader.frame_partial_reader import Frame_partial_reader
import datetime

# на вход top 10 брендов которыми торгуют у нас (выкинуть те бренды из списка которых у нас нет(посмотреть по классификатору + срезу)
#

#если полученный бренд со скобками то надо смотреть по срезу только те товарные группы в этом бренде которые есть сверху в группах(это то чем мы торгуем), только эти товарные группы мы и будем в спиосе

#на вход:
#бренд - товарная группа
#бренд - товарная группа

#если есть совпадение, то берем в анализ, если нет, то не берем

#извлекаем из запроса список брендов - групп - если есть совпадение с моим списком, то берем в анализ
#преобразовать timestamp - разделить на день/месяц/год и секунды отделить
#сгруппировать по дням, сделать count по дням - это и будет временной ряд




class Searches_db_worker(Base_Elastic):

    def __init__(self,index,timestampe_field_name):

        pr = Frame_partial_reader()
        pr.read("../searches/parts_with_sg_mg.csv")

        self.goods_classifier = pr.parse_result
        #with open('goods_classifier.json', 'w') as fp:
        #    json.dump(self.goods_classifier, fp)

        #
        self.list_of_our_brands_groups = {}
        self.create_our_brand_group_list()

        self.timestamp_for_query = timestampe_field_name


        self.brands_dict = list(pr.brands)
        self.groups_dict = list(pr.groups)
        self.not_in_q = 0
        self.from_timestamp = 10#1515903152# 14 января

        self.result_frame = pd.DataFrame(columns=["Brand","Day"])

    def create_our_brand_group_list(self):
        df = pd.read_excel('our_brands_groups.xlsx',header=None)
        rows = df.shape[0]
        for row in range(0,rows,1):
            line = df.iloc[row]

            if line[0] not in self.list_of_our_brands_groups:
                self.list_of_our_brands_groups[line[0]] = {}
            else:
                self.list_of_our_brands_groups[line[0]][line[1]] = 1




    def get_query(self,q,list_of_args=list()):

        list_of_args.append(str(self.from_timestamp))

        query = q(list_of_args)
        return query

    def do_logic(self, list_of_elements):
        length = len(list_of_elements)
        for i in range(0, length, 1):
            q = list_of_elements[i]["_source"]

            if q["search_query"] in self.goods_classifier:

                result = self.is_in_our_brands_list(q["search_query"])
                if result[0]:
                    brand = result[1]

                    readable = datetime.datetime.fromtimestamp(int(q["search_timestamp"])).isoformat()
                    day = readable.split("T")[0]

                    self.result_frame.loc[len(self.result_frame)] = [brand,day]


            else:
                self.not_in_q +=1

    def is_in_our_brands_list(self, search_query):

        for key in self.goods_classifier[search_query]:
            # (self.brands[brand], self.groups[gs])
            brand_key = key[0]
            group_key = key[1]
            brand_name = self.brands_dict[brand_key]
            group_name = self.groups_dict[group_key]

            if brand_name in self.list_of_our_brands_groups:
                if len(self.list_of_our_brands_groups[brand_name])==0:
                    return [True, brand_name]  # хотя бы один раз есть в списке наших брендов-групп
                elif group_name in self.list_of_our_brands_groups[brand_name]:
                    return [True,brand_name]#хотя бы один раз есть в списке наших брендов-групп

        return [False,-1]


    def query_for_searches(self,list_of_args):


        if len(list_of_args)==1:
            gt = list_of_args[0]
        else:
            gt = list_of_args[0]
        query = {
            "bool": {
                "must": [
                    {"range": {"search_timestamp": {"gt": gt}}},

                    {
                        "match": {
                            "search_city": "Новосибирск"
                        }
                    }


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



index = 'portal_search'
timestampe_field_name = "search_timestamp"
search_results = Searches_db_worker(index,timestampe_field_name)
q = search_results.query_for_searches
search_results.get_data(q, index, timestampe_field_name,size=500)

#todo экспортнем в ексель фрейм а там получим все что надо

writer = pd.ExcelWriter('top_10_brands_requests_timeseries1.xlsx')
search_results.result_frame.to_excel(writer, 'Sheet1')
writer.save()

df = pd.read_excel('top_10_brands_requests_timeseries1.xlsx')


r = df.groupby(["Brand","Day"]).size().reset_index(name='count')


writer = pd.ExcelWriter('top_10_brands_requests_timeseries2.xlsx')
r.to_excel(writer, 'Sheet1')

writer.save()



