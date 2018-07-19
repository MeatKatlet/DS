import json
#from pprint import pprint
from pandas.io.json import json_normalize
import pandas as pd
#import matplotlib.pyplot as plt


#import sklearn

import numpy as np
import requests
import warnings
import time
warnings.filterwarnings('ignore')

"""
запись json
my_list = [ 'a', 'b', 'c']
my_json_string = json.dumps(my_list)
with open('data.json', 'w') as outfile:
    json.dump(my_json_string, outfile)
    
чтение json

with open('data.json') as f:
    data = json.load(f)
json.loads(data)
#получаем list
"""

#def main():
    #получим список и далее будем проверять на присутствие search_uid в нем из дальнейшего запроса
    #roesportal.rossko.local/cart_item_event-10.07.2018/_search

class Base_Elastic():

    def __init__(self):
        self.df = pd.DataFrame()
        # self.query = ""
        self.f = open('result.json', 'a')

    def get_data(self, q, index, timestampe_field_name,size=100):
        query = q()
        body = {
            "size": size,
            "query": query,
            "sort": [{timestampe_field_name: {"order": "asc"}}]}

        headers = {'Accept': 'application/json', 'Content-type': 'application/json'}
        elastic_url = 'http://roesportal.rossko.local:80/' + index + '-*/_search/?size='+str(size)+'&pretty'
        query = json.dumps(body)


        while True:
            response = requests.post(elastic_url, data=query, verify=False, headers=headers)
            if (response.text.find("502 Bad Gateway") > 0):
                print("Bad Gateway... REPEAT")
                continue
            else:

                break

        deserilised = json.loads(response.text)

        total_size = deserilised["hits"]["total"]

        # response2 = json_normalize(json.loads(response.text))
        # total_size = int(response2['hits.total'])
        scroll_size = total_size - size

        if total_size <= size:
            # запись в файл json - сделать функцию для этого!
            self.do_logic_short(deserilised["hits"]["hits"])
            return

        self.do_logic(deserilised["hits"]["hits"])

        # self.df = pd.DataFrame(json_normalize(response2['hits.hits'][0]))

        # tsmp = df["_source.search_timestamp"].max()
        l = len(deserilised["hits"]["hits"])
        #last_tsmp = deserilised["hits"]["hits"][size-1]["_source"]["timestamp"]  # поскольку они отсортированы то последний это максимальный timestamp должен быть
        last_tsmp = deserilised["hits"]["hits"][l - 1]["_source"]["timestamp"]



        self.json_begin_file()

        while True:

            query = q(str(last_tsmp))

            body = {
                "size": size,
                "query": query,
                "sort": [{timestampe_field_name: {"order": "asc"}}]}

            query = json.dumps(body)

            response = requests.post(elastic_url, data=query, verify=False, headers=headers)
            #time.sleep(5)
            if (response.text.find("502 Bad Gateway") > 0):
                print("Bad Gateway... REPEAT")
                continue

            deserilised = json.loads(response.text)
            #time.sleep(5)
            self.do_logic(deserilised["hits"]["hits"])

            # tsmp = df["_source.search_timestamp"].max()

            l = len(deserilised["hits"]["hits"])

            # last_tsmp = response['_source.'+timestampe_field_name]
            last_tsmp = deserilised["hits"]["hits"][l - 1]["_source"]["timestamp"]
            scroll_size = scroll_size - size
            # print(str(((int(total_size) - int(scroll_size)) / int(total_size)) * 100) + "%")
            if (int(scroll_size) <= 0):
                break


        self.json_end_file()

    def do_logic_short(self, list_of_elements):
        self.f.write("[")
        self.save_json_to_file(list_of_elements)
        self.f.write("{}")
        self.f.write("]")
        self.f.close()

    def do_logic(self, list_of_elements):

        self.save_json_to_file(list_of_elements)

    def save_json_to_file(self, list_of_elems):

        length = len(list_of_elems)-1
        for el in range(0, length, 1):
            my_json_string = json.dumps(list_of_elems[el]["_source"])
            self.f.write(my_json_string)
            self.f.write(",")

    def json_begin_file(self):
        self.f.write("[")

    def json_end_file(self):
        self.f.write("{}")
        self.f.write("]")
        self.f.close()

class Searches_in_input_field_event(Base_Elastic):
    def __init__(self):
        self.list_of_search_uids = dict()

    def do_logic(self, list_of_elements):
        # todo сохраняем в список, все search_uid Для дальнейшего доступа и проверки на присутствие
        l = len(list_of_elements)-1
        for i in range(0, l, 1):
            if "search_uid" in list_of_elements[i]["_source"]:
                self.list_of_search_uids[list_of_elements[i]["_source"]["search_uid"]] = 0

    def do_logic_short(self, list_of_elements):
        self.do_logic(list_of_elements)

    def json_begin_file(self):
        return

    def json_end_file(self):
        return

class Search_results_events(Base_Elastic):
    def __init__(self):
        #self.list_of_search_uids = list_of_search_uids

        # создаем df в котором будут в каждой строке инфа по поиску запчасти, колонки искомый артикул?, результат поиска(1/0, удачный/неудачный), бренд, регион, товарная группа,
        self.all_searches = pd.DataFrame(columns=['Search_query', 'Search_result', 'Brand', 'Region', 'Group'])

        self.with_tcp = False  # брать в расчет или нет товары строннних поставщиков(заменители) (is_approximate_delivery_interval?) это для задачи 2, для задачи 1 их в расчет не брать
        # кроссы не буду брать в расчет пока
        self.with_crosses = True

        #тсп/не тсп(одна партерра пока видимо)
        #локальный склад/не локальный
        #есть кросс или нет на искомый товар
        #регион локального склада
        #регион не локального склада




    def do_logic(self, list_of_elements):
        # list_of_elements - это список объектов из объекта hits в результате выдачи
        # здесь будем фильтровать нужные мне результаты выдачи и сохранять во фрейм
        length = len(list_of_elements)-1
        for i in range(0, length, 1):

            hit = list_of_elements[i]["_source"]

            if "region" in hit:
                #if hit["search_uid"] in self.list_of_search_uids:

                length2 = len(hit["results_groups"]) - 1

                for k in range(0, length2, 1):#по всем элементам results_groups, в каждом элементе один атрибут search_results
                    results_group_item = hit["results_groups"][k]

                    length3 = len(results_group_item["search_results"]) - 1

                    for j in range(0, length3, 1):
                        element_of_search_results = results_group_item["search_results"][j]

                        search_query = hit["search_query"]
                        brand = element_of_search_results["brand_name"]
                        region = hit["region"]
                        group = element_of_search_results["market_group_name"]

                        # отсутствующая в наличии запчасть это неудачный поиск!
                        if "part" in element_of_search_results:
                            search_result = 1  # удачный поиск
                            cross = "cross" in element_of_search_results

                            if element_of_search_results["part"]["is_local_delivery"] == True:  # только с локального склада

                                if element_of_search_results["part"]["is_approximate_delivery_interval"] == False and self.with_tcp == False:  # не ТСП и ТСП не заказывали для запроса
                                    # все кроме ТСП, свои товары

                                    #все подряд с кроссами и без запишем
                                    #if cross == True and self.with_crosses == True:  # кроссы есть и мы их заказывали, тогда считаем
                                        self.all_searches.loc[len(self.all_searches)] = [search_query, search_result, brand, region, group]  # товар с кроссами
                                    #elif cross == False:  # кроссов нет
                                        #self.finded.loc[len(self.finded)] = [search_query, search_result, brand,region, group]  # товар без кроссов

                                elif self.with_tcp == True:  # заказали и те и те (задача 2)

                                    #if cross == True and self.with_crosses == True:  # кроссы есть и мы их заказывали, тогда считаем
                                        self.all_searches.loc[len(self.all_searches)] = [search_query, search_result, brand, region, group]  # товар с кроссами
                                    #elif cross == False and self.with_crosses == False:  # кроссов нет и мы их не заказывали!, тогда считаем
                                        #self.finded.loc[len(self.finded)] = [search_query, search_result, brand,region, group]  # товар без кроссов
                            else:
                                search_result = 0  # не удачный поиск
                                self.all_searches.loc[len(self.all_searches)] = [search_query, search_result, brand, region, group]


                        else:
                            search_result = 0  # не удачный поиск
                            self.all_searches.loc[len(self.all_searches)] = [search_query, search_result, brand, region, group]


    def do_logic_short(self, list_of_elements):
        self.do_logic(list_of_elements)

    def json_begin_file(self):
        return

    def json_end_file(self):
        return


def query_make(gt=10):
    query = {
        "bool": {
            "must": [
                {"range": {"timestamp": {"gt": gt}}},
                {
                    "match": {
                        "event": "past_product_article"
                    }
                },
                {
                    "match": {
                        "is_internal_user": False
                    }
                }
            ]
        }
    }
    return query


def query_make2(gt=10):
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
                        "event": "search"
                    }
                },
                {
                    "exists": {"field": "results_groups.search_results.part"}
                }
            ]
        }
    }

    return query


def run_logic(from_db=True):
    #todo надо будет делать чтобы уже сохраненные данные в файле не запрашивались, видимо time stamp, надо сохранить timestamp последнего документа, или вообще хранить их в отдельной колонке, поскольку временные ряды пригодятся!
    if from_db:
        index = 'cart_item_event'
        timestampe_field_name = "timestamp"

        """
               # 0. пробуем базовый Elastic
                base_elastic = Base_Elastic()
                    # todo проверить как считывается записанный json!
                    # todo сделать пути к фалу аргументом чтобы не затирался!
                base_elastic.get_data(q, index, timestampe_field_name)

            """
        # 1. запрос
        # searches_input_field = Searches_in_input_field_event()
        # searches_input_field.get_data(q, index, timestampe_field_name)
        #q = query_make
        search_results = Search_results_events()#searches_input_field.list_of_search_uids передавать когда понадобится фильтрация по мобытию вставки номера детали в строку поиска
        q = query_make2
        search_results.get_data(q, index, timestampe_field_name)

        # search_results.finded - фрейм с результатами поиска с брендами, регионами, товарными группами
        all_searches = search_results.all_searches



        all_searches.to_csv('out.csv')

    else:

        all_searches = pd.read_csv('out.csv')


    df_sucsess_searches = all_searches.query("Search_result == 1")

    return [df_sucsess_searches, all_searches]



def calc_percentage(search_results,df_sucsess_searches, f):

    all_searches,sucsess_searches = f(search_results, df_sucsess_searches)


    percentage_of_sucsess = pd.DataFrame(index = all_searches.index, columns=all_searches.columns.values)
    #percentage_of_sucsess = pd.DataFrame()

    s = all_searches.shape
    for i in range(0, s[0], 1):  # по строкам
        row = []
        row_name = all_searches.index[i]
        for j in range(0, s[1], 1):  # по столбцам

            col_name = all_searches.columns[j]

            if row_name in sucsess_searches.index and col_name in sucsess_searches.columns:
                if all_searches.iloc[i][j]==0:
                    percentage_in_cell = float(0)
                else:
                    percentage_in_cell = float((sucsess_searches.loc[row_name][col_name] * 100) / all_searches.iloc[i][j])
            #sucsess_searches.loc["Братск"]["ERA"]
            else:
                percentage_in_cell = float(0)


            row.append(percentage_in_cell)

        percentage_of_sucsess.iloc[i] = row

    return percentage_of_sucsess


def region_brand(search_results,df_sucsess_searches):

    all_searches = pd.crosstab(search_results.Region, search_results.Brand)  # таблица по всем поискам
    sucsess_searches = pd.crosstab(df_sucsess_searches.Region, df_sucsess_searches.Brand)  # таблица по удачным поискам

    return [all_searches, sucsess_searches]

def region_group(search_results,df_sucsess_searches):

    all_searches = pd.crosstab(search_results.Region, search_results.Group)  # таблица по всем поискам
    sucsess_searches = pd.crosstab(df_sucsess_searches.Region, df_sucsess_searches.Group)  # таблица по удачным поискам

    return [all_searches, sucsess_searches]


def brand_group(search_results,df_sucsess_searches):

    all_searches = pd.crosstab(search_results.Brand, search_results.Group)  # таблица по всем поискам
    sucsess_searches = pd.crosstab(df_sucsess_searches.Brand, df_sucsess_searches.Group)  # таблица по удачным поискам

    return [all_searches, sucsess_searches]




#---------------------------------------

"""
"4b99e04d6bfc52e4290d48ade6fdeff0" - в нем есть part
"""

#if __name__== "__main__":
  #main()
