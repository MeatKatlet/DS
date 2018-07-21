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

    def get_query(self,q,list_of_args=list()):
        query = q(list_of_args)
        return query

    def get_data(self, q, index, timestampe_field_name,size=100):
        #query = q()
        query = self.get_query(q)
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

            #query = q(str(last_tsmp))
            query = self.get_query(q, list(str(last_tsmp)))

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
        self.all_searches = pd.DataFrame(columns=['Search_uid','Timestamp', 'Search_query', 'Brand', 'Region', 'Group'])
        self.searches_not_standart = pd.DataFrame(columns=['Timestamp', 'Brand', 'Group'])#статистика по нестандартным выдачам

        self.with_tcp = False  # брать в расчет или нет товары строннних поставщиков(заменители) (is_approximate_delivery_interval?) это для задачи 2, для задачи 1 их в расчет не брать
        # кроссы не буду брать в расчет пока
        self.with_crosses = True

        self.not_succsess = 0
        #self.count = 0
        #self.count2 = 0
        #self.count3 = 0

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
            #self.count += 1
            hit = list_of_elements[i]["_source"]
            timestamp = hit["timestamp"]
            search_query = hit["search_query"]

            if "region" in hit:
                region = hit["region"]
                #if hit["search_uid"] in self.list_of_search_uids:

                length2 = len(hit["results_groups"])


                #надо узнать статистику случаев когда больше двух брендов в результате выдачи
                #надо записывать бренды
                succsess_searches_count = 0
                brands = list()
                market_groups = list()

                for k in range(0, length2, 1):#по всем элементам results_groups, в каждом элементе один атрибут search_results
                    results_group_item = hit["results_groups"][k]

                    length3 = len(results_group_item["search_results"])

                    for j in range(0, length3, 1):
                        element_of_search_results = results_group_item["search_results"][j]


                        # отсутствующая в наличии запчасть это неудачный поиск!
                        if "part" in element_of_search_results and "brand_name" in element_of_search_results and "market_group_name" in element_of_search_results:

                            brand = element_of_search_results["brand_name"]
                            group = element_of_search_results["market_group_name"]
                            #search_result = 1  # удачный поиск
                            #cross = "cross" in element_of_search_results

                            if element_of_search_results["part"]["is_local_delivery"] == True:  # только с локального склада

                                if element_of_search_results["part"]["is_approximate_delivery_interval"] == False and self.with_tcp == False:  # не ТСП и ТСП не заказывали для запроса
                                    # все кроме ТСП, свои товары


                                    #self.all_searches.loc[len(self.all_searches)] = [timestamp, search_query, brand, region, group]  # товар с кроссами
                                    succsess_searches_count += 1
                                    brands.append(brand)
                                    market_groups.append(group)
                                    search_uid = hit["search_uid"]


                                elif self.with_tcp == True:  # заказали и те и те (задача 2)

                                    #self.all_searches.loc[len(self.all_searches)] = [timestamp, search_query, brand, region, group]  # товар с кроссами
                                    succsess_searches_count += 1
                                    brands.append(brand)
                                    market_groups.append(group)
                                    search_uid = hit["search_uid"]

                #keys = list(brands.keys())
                #keys2 = list(market_groups.keys())
                unique_brands = set(brands)
                unique_market_groups = set(market_groups)


                if succsess_searches_count == 0:#ни одного - неудачный
                    #+1 к счету таких случаев
                    self.not_succsess += 1

                elif succsess_searches_count == 1:#удачный - один в выдаче

                    #self.count3 += 1
                    self.all_searches.loc[len(self.all_searches)] = [search_uid,timestamp, search_query, list(unique_brands)[0], region, list(unique_market_groups)[0]]

                elif succsess_searches_count > 1 and len(unique_brands) == 1 and len(unique_market_groups) == 1:#удачный - 2 и один уникальный юренд и товарная группа
                    #self.count3 += 1
                    self.all_searches.loc[len(self.all_searches)] = [search_uid, timestamp, search_query, list(unique_brands)[0], region, list(unique_market_groups)[0]]

                elif succsess_searches_count > 1:
                    #self.count2 += 1
                    for row in range(0,len(brands),1):

                        self.searches_not_standart.loc[len(self.searches_not_standart)] = [timestamp, brands[row],market_groups[row]]

                #статистику по одиночным посчитаем по self.all_searches


    def do_logic_short(self, list_of_elements):
        self.do_logic(list_of_elements)

    def json_begin_file(self):
        return

    def json_end_file(self):
        return

class Search_sales(Base_Elastic):

    def __init__(self,search_uids_list,from_timestamp):

        self.search_uids_list = search_uids_list
        self.from_timestamp = from_timestamp

        self.sales = pd.DataFrame(columns=['Search_uid','Timestamp_of_sale','Sale'])

        self.sales_without_searches = pd.DataFrame(columns=['Search_uid','Timestamp_of_sale','Sale'])

    def get_query(self,q,list_of_args=list()):

        list_of_args.append(str(self.from_timestamp))

        query = q(list_of_args)
        return query


    def do_logic(self,list_of_elements):

        length = len(list_of_elements)
        for i in range(0, length, 1):
            sale = list_of_elements[i]
            if sale["search_uid"] in self.search_uids_list:
                self.sales.loc[len(self.sales)] = [sale["search_uid"],sale["timestamp"],1]
            else:
                self.sales_without_searches.loc[len(self.sales_without_searches)] = [sale["search_uid"],sale["timestamp"],1]


        #составим список, присоединим потом к фрейму

    def do_logic_short(self, list_of_elements):
        self.do_logic(list_of_elements)

    def json_begin_file(self):
        return

    def json_end_file(self):
        return

def query_make(list_of_args):
    if len(list_of_args)==0:
        gt = 10
    else:
        gt = list_of_args[0]
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


def query_make2(list_of_args):
    if len(list_of_args)==0:
        gt = 10
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

def query_make3(list_of_args):
    if len(list_of_args)==0:
        gt = 10
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
                        "event": "search"
                    }
                },
                {
                    "exists" : { "field" : "results_groups.search_results.brand_name" }
                },
                {
                    "exists" : { "field" : "results_groups.search_results.market_group_name" }
                }
              ]
       }
    }
    return query

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
                 {
                     "match": {
                        "event": "checkout"
                     }
                 }
             ]
         }
    }
    return query



def draw_statistics(search_results):
    s = search_results.all_searches.shape
    print(s[0])#количество удачных поисков всего

    a1 = search_results.searches_not_standart.groupby('Timestamp', as_index=False)['Brand'].count()
    a2 = search_results.searches_not_standart.groupby('Timestamp', as_index=False)['Group'].count()

    r = pd.merge(a1, a2)
    r.to_csv('two_or_more_brands_or_groups_in_serch_results.csv')

    #r.groupby("Timestamp",'Brand','Group', as_index=False).count()#смотрим сколько каких сочетаний бренда и групп есть
    #дальше на основе этого будем смотреть что делать дальше!



def run_logic(from_db=True,with_sales=False):
    #todo надо будет делать чтобы уже сохраненные данные в файле не запрашивались, видимо time stamp, надо сохранить timestamp последнего документа, или вообще хранить их в отдельной колонке, поскольку временные ряды пригодятся!

    n = 0
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
        q = query_make3
        search_results.get_data(q, index, timestampe_field_name)

        # search_results.finded - фрейм с результатами поиска с брендами, регионами, товарными группами
        all_searches = search_results.all_searches

        n = search_results.not_succsess#todo сохранить в файл

        draw_statistics(search_results)

        #+++++++++++++++
        if with_sales==True:

            #делаем запрос на продажи
            sales = Search_sales(Base_Elastic,list(all_searches.columns[0:1]))
            q = query_make4
            sales.get_data(q, index, timestampe_field_name)

            #todo проверить на уникальность все значения индекса!
            l1 = len(all_searches['Search_uid'].unique())
            l2 = len(sales.sales['Search_uid'].unique())
            if l1 != all_searches.shape[0] or l2 != sales.sales.shape[0]:
                exit(1)

            all_searches.set_index('Search_uid')
            sales.sales.set_index('Search_uid')

            all_searches = pd.concat([all_searches, sales.sales], axis=1, sort=False)
            all_searches.fillna(0)


        all_searches.to_csv('out.csv')

    else:

        all_searches = pd.read_csv('out.csv')


    #df_sucsess_searches = all_searches.query("Search_result == 1")

    return  [all_searches,1]



def calc_percentage(search_results, f,n):

    all_searches = f(search_results)

    sum = all_searches.values.sum()

    percentage_of_sucsess = pd.DataFrame(index = all_searches.index, columns=all_searches.columns.values)
    #percentage_of_sucsess = pd.DataFrame()

    s = all_searches.shape
    for i in range(0, s[0], 1): # по строкам
        row = []
        #row_name = all_searches.index[i]
        for j in range(0, s[1], 1):  # по столбцам

            """
            col_name = all_searches.columns[j]

            if row_name in sucsess_searches.index and col_name in sucsess_searches.columns:
                if all_searches.iloc[i][j]==0:
                    percentage_in_cell = float(0)
                else:
                    percentage_in_cell = float((sucsess_searches.loc[row_name][col_name] * 100) / all_searches.iloc[i][j])
            #sucsess_searches.loc["Братск"]["ERA"]
            else:
                percentage_in_cell = float(0)
            """
            percentage_in_cell = (all_searches.iloc[i][j]*100)/(sum + n)
            #if percentage_in_cell>0:
             #   percentage_in_cell = 1



            row.append(percentage_in_cell)

        percentage_of_sucsess.iloc[i] = row

        #++++++++++++++++++++
        #иатрицу конверсии в продажи считаем
        #заодно проверим согласованноссть с поисками
        #сначала отфильтровать поиски завершившиеся продажей Sales==1,
        #потом на основе этого сделать кросстаб по разрезам,
        #пройтись по всем поискам по координатам поисков завершенных продажей в каждой ячейке посчитать % удачных поисков завершенных продажей, идти по поисковой матрице , если нет индекса в продажной то поиски в ячейке без продаж!


    return percentage_of_sucsess


def region_brand(search_results):

    all_searches = pd.crosstab(search_results.Region, search_results.Brand)  # таблица по всем поискам
    #sucsess_searches = pd.crosstab(df_sucsess_searches.Region, df_sucsess_searches.Brand)  # таблица по удачным поискам

    return all_searches

def region_group(search_results):

    all_searches = pd.crosstab(search_results.Region, search_results.Group)  # таблица по всем поискам
    #sucsess_searches = pd.crosstab(df_sucsess_searches.Region, df_sucsess_searches.Group)  # таблица по удачным поискам

    return all_searches


def brand_group(search_results):

    all_searches = pd.crosstab(search_results.Brand, search_results.Group)  # таблица по всем поискам
    #sucsess_searches = pd.crosstab(df_sucsess_searches.Brand, df_sucsess_searches.Group)  # таблица по удачным поискам

    return all_searches




#---------------------------------------

"""
"4b99e04d6bfc52e4290d48ade6fdeff0" - в нем есть part
"""

#if __name__== "__main__":
  #main()

"""
делаем вот такой запрос, на получение всех поисков- сейчас таких документов ~71000
{
  "query": {
    "bool": {
      "must": [
       
        {
          "match": {
            "is_internal_user": false
          }
        },
        {
        "match": {
            "event": "search"
          }
        },
        {
        	"exists" : { "field" : "results_groups.search_results.brand_name" }
        },
        {
        	"exists" : { "field" : "results_groups.search_results.market_group_name" }
        }
      ]
    }
  },
  "sort": [{"timestamp": {"order": "asc"}}]
}

удачный поиск т.е. +1 к числу удачных поисков это когда мы 

делаем запрос который отдает документы в которых была некая выдача(не пустое поле results_groups)

из этих документов по запросу выше отбираем те внутри которых есть хотябы один
results_groups.search_results.part (товар есть в наличии)
и в этом part свойство is_local_delivery = True(локальный склад)
и в этом part свойство is_approximate_delivery_interval == False(не ТСП, свои товары)

т.е. если выполняется все 3 условия хотя-бы один раз, то это +1 к числу удачных поисков


"""

"""
веса брендов - nsi есть,
есть список наших брендов
если есть хотя бы один с 3 мя условиями то этот поиск удачный (+1) к  счету удачных поисков
если нет ничего с этими 3 мя условиями то это поиск неудачный

открытый вопрос, какой бренд и маркетинговую группу брать для удачных и неудачных поисков?

есть 3 вида выдачи:
выдача на старом сайте
выдача на новом может быть 2 х видов: предварительная(списком) и сразу карточка товара, - для карточки сразу берем бренд и маркетинговую группу, тут понятно

а для списков, что брать?

надо смотреть на количество (стат. значимость?) случаев когда >1 подходящего под условие товара, если их мало, то можно не брать пока в расчет

надо поделать запросы на сайте и сразу посмотреть в elastic какие делаются записи(это способ понять чем отличаются разные интерфесы в elastic)

неудачный поиск региона, бренда и товарной группы не имеет!
"""
#ghj

"""
"results_groups": [
                        {
                            "search_results": [
                                {
                                    "brand_name": "Lemforder",
                                    "market_group_name": "Рычаг подвески",
                                    "link": "http://tvr.rossko.ru/product?text=NSIN0019752547",
                                    "goods_name": "Рычаг подвески  | зад лев |",
                                    "crosses_count": 22,
                                    "part": {
                                        "price": 2249.64,
                                        "availability": 0,
                                        "is_approximate_delivery_interval": false,
                                        "is_local_delivery": true
                                    },
                                    "cross": {
                                        "price": 387.13,
                                        "availability": 0,
                                        "is_approximate_delivery_interval": false,
                                        "is_local_delivery": false
                                    }
                                },
                                {
                                    "brand_name": "Delphi",
                                    "link": "http://tvr.rossko.ru/product?text=NSII0015289905",
                                    "goods_name": "Наконечник рулевой",
                                    "crosses_count": 1,
                                    "part": {
                                        "price": 2249.64,
                                        "availability": 0,
                                        "is_approximate_delivery_interval": false,
                                        "is_local_delivery": true
                                    },
                                    "cross": {
                                        "price": 404.08,
                                        "availability": 4,
                                        "is_approximate_delivery_interval": false,
                                        "is_local_delivery": false
                                    }
                                },
                                {
                                    "brand_name": "Бренд1",
                                    "link": "http://tvr.rossko.ru/product?text=NSII0015289905",
                                    "goods_name": "Наконечник рулевой",
                                    "crosses_count": 1,
                                    "cross": {
                                        "price": 404.08,
                                        "availability": 4,
                                        "is_approximate_delivery_interval": false,
                                        "is_local_delivery": false
                                    }
                                },
                            ]
                        }
                    ],

"""