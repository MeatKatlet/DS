import json
#from pprint import pprint
from pandas.io.json import json_normalize
import pandas as pd
import matplotlib.pyplot as plt
import pandas.tools.plotting as plotting

#import sklearn

import numpy as np
import requests
import warnings
import time
import collections
#import searches.searches
#from searches import searches
from validate.validate_searches import Availebness_in_1c
from  frame_partial_reader.frame_partial_reader import Frame_partial_reader
import psutil
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
    first_timestamp_from_query = 0
    timestamp_for_query = 'timestamp'
    index = 'cart_item_event'
    def __init__(self):
        self.df = pd.DataFrame()
        # self.query = ""
        self.f = open('result.json', 'a')


    def get_query(self,q,list_of_args=list()):
        query = q(list_of_args)
        return query

    def get_data(self, q,size=100):
        #query = q()
        query = self.get_query(q)
        body = {
            "size": size,
            "query": query,
            "sort": [{self.timestamp_for_query: {"order": "asc"}}]}

        headers = {'Accept': 'application/json', 'Content-type': 'application/json'}
        elastic_url = 'http://roesportal.rossko.local:80/' + self.index + '-*/_search/?size='+str(size)+'&pretty'
        query = json.dumps(body)


        while True:
            response = requests.post(elastic_url, data=query, verify=False, headers=headers)
            if (response.status_code == 502):#, response.text.find("502 Bad Gateway") > 0
                print("Bad Gateway... REPEAT")
                continue
            else:

                break

        deserilised = json.loads(response.text)


        total_size = deserilised["hits"]["total"]
        if total_size>0:
            self.first_timestamp_from_query = deserilised["hits"]["hits"][0]["_source"][self.timestamp_for_query]

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
        last_tsmp = deserilised["hits"]["hits"][l - 1]["_source"][self.timestamp_for_query]



        self.json_begin_file()

        while True:

            #query = q(str(last_tsmp))
            query = self.get_query(q, [str(last_tsmp)])

            body = {
                "size": size,
                "query": query,
                "sort": [{self.timestamp_for_query: {"order": "asc"}}]}

            query = json.dumps(body)

            response = requests.post(elastic_url, data=query, verify=False, headers=headers)
            #time.sleep(5)
            if (response.status_code == 502):#response.text.find("502 Bad Gateway") > 0
                print("Bad Gateway... REPEAT")
                continue

            deserilised = json.loads(response.text)
            #time.sleep(5)
            self.do_logic(deserilised["hits"]["hits"])

            # tsmp = df["_source.search_timestamp"].max()

            l = len(deserilised["hits"]["hits"])
            if l>0:
                k = l - 1

                # last_tsmp = response['_source.'+timestampe_field_name]
                last_tsmp = deserilised["hits"]["hits"][k]["_source"][self.timestamp_for_query]
            else:
                break
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
        self.from_timestamp = 1530403200
        #self.list_of_search_uids = list_of_search_uids

        # создаем df в котором будут в каждой строке инфа по поиску запчасти, колонки искомый артикул?, результат поиска(1/0, удачный/неудачный), бренд, регион, товарная группа,
        self.all_searches = pd.DataFrame(columns=['Search_uid','Search_query', 'brand', 'region', 'group', 'Search_result'])#бренд/товарная группа браться будут из поискового запроса
        #self.searches_not_standart = pd.DataFrame(columns=['Timestamp', 'Brand', 'Group'])#статистика по нестандартным выдачам
        self.all_searches_dict = {}
        self.row_count = 0
        self.with_tcp = False  # брать в расчет или нет товары строннних поставщиков(заменители) (is_approximate_delivery_interval?) это для задачи 2, для задачи 1 их в расчет не брать
        # кроссы не буду брать в расчет пока
        self.with_crosses = True

        self.goods_classifier = list()
        self.dump = 0


        #f = pd.read_csv("searches.csv")

        #self.list_of_search_uids_of_searches_events_ended_with_sales = f["search_uid"].unique()


        pr = Frame_partial_reader()
        pr.read("parts_with_sg_mg.csv")



        self.goods_classifier = pr.parse_result
        #self.goods_classifier = pr.nsi_dict#это для тестирования!

        self.brands_dict = pr.brands
        self.groups_dict = pr.groups
        #self.articles_dict = pr.articles

        self.regions_dict = collections.OrderedDict()

        #номер детали - хеш(бренд+группа) (бренд группа)
        self.not_founded_several_brands = 0

        self.not_founded_out_of_articles_range = 0
        self.founded_several_combinations = {}
        #self.founded_several_combinations2 = 0

        #self.not_succsess2 = 0
        #self.count = 0
        #self.count2 = 0
        #self.count3 = 0

        #тсп/не тсп(одна партерра пока видимо)
        #локальный склад/не локальный
        #есть кросс или нет на искомый товар
        #регион локального склада
        #регион не локального склада



    def test_query(self,list_of_args):
        if len(list_of_args) == 1:
            gt = list_of_args[0]  # базовый от 1 июля
        else:
            gt = list_of_args[0]
        query = {
            "bool": {
                "must": [
                    {"range": {"timestamp": {"gt": gt,"lte": 1533081600}}},

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
                        "match": {
                            "region": "Новосибирск"
                        }
                    },

                    {
                        "exists": {"field": "results_groups"}
                    },
                    {
                        "terms": {
                            "search_uid": list(self.list_of_search_uids_of_searches_events_ended_with_sales)
                        }
                    },
                ]

            }
        }

        return query
    """
    {
          "exists": {"field": "results_groups.search_results.brand_name"}
    },
    {
          "exists": {"field": "results_groups.search_results.market_group_name"}
     },
    """
    def get_query(self,q,list_of_args=list()):

        list_of_args.append(str(self.from_timestamp))

        query = q(list_of_args)
        return query

    def do_logic(self, list_of_elements):
        # list_of_elements - это список объектов из объекта hits в результате выдачи
        # здесь будем фильтровать нужные мне результаты выдачи и сохранять во фрейм
        length = len(list_of_elements)-1
        for i in range(0, length, 1):

            hit = list_of_elements[i]["_source"]
            #timestamp = hit["timestamp"]
            #if len(self.all_searches) > 100000:#если памяти меньше 100 мб то дамп делаем фрейма и обнуляем его!
                #self.all_searches.to_csv("dump_"+str(self.dump)+".csv", index=False)
                #self.dump += 1
                #self.all_searches = pd.DataFrame(columns=['Search_uid', 'Search_query', 'brand', 'region', 'group', 'Search_result'])

            if "search_query" not in hit:
                continue
            search_query = hit["search_query"]
            search_uid = hit["search_uid"]
            #page = hit["page_number"]

            #have_region = 0
            #results_groups = 0
            #search_results = 0
            #availably = 0
            #brand_group_have_names = 0
            #query_dictionary_brand_group_overlap = 0
            #local_store = 0
            #tcp = 0
            succsess_searches_count = 0  # не ТСП

            #brand_from_query = self.get_from_query(search_query)

            #список сочетаний брендов-товарных групп

            if "region" in hit:
                region = hit["region"]
                #have_region += 1

                if region not in self.regions_dict:
                   self.regions_dict[region] = len(self.regions_dict)

                length2 = len(hit["results_groups"])
                #if length2 == 0:
                    #results_groups += 1

                #надо узнать статистику случаев когда больше двух брендов в результате выдачи
                #надо записывать бренды
                succsess_searches_count = 0
                #succsess_searches_count2 = 0
                #brands = list()
                #market_groups = list()
                brand_group_variants = {}
                #df = pd.DataFrame(columns=["brand","group"])
                empty_result = True

                for k in range(0, length2, 1):#по всем элементам results_groups, в каждом элементе один атрибут search_results
                    results_group_item = hit["results_groups"][k]

                    length3 = len(results_group_item["search_results"])
                    #if length3==0:
                        #search_results += 1

                    for j in range(0, length3, 1):
                        element_of_search_results = results_group_item["search_results"][j]
                        empty_result = False

                        # отсутствующая в наличии запчасть это неудачный поиск!
                        if "part" in element_of_search_results and "brand_name" in element_of_search_results and "market_group_name" in element_of_search_results:
                            #availably += 1

                            brand = element_of_search_results["brand_name"]#.replace(r"'", "").replace("\\","")
                            group = element_of_search_results["market_group_name"]
                            nsi = element_of_search_results["link"].split("=")[1] #из ссылки извлекаем
                            #совпадение по бренду/товарной группе из поисковой строки

                            res = self.check_presence_in_list(search_query, nsi)#[result, brand_group_combination]


                            if brand!= "" and group!= "" and res[0] == True:#есть в списке сочетаний брендов и групп по этому номеру товара
                                #query_dictionary_brand_group_overlap += 1

                                #search_result = 1  # удачный поиск
                                cross = "cross" in element_of_search_results

                                if element_of_search_results["part"]["is_local_delivery"] == True:  # только с локального склада
                                    #local_store += 1

                                    if element_of_search_results["part"]["is_approximate_delivery_interval"] == False and self.with_tcp == False:  # не ТСП и ТСП не заказывали для запроса
                                        # все кроме ТСП, свои товары
                                        succsess_searches_count += 1

                                        #res[1] - это tuple из комбинации кодов (бренд, группа)

                                        if res[1] not in brand_group_variants:
                                            brand_group_variants[res[1]] = {"cards":0,"carts":0,"preview":0}
                                        #else:
                                            #brand_group_variants[res[1]] ={}



                                    elif self.with_tcp == True:  # заказали и те и те (задача 2)

                                        succsess_searches_count += 1


                                        if res[1] not in brand_group_variants:
                                            brand_group_variants[res[1]] = {"cards":0,"carts":0,"preview":0}
                                        #else:
                                            #brand_group_variants[res[1]] += 1

                                    #else:
                                        #tcp +=1
                #unique_brands = set(brands)
                #unique_market_groups = set(market_groups)

                #if succsess_searches_count2 > 0:
                    #self.not_succsess2 += 1

                if succsess_searches_count == 0 and empty_result == False:#ни одного - неудачный + непустые выдачи были
                    r = self.check_variants(search_query)
                    q = r[1]
                    if r[0]:#искал то что у нас есть обычно(может быть в принципе)!
                        #либо юзер подразумевал сразу несколько сочетаний брендов и групп, которые ищет
                        if len(self.goods_classifier[q][1])>1:#

                            self.not_founded_several_brands += 1#5379 - не было совпадений в выдаче с тем что подразумевается под номером детали(совпадений с наличием?)
                        # либо одно сочетание бренда группы которое ищет
                        elif len(self.goods_classifier[q][1])==1:#т.е. то что не нашел то и записываем в ответ
                            brand_code = list(self.goods_classifier[q][1].keys())[0][0]
                            group_code =  list(self.goods_classifier[q][1].keys())[0][1]
                            #brand_code = list(self.goods_classifier[search_query][1].keys())[0][0]
                            #group_code = list(self.goods_classifier[search_query][1].keys())[0][1]

                            self.add_to_frame(search_uid, q, brand_code, region, group_code, 0)

                    else:#такого запроса нет в классификаторе!
                        self.not_founded_out_of_articles_range += 1#17523


                elif succsess_searches_count == 1 and len(brand_group_variants)==1:#одно совпадение но с одним и тем же
                    r = self.check_variants(search_query)
                    q = r[1]

                    # либо юзер подразумевал сразу несколько сочетаний брендов и групп, которые ищет
                    if len(self.goods_classifier[q][1])>1:#подразумевал несколько сочетаний а нашел 1 совпадение из подразумеваемых с выжачей
                        #todo какой бренд/группу брать для этого? правильно ли я сделал, ведь юзер подразумевает несколько сочетаний
                        #получается те которые нашел!, совпал 1 раз по наличию со списком подразумеваемого

                        brand_code = list(brand_group_variants.keys())[0][0]
                        group_code = list(brand_group_variants.keys())[0][1]


                        self.add_to_frame(search_uid, q, brand_code, region, group_code, 1)


                    # либо одно сочетание бренда группы которое ищет
                    elif len(self.goods_classifier[q][0]) == 1:
                        brand_code = list(brand_group_variants.keys())[0][0]
                        group_code = list(brand_group_variants.keys())[0][1]

                        self.add_to_frame(search_uid, q, brand_code, region, group_code, 1)

                elif succsess_searches_count > 1 and len(brand_group_variants)==1:#несколько совпадений но с одним и тем же
                    r = self.check_variants(search_query)
                    q = r[1]
                    # либо юзер подразумевал сразу несколько сочетаний брендов и групп, которые ищет
                    if len(self.goods_classifier[q][1])>1:
                        brand_code = list(brand_group_variants.keys())[0][0]
                        group_code = list(brand_group_variants.keys())[0][1]

                        self.add_to_frame(search_uid, q, brand_code, region, group_code, 1)
                    # либо одно сочетание бренда группы которое ищет
                    elif len(self.goods_classifier[q][1]) == 1:
                        brand_code = list(brand_group_variants.keys())[0][0]
                        group_code = list(brand_group_variants.keys())[0][1]

                        self.add_to_frame(search_uid,q, brand_code, region, group_code, 1)

                elif succsess_searches_count > 1 and len(brand_group_variants) > 1:  # несколько совпадений но с разными сочетаниями
                    r = self.check_variants(search_query)
                    q = r[1]
                    if len(self.goods_classifier[q][1]) > 1:#здесь только так может быть!
                        #todo какой бренд группу брать?
                        #self.founded_several_combinations += 1#46 таких
                        self.founded_several_combinations[search_uid] = brand_group_variants#пагинация должна уменьшить количество конфликтных ситуаций
                        #удалять строку из фрейма если из-за пагинации там уже что-то есть
                        #if (self.all_searches['Search_uid'] == search_uid).any() == True:
                            #self.all_searches.drop(self.all_searches[self.all_searches['Search_uid'] == search_uid].index,inplace=True)


                #статистику по одиночным посчитаем по self.all_searches



    def add_to_frame(self,search_uid,search_query,brand_code,region,group_code,result):
        #только если не в конфликтых уже
        if search_uid not in self.founded_several_combinations:
            self.all_searches_dict[self.row_count] = [
                search_uid,
                search_query,
                brand_code,
                self.regions_dict[region],
                group_code,
                result
            ]
            self.row_count += 1
            """
            self.all_searches.loc[self.all_searches.shape[0]] = [
                search_uid,
                search_query,
                brand_code,
                self.regions_dict[region],
                group_code,
                result
            ]
            """


    def check_presence_in_list_old(self,search_query, brand, group):

        if search_query in self.goods_classifier:
            #if brand in self.goods_classifier[search_query] and self.goods_classifier[search_query][brand]==group:#совпадает по бренду и товарной группе
            #входит ли сочетание бренд/группа в список сочетаний под этим номером
            if brand in self.brands_dict and group in self.groups_dict and (self.brands_dict[brand],self.groups_dict[group]) in self.goods_classifier[search_query]:
                return True
        return False

    def check_variants(self,search_query):
        raw = search_query
        without_spaces = search_query.replace(" ", "")
        without_spaces_and_defises = without_spaces.replace("-", "")
        without_dies_suffix = without_spaces_and_defises.split("#")[0]
        result = False
        q= 0
        if raw in self.goods_classifier:
            result = True
            q = raw

        elif without_spaces in self.goods_classifier:
            result = True
            q = without_spaces

        elif without_spaces_and_defises in self.goods_classifier:
            result = True
            q = without_spaces_and_defises

        elif without_dies_suffix in self.goods_classifier:
            result = True
            q = without_dies_suffix

        return [result,q]

    def check_presence_in_list(self,search_query, nsi):
        #todo артикулы в справочнике могут не совпадать!
        #надо пробовать по разному - сырой, без пробелов, без дефисов
        #бренды в elastic не совпадают с брендами в справочнике NSI поэтому
        #надо получить по артикулу список NSI и проверить присутствие
        result = False
        brand_group_combination = ()

        """
        self.goods_classifier[0][NSI 1] = id1 - номер соответствующего сочетания бренд/группа для этого NSI
        self.goods_classifier[0][NSI 2] = id2
        
        self.goods_classifier[1][id1] = (код бренда, код группы)
        self.goods_classifier[1][id2] = (код бренда, код группы)
        """
        r = self.check_variants(search_query)
        if r[0]:
            if nsi in self.goods_classifier[r[1]][0]:
                result = True

                position_of_key_in_list = self.goods_classifier[r[1]][0][nsi]


                brand_group_combination = list(self.goods_classifier[r[1]][1])[position_of_key_in_list]


        return [result,brand_group_combination]#список совпавших будет возвращять!


    def do_logic_short(self, list_of_elements):
        self.do_logic(list_of_elements)

    def json_begin_file(self):
        return

    def json_end_file(self):
        return

class Search_sales(Base_Elastic):

    def __init__(self,search_uids_list,from_timestamp):

        length = search_uids_list.shape[0]
        self.search_uids_list = dict(zip(list(search_uids_list), [i for i in range(0, length)]))

        self.from_timestamp = from_timestamp

        self.sales = pd.DataFrame(columns=['Search_uid_sales','Timestamp_of_sale','Sale'])

        self.sales_without_searches = pd.DataFrame(columns=['Search_uid','Timestamp_of_sale','Sale'])

    def get_query(self,q,list_of_args=list()):

        list_of_args.append(str(self.from_timestamp))

        query = q(list_of_args)
        return query

    def query_make4(self,list_of_args):
        if len(list_of_args) == 1:
            gt = list_of_args[0]  # базовый timestamp будет добавлять
            range = {"timestamp": {"gt": gt, "lt": 1533081600}}
        else:
            gt = list_of_args[0]  # это будет когда уже в цикле идет запрос
            range = {"timestamp": {"gt": gt, "lt": 1533081600}}

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
                    },
                    {
                        "terms": {
                            "region": ["Новосибирск", "Санкт-Петербург"]
                        }
                    },

                ]
            }
        }
        return query#получить список всех member_id Нового портала ~1500 и запросы на оформление заказа делать только по этому списку(т.е. это пользователи нового портала!)
    def do_logic(self,list_of_elements):

        length = len(list_of_elements)
        for i in range(0, length, 1):
            sale = list_of_elements[i]
            if sale["_source"]["search_uid"] in self.search_uids_list:
                self.sales.loc[len(self.sales)] = [sale["_source"]["search_uid"],sale["_source"]["timestamp"],1]
            else:
                self.sales_without_searches.loc[len(self.sales_without_searches)] = [sale["_source"]["search_uid"],sale["_source"]["timestamp"],1]
                #запросить события с такими id с чего они начались эти продажи?пройтись циклом и посчитать количество событий продаж

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
    if len(list_of_args)==1:
        gt = list_of_args[0]#базовый от 1 июля
    else:
        gt = list_of_args[0]
    query = {
       "bool": {
              "must": [
                {"range": {"timestamp": {"gt": gt,"lt": 1533081600}}},

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

"""
                {
                      "match": {
                          "region": "Новосибирск"
                      }
                },
"""
def query_make4(list_of_args):
    if len(list_of_args)==1:
        gt = list_of_args[0]  # базовый timestamp будет добавлять
        range = {"timestamp": {"gt": gt,"lt": 1533081600}}
    else:
        gt = list_of_args[0]#это будет когда уже в цикле идет запрос
        range = {"timestamp": {"gt": gt,"lt": 1533081600}}

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

"""
{
                     "terms": {
                         "region": ["Новосибирск", "Санкт-Петербург"]
                     }
                 }
"""

"""
brands_list = []

def build_query():
    q = [

        {
            "bool": {
                "must": [
                    {
                        "match": {
                            "results_groups.search_results.brand_name": "имя бренда"
                        }
                    },
                    {
                        "match": {
                            "results_groups.search_results.market_group_name": "имя маркетинговой группы"
                        }
                    }
                ]
            }

        },

        {
            "match": {
                "results_groups.search_results.brand_name": "имя маркетинговой группы"
            }
        }

    ]
    return q
def query_make5(list_of_args):
    global brands_list
    if len(list_of_args)==0:
        gt = 10
    else:
        gt = list_of_args[0]

    if len(brands_list)==0:
        brands_list = build_query()


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
                     "match": {
                          "region": "Новосибирск"
                     }
                },
                {
                    "exists" : { "field" : "results_groups.search_results.brand_name" }
                },
                {
                    "exists" : { "field" : "results_groups.search_results.market_group_name" }
                },
                {
                    "bool": {
                          "should": brands_list,
                          "minimum_should_match": 1
                      }
                }

              ]

       }
    }
    return query
"""



class Search_plots_factory():
    first_timestamp_from_query = 0
    def __init__(self):
        self.index = 'cart_item_event'
        self.timestampe_field_name = "timestamp"

        self.main_frame = pd.DataFrame()

        self.slice_region_brand = pd.DataFrame()
        self.slice_region_group = pd.DataFrame()
        self.slice_brand_group = pd.DataFrame()
        self.slice_col1 = ""#должно быть одинаковым и во фрейме и в имени поля для сохранения
        self.slice_col2 = ""

        #self.not_succsess_searches = 0
        #self.founded_several_combinations = 0
        self.brand_dict = []#todo нужно разобраться с тем почему не доступно в классе наследнике и приходится через передачу объекта родителя в конструктор присваивать значения атрибутам
        self.group_dict = []
        self.region_dict = []

    prefix = ""

    def get_main_dataframe(self,from_db=True):#получает главный фрейм из бд или файла и сохраняет его в атрибут класса, для дальнейшего доступа к нему откуда угодно

        #todo пропустить весь главный фрейм через фильтр с нашими брендами!, там написаны в ручную имена товарных групп, надо как-то это делать вручную!
        #пока оставим как есть все!
        # todo надо будет делать чтобы уже сохраненные данные в файле не запрашивались, видимо time stamp, надо сохранить timestamp последнего документа!
        #ищет бренд/группу - строка запроса, - результат в выдаче есть соответствующий его бренду/товарной группе и этот товар в наличии (это удачный поиск)
        #ищет бренд/группу - строка запроса, - результат в выдаче есть соответствующий его бренду/товарной группе и этого товара не в наличии (это неудачный поиск)
        # это значит что у неудачного поиска будет бренд и товарная группа! и в каждой ячейке мы будем брать за 100% все поиски в этой ячейке за 100%

        # в каждой выдаче надо получить из поискового запроса бренд и товарную группу которую ищет юзер, нужно создать справочник номеров деталей, каждой детали будет соответствовать бренд и товарная группа запишкм их в массив(или сделаем специальную функцию проходящую по фрейму несколькок раз)
        #оставляем логику как есть, но добавляем на проверку удачных совпадение по бренду/товарной группе с выдачей(+1 условие)
        #если совпадение есть по бренду/группе но нет в наличии то это неудачный поиск и он имеет город бренд товарную группу и за 100 % мы будем брать только одну ячейку, (или не важно есть ли совпадение с выдачей или нет, то считаем этот поиск за +1 неудачный и приписываем ему бренд /группу)


        if from_db:
            # todo из файла получать только наши бренды!


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
            # q = query_make
            search_results = Search_results_events()  # searches_input_field.list_of_search_uids передавать когда понадобится фильтрация по мобытию вставки номера детали в строку поиска
            q = query_make3
            #q = search_results.test_query
            search_results.get_data(q)
            #search_results.types_of_results.to_csv("test_10000.csv", index=False)
            #search_results.all_searches_dict
            self.main_frame = pd.DataFrame.from_dict(search_results.all_searches_dict, orient='index',columns=['Search_uid','Search_query', 'brand', 'region', 'group', 'Search_result'])
            search_results.all_searches_dict = {}
            #self.main_frame = search_results.all_searches
            self.brand_dict = search_results.brands_dict
            self.group_dict = search_results.groups_dict
            self.region_dict = search_results.regions_dict
            search_results.goods_classifier = {}
            #self.goods_classifier = search_results.goods_classifier

            #self.founded_several_combinations = search_results.founded_several_combinations
            #self.not_succsess_searches = search_results.not_succsess

            #special = pd.DataFrame(columns=["Data"])
            #special.loc[len(special)] = [self.not_succsess_searches]
            #special.to_csv('special.csv')#сохранить в файл

            #self.draw_statistics(search_results)
            self.collapse_rows_from_one_pagination_chain()
            #before_frame.to_csv('out2.csv', index=False)
            #self.test_frame(before_frame)

            self.main_frame.to_csv('out.csv', index=False)
            self.first_timestamp_from_query = search_results.first_timestamp_from_query

            with open('special.json', 'w') as fp:
                json.dump(self.first_timestamp_from_query, fp)
            with open('brand_dict.json', 'w') as fp:
                json.dump(self.brand_dict, fp)
            with open('group_dict.json', 'w') as fp:
                json.dump(self.group_dict, fp)
            with open('region_dict.json', 'w') as fp:
                json.dump(self.region_dict, fp)

        else:
            #special = pd.read_csv('special.csv')
            #self.not_succsess_searches = special.iloc[0][1]

            #search_results = Search_results_events()
            #self.brand_dict = search_results.brands_dict
            #self.group_dict = search_results.groups_dict
            #self.region_dict = search_results.regions_dict
            #self.goods_classifier = search_results.goods_classifier

            #self.main_frame = pd.read_csv('out2.csv')
            #self.main_frame = pd.read_csv('out.csv')


            #before_frame = self.collapse_rows_from_one_pagination_chain()

            #v = pd.read_csv('validated.csv')
            #self.test_frame(before_frame)

            self.main_frame = pd.read_csv('out.csv')
            #self.collapse_rows_from_one_pagination_chain()
            with open('special.json') as data_file:
                self.first_timestamp_from_query = json.load(data_file)
            with open('brand_dict.json') as data_file:
                self.brand_dict = json.load(data_file)
            with open('group_dict.json') as data_file:
                self.group_dict = json.load(data_file)
            with open('region_dict.json') as data_file:
                self.region_dict = json.load(data_file)
            #+++++++++++++++++++++++++++++++++++++++++++++++
            #writer = pd.ExcelWriter('main_frame.xlsx')
            #self.main_frame.to_excel(writer, 'Sheet1')

            #writer.save()
            #todo из фрейма фильтровать ьлдбко наши бренды!

    def test_frame(self,before_frame):
        # -----------------------------------для теста-----------------------------------------------------
        availebness_1c = Availebness_in_1c()

        availebness_1c.parse_xml_fill_dictionary("vigruzka.xml")

        #availebness_1c.presence_in_1c  # проверка присутствия по NSI в этом словаре!

        # -------------------------------------------------------------------------------------------------

        d = pd.DataFrame(np.zeros((self.main_frame.shape[0], 1)))
        self.main_frame['validate_result'] = d
        rows = self.main_frame.shape[0]
        for row in range(0,rows,1):
            line = self.main_frame.iloc[row]
            article = line[1]
            search_uid = line[0]
            nsi_list = list(self.goods_classifier[article][0])

            df = before_frame[(before_frame["Search_uid"] == search_uid) & (before_frame["Search_query"]==article)]
            rows2 = df.shape[0]
            result = 0
            for row2 in range(0,rows2,1):
                line2 = df.iloc[row2]
                timestamp = line2[1]

                r = availebness_1c.check_good_availible(timestamp,nsi_list)
                if r:
                    result+=1

            if result>0:
                self.main_frame.iloc[row][6] = 1

        m1 = self.main_frame[(self.main_frame['Search_result'] == 0) & (self.main_frame['validate_result'] == 0)].shape[0]
        m2 = self.main_frame[(self.main_frame['Search_result'] == 0) & (self.main_frame['validate_result'] == 1)].shape[0]#таких должно быть мало
        m3 = self.main_frame[(self.main_frame['Search_result'] == 1) & (self.main_frame['validate_result'] == 0)].shape[0]#таких должно быть мало
        m4 = self.main_frame[(self.main_frame['Search_result'] == 1) & (self.main_frame['validate_result'] == 1)].shape[0]

        print([m1,m2, m3, m4])




    def collapse_rows_from_one_pagination_chain(self):
        #todo проконтролировать чтобы во фрейм попадали только search_uid из одной серии пагинаций, чтобы других причин для дубликатов не было!
        #эта функция схлопывает только пагинацию и выявляет странные дубликаты с разными поисковыми запросами, она не работает с конфликтами,
        #группируем по полям, объединяем тем самым выдачи из одной серии страниц пагинации, суммируем для выяснения общего результата поиска в одной серии страниц пагинации
        #before_frame = self.main_frame

        self.main_frame = self.main_frame.groupby(['Search_uid', 'Search_query','region','brand', 'group'], as_index=False)['Search_result'].agg('sum')



        conflicted_base = self.main_frame.groupby(['Search_uid'])['Search_uid'].agg('count')
        conflicted = conflicted_base[conflicted_base > 1]
        #self.founded_several_combinations += conflicted.shape[0]
        #подсчитать количество таких search_uid(>1) и записать в счетяик таких ситуаций!
        #удалить конфликтные id
        #c = self.main_frame[self.main_frame['Search_uid'].isin(conflicted.index)].groupby( ['Search_uid', 'Search_query']).agg("count")
        #c2 = self.main_frame[self.main_frame['Search_uid'].isin(conflicted.index)].groupby(['Search_uid', 'region', 'brand', 'group']).size()
        #todo заменить везде где Search_result > 0 на 1 - это из-за суммирования!

        self.main_frame.drop(self.main_frame[self.main_frame['Search_uid'].isin(conflicted.index)].index,inplace=True)


        self.main_frame.loc[self.main_frame['Search_result'] > 0, 'Search_result'] = 1
        #исключаем странные события все! делаем только уникальные
        #return before_frame

    def add_pretty_rows_and_columns_names(self,df):

        slice_rows = self.slice_col1
        slice_columns = self.slice_col2
        list1 = list(getattr(self, slice_rows+"_dict"))#справочник1 для строк
        list2 = list(getattr(self, slice_columns+"_dict"))#справочник2 для колонок

        result = []
        for i, val in enumerate(df.index):
            result.append(list1[val])
        df.index = result

        result = []
        for i, val in enumerate(df.columns):
            result.append(list2[val])
        df.columns = result

        return df


    def create_crosstab(self,f,frame):#этот метод нужен чтобы делать кросстаб независимо от хитмапов
        slice = getattr(self, "slice_"+self.slice_col1+"_"+self.slice_col2)
        if slice.shape[0] == 0 and slice.shape[1] == 0:
            slice = f(frame)#разрез
            setattr(self, "slice_"+self.slice_col1+"_"+self.slice_col2, slice)

        return slice

    def test_percent_slice(self,df):
        s = df.shape
        for i in range(0, s[0], 1):  # по строкам

            # row_name = all_searches.index[i]
            for j in range(0, s[1], 1):  # по столбцам
                if self.slice_col1 == "brand" and self.slice_col2 == "group":
                    row_name = df.index[i]
                    col_name = df.columns[j]
                    all = self.main_frame[(self.main_frame["brand"] == row_name) & (self.main_frame["group"] == col_name)].shape[0]
                    suc = self.main_frame[(self.main_frame["brand"] == row_name) & (self.main_frame["group"] == col_name)& (self.main_frame["validate_result"] == 1)].shape[0]
                    x =(suc*100)/all
                    if df.iloc[i][j] != x:
                        print([row_name,col_name])

                elif self.slice_col1 == "region" and self.slice_col2 == "group":

                    col_name = df.columns[j]
                    all = self.main_frame[(self.main_frame["group"] == col_name)].shape[0]
                    suc = self.main_frame[(self.main_frame["group"] == col_name) & (self.main_frame["validate_result"] == 1)].shape[0]
                    x = (suc * 100) / all
                    if df.iloc[i][j] != x:
                        print(["Новосибирск", col_name])

                elif self.slice_col1 == "region" and self.slice_col2 == "brand":

                    col_name = df.columns[j]
                    all = self.main_frame[(self.main_frame["brand"] == col_name)].shape[0]
                    suc = self.main_frame[(self.main_frame["brand"] == col_name) & (self.main_frame["validate_result"] == 1)].shape[0]
                    x = (suc * 100) / all
                    if df.iloc[i][j] != x:
                        print(["Новосибирск",col_name])




    def create_heatmap(self,plot_heatmap,f,plot_name, plot_size):#по параметрам будем делать heatmap

        df = self.create_frame_for_plot(f)

        df = self.add_pretty_rows_and_columns_names(df)
        df = df.loc[(df != 0).any(axis=1)]
        df = df.loc[:, (df != 0).any(axis=0)]
        writer = pd.ExcelWriter("slice_" + self.slice_col1 + "_" + self.slice_col2 +"_"+self.prefix+ '.xlsx')
        df.to_excel(writer, 'Sheet1')

        writer.save()

        #self.test_percent_slice(df)

        #можно отфильтровать только удачные из фрейма , затем crosstab по ним, затем приделываем имена колонок и строк и готов разрез в штуках по удачным
        s = self.main_frame.query("Search_result == 1")
        s2 = pd.crosstab(s[self.slice_col1],s[self.slice_col2])
        s2 = s2.loc[(s2 != 0).any(axis=1)]
        s2 = s2.loc[:, (s2 != 0).any(axis=0)]
        s2 = self.add_pretty_rows_and_columns_names(s2)#разрез в абсолютных величинах по удачным

        writer = pd.ExcelWriter("slice_absolute_" + self.slice_col1 + "_" + self.slice_col2 +"_"+self.prefix+  '.xlsx')
        s2.to_excel(writer, 'Sheet1')
        writer.save()


        #plot_heatmap(df, plot_name, plot_size)


    def draw_statistics(self,search_results):
        s = self.main_frame.shape
        print(s[0])  # количество удачных поисков всего

        a1 = search_results.searches_not_standart.groupby('Timestamp', as_index=False)['Brand'].count()
        a2 = search_results.searches_not_standart.groupby('Timestamp', as_index=False)['Group'].count()

        r = pd.merge(a1, a2)
        r.to_csv('two_or_more_brands_or_groups_in_serch_results.csv',index=False)

        # r.groupby("Timestamp",'Brand','Group', as_index=False).count()#смотрим сколько каких сочетаний бренда и групп есть
        # дальше на основе этого будем смотреть что делать дальше!

    def create_frame_for_plot(self,f):

        sliced_searches = self.create_crosstab(f,self.main_frame)#разрез всего  поисков


        percentage_of_sucsess = pd.DataFrame(index=sliced_searches.index, columns=sliced_searches.columns.values)

        s = sliced_searches.shape
        for i in range(0, s[0], 1):  # по строкам
            row = []
            # row_name = all_searches.index[i]
            for j in range(0, s[1], 1):  # по столбцам

                row.append(self.fill_cell_value(sliced_searches,i,j))

            percentage_of_sucsess.iloc[i] = row


        return percentage_of_sucsess

    def fill_cell_value(self, sliced_searches,i,j,list_additional_params=list()):
        #sum = list_additional_params[0]
        row_name = sliced_searches.index[i]
        col_name = sliced_searches.columns[j]
        if sliced_searches.loc[row_name][col_name]==0:
            return 0

        #делаем запрос в main_frame по неудачным поискам в регионе, бренде



        df = self.main_frame[(self.main_frame[self.slice_col1] == row_name) & (self.main_frame[self.slice_col2] == col_name) & (self.main_frame["Search_result"] == 0)]

        sucsess_in_cell = sliced_searches.loc[row_name][col_name] - df.shape[0]#всего - неудачных
        """
        sliced_searches.iloc[i][j](всего в клетке)- 100
        удачных в клетке  -  x%
        """

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

        #percentage_in_cell = (sliced_searches.iloc[i][j] * 100) / (sum + self.not_succsess_searches)

        percentage_in_cell = (sucsess_in_cell * 100) / (sliced_searches.loc[row_name][col_name])

        # if percentage_in_cell>0:
        #   percentage_in_cell = 1

        return percentage_in_cell

class Sales_plots_factory(Search_plots_factory):

    def __init__(self,search_plots_factory,from_db):
        self.prefix = search_plots_factory.prefix
        # сначала отфильтровать поиски завершившиеся продажей Sales==1,
        self.main_frame = search_plots_factory.main_frame

        #if 'Sale' not in search_plots_factory.main_frame.columns:
        #1530813106 - это самый ранний timestamp
        if from_db:
            # делаем запрос на продажи
            df = self.main_frame
            sales = Search_sales(df["Search_uid"], 1530403200)#1530403200 self.first_timestamp_from_query ранний timestamp записали todo когда из файла извлечено то лишняя колонка?
            q = sales.query_make4
            sales.get_data(q,size=500)

            #sales.sales_without_searches.to_csv("sales_without_searches2_"+self.prefix+ ".csv", index=False)
            sales.sales.to_csv("sales2_"+self.prefix+ ".csv", index=False)

            #sales.sales_without_searches = pd.read_csv("sales_without_searches2.csv")
            #sales.sales = pd.read_csv("sales2.csv")


            sales.sales_without_searches = sales.sales_without_searches.groupby(["Search_uid"], as_index=False)["Sale"].agg("count")#todo заменить везде где больше 1 на 1 для продаж законченных без поисков тоже надо группировать!
            sales.sales = sales.sales.groupby(["Search_uid_sales"], as_index=False)["Sale"].agg("count")#todo заменить везде где больше 1 на 1 группируем по id поскольку один товар - это одно оформление заказа в эластике - одно событие(но в корзине может быть несколько товаров)
            sales.sales.loc[sales.sales['Sale'] > 0, 'Sale'] = 1
            sales.sales_without_searches.loc[sales.sales_without_searches['Sale'] > 0, 'Sale'] = 1
            """
                #проверить на уникальность все значения индекса!
            l1 = len(df['Search_uid'].unique())
            l2 = len(sales.sales['Search_uid'].unique())
            if l1 != df.shape[0] or l2 != sales.sales.shape[0]:
                exit(1)
            """
            # удачные с продажей
            # удачные без продажи
            # неудачные с продажей
            # неудачные без продажи
            self.main_frame.isnull().any().any()
            #self.main_frame.set_index('Search_uid')
            #sales.sales.set_index('Search_uid')
            #todo есть неуникальные Search_uid и Search_uid_sales, и продаж без поисков у нас 38000(вне диапазона id) а с поисками 23000
            #todo надо что-то делать с дубликатами - это могут быть поиски по пагинации - вторые и третьи страницы
            #self.frame_all_searches_with_sales = pd.concat([self.main_frame, sales.sales], axis=1, sort=False)#и удачные и неудачные для самопроверки!
            self.frame_all_searches_with_sales =  self.main_frame.merge(sales.sales, how='left', left_on='Search_uid', right_on='Search_uid_sales')
            self.frame_all_searches_with_sales.fillna(0, inplace=True)#в файл
            self.frame_all_searches_with_sales.to_csv("frame_all_searches_with_sales_"+self.prefix+ ".csv",index=False)#frame_all_searches_with_sales.csv

            self.sales_without_searches = sales.sales_without_searches#продажи вне диапазона id поисков, у них цепочки не начались с поисков!
            #self.sales_without_searches.to_csv("sales_without_searches_"+self.prefix+ ".csv",index=False)
        else:
            self.frame_all_searches_with_sales = pd.read_csv("frame_all_searches_with_sales_"+self.prefix+ ".csv")#frame_all_searches_with_sales.csv
            # продажи без поисков вообще
            self.sales_without_searches = pd.read_csv("sales_without_searches_"+self.prefix+ ".csv")


        # удачные с продажей
        self.frame_sucsess_searches_with_sales = self.frame_all_searches_with_sales.query("Search_result == 1 & Sale == 1")#чтобы это здесь было нужно чтобы в файле уже было проведено объединение
        # удачные без продажи
        self.frame_sucsess_searches_without_sales = self.frame_all_searches_with_sales.query("Search_result == 1 & Sale == 0")
        # неудачные с продажей
        self.frame_not_sucsess_searches_with_sales = self.frame_all_searches_with_sales.query("Search_result == 0 & Sale == 1")
        # неудачные без продажи
        self.frame_not_sucsess_searches_without_sales = self.frame_all_searches_with_sales.query("Search_result == 0 & Sale == 0")




        # cross tab срез по 3 м ситуациям, только для поисков завершенных продажей
        self.slice_region_brand = pd.DataFrame()
        self.slice_region_group = pd.DataFrame()
        self.slice_brand_group = pd.DataFrame()

        self.region_dict = search_plots_factory.region_dict
        self.brand_dict = search_plots_factory.brand_dict
        self.group_dict = search_plots_factory.group_dict




    def get_statistics_of_serches_and_sales(self):
        #% поисков с продажами - мы сравниваем только эту часть по продажам
        #% продаж без поисков
        #% поисков без продаж
        #% не было выдачи + не было продажи(в корзину положили но не купили?)

        #удачные с продажей
        #удачные без продажи
        #неудачные с продажей
        #неудачные без продажи

        #продажа без поисков

        #пока только 3 типа
        #without_sales = self.frame_searches_with_sales.query("Sale == 0")
        #all_chains = self.sales_without_searches.shape[0] + without_sales.shape[0] + self.frame_searches_with_sales_only.shape[0]
        all_chains = self.frame_sucsess_searches_with_sales.shape[0] + self.frame_sucsess_searches_without_sales.shape[0] + self.frame_not_sucsess_searches_with_sales.shape[0] + self.frame_not_sucsess_searches_without_sales.shape[0] + self.sales_without_searches.shape[0]

        percent1 = (self.frame_sucsess_searches_with_sales.shape[0] * 100) / all_chains # удачные с продажей
        percent2 = (self.frame_sucsess_searches_without_sales.shape[0] * 100) / all_chains# удачные без продажи
        percent3 = (self.frame_not_sucsess_searches_with_sales.shape[0] * 100) / all_chains# неудачные с продажей
        percent4 = (self.frame_not_sucsess_searches_without_sales.shape[0] * 100) / all_chains# неудачные без продажи
        percent5 = (self.sales_without_searches.shape[0] * 100) / all_chains# продаж без поисков вообще



        #percent1 = (self.sales_without_searches.shape[0]*100)/all_chains #% продаж без поисков.
        #percent2 = (without_sales.shape[0]*100)/all_chains#% поисков без продаж
        #percent3 = (self.frame_searches_with_sales_only.shape[0]*100)/all_chains#% поисков с продажами

        labels = 'удачные с продажей', 'удачные без продажи', 'неудачные с продажей','неудачные без продажи','продаж без поисков(с нового сайта) вообще'
        fracs = [percent1, percent2, percent3,percent4,percent5] #[2548, 8355, 2578, 28793, 61719] [159, 507, 3075, 24178, 71212]
        # [5009, 13088, 832, 4769, 111135] - это продажи по матрице региона(НСК и питер)
        plt.pie(fracs, labels=labels, autopct='%1.1f%%', shadow=False)
        plt.title('Виды цепочек действий пользователей')
        plt.show()

        print([self.frame_sucsess_searches_with_sales.shape[0],
               self.frame_sucsess_searches_without_sales.shape[0],
               self.frame_not_sucsess_searches_with_sales.shape[0],
               self.frame_not_sucsess_searches_without_sales.shape[0],
               self.sales_without_searches.shape[0]
               ])
        """
        values = np.arange(0, 100, step=5)
        colors = plt.cm.BuPu(np.linspace(0, 0.5, 3))

        plt.bar(np.arange(1), [], 0.4, color=colors[0])
        plt.ylabel("%")

        plt.yticks(values)
        plt.xticks([])
        plt.title('Виды цепочек действий пользователей')

        plt.show()
        """


    def create_frame_for_plot(self,f):

        #sliced_searches = self.create_crosstab(f,self.frame_all_searches_with_sales.query("Search_result == 1"))#разрез всего удачных поисков
        sliced_searches = f(self.frame_all_searches_with_sales.query("Search_result == 1"))  #разрез всего удачных поисков


        percentage_of_sucsess = pd.DataFrame(index=sliced_searches.index, columns=sliced_searches.columns.values)

        s = sliced_searches.shape
        for i in range(0, s[0], 1):  # по строкам
            row = []
            # row_name = all_searches.index[i]
            for j in range(0, s[1], 1):  # по столбцам

                row.append(self.fill_cell_value(sliced_searches,i,j))

            percentage_of_sucsess.iloc[i] = row


        return percentage_of_sucsess

    def create_heatmap(self,plot_heatmap, f, plot_name, plot_size):  # по параметрам будем делать heatmap
        #переопредедяем этот метод чтобы иметь отдельный кросстабы по срезам! они нужны для расчетов %
        #можно и делать по региону - бренду запросы в self.frame_searches_with_sales_only  для расчета % в функции fill_cell_value(для подсчета количества поисков в ячейке завершенных продажей)- но это может быть дольше

        self.create_crosstab(f, self.frame_sucsess_searches_with_sales)  #делаем слайс по удачным поискам завершенным продажей, сохраняем в атрибут - этот % мы и должны считать от общего числа удачных поисков

        df = self.create_frame_for_plot(f)

        df = self.add_pretty_rows_and_columns_names(df)
        df = df.loc[(df != 0).any(axis=1)]
        df = df.loc[:, (df != 0).any(axis=0)]
        writer = pd.ExcelWriter("slice_sales_" + self.slice_col1 + "_" + self.slice_col2 + "_"+self.prefix+ '.xlsx')
        df.to_excel(writer, 'Sheet1')

        writer.save()
        # можно отфильтровать только удачные из фрейма , затем crosstab по ним, затем приделываем имена колонок и строк и готов разрез в штуках по удачным
        s = self.frame_sucsess_searches_with_sales# в абсолютных числах происки завершенные продажей
        s2 = pd.crosstab(s[self.slice_col1], s[self.slice_col2])
        s2 = self.add_pretty_rows_and_columns_names(s2)  # разрез в абсолютных величинах по удачным
        s2 = s2.loc[(s2 != 0).any(axis=1)]
        s2 = s2.loc[:, (s2 != 0).any(axis=0)]
        writer = pd.ExcelWriter("slice_sales_absolute_" + self.slice_col1 + "_" + self.slice_col2 +"_"+self.prefix+  '.xlsx')
        s2.to_excel(writer, 'Sheet1')
        writer.save()

        #plot_heatmap(df, plot_name, plot_size)


    def fill_cell_value(self, sliced_searches, i, j, list_additional_params=list()):#логика для каждой ячейки будет описана здесь!
        row_name = sliced_searches.index[i]
        col_name = sliced_searches.columns[j]

        slice = getattr(self, "slice_"+self.slice_col1+"_"+self.slice_col2)#получаем разрез с удачными поисками завершенными продажей из этого класса

        percentage_of_saled_searches_in_cell = 0

        if row_name in slice.index and col_name in slice.columns:#если есть то в поисках завершенных продажей такая строка колонк аесть

            if sliced_searches.loc[int(row_name)][int(col_name)] == 0:#срез где все удачные поиски!
                percentage_of_saled_searches_in_cell = 0

                #assert (slice.loc[row_name][col_name] == 0),"Slice "+"slice_"+self.slice_col1+"_"+self.slice_col2+" Not equal values in crosstab of all searches and in crosstab of serches ended with sales"
            else:
                percentage_of_saled_searches_in_cell = slice.loc[row_name][col_name] / sliced_searches.loc[row_name][col_name]

                #assert (slice.loc[row_name][col_name] != 0), "Slice " + "slice_"+self.slice_col1+"_"+self.slice_col2 + " Not equal values in crosstab of all searches and in crosstab of serches ended with sales"

        return percentage_of_saled_searches_in_cell

        #сверяем каждую матрицу с разрезом по удачным поискам с соответствующей матрицей поисков завершенных продажей!
        #тут нужен проход по всем ячейкам фрейма с поиском и проверка существования соответствующих ячеек во фрейме с поисками!


        #для сравнения поисков с проажами, надо проходя по фрейму с продажами , проверять существование ячейки в фрейме поисков, и проверять что значение по этому индексу >0
        #так мы проверим только часть поисков завершенных продажей, остальные без продаж, значит не проверены



def region_brand(frame):

    all_searches = pd.crosstab(frame["region"], frame["brand"])  # таблица по всем поискам

    #sucsess_searches = pd.crosstab(df_sucsess_searches.Region, df_sucsess_searches.Brand)  # таблица по удачным поискам
    #writer = pd.ExcelWriter('output.xlsx')
    #all_searches.to_excel(writer, 'Sheet1')

    #writer.save()
    # добавить нормальные имена строк и колонок в соответствии с их id
    all_searches = all_searches.loc[(all_searches != 0).any(axis=1)]
    all_searches = all_searches.loc[:, (all_searches != 0).any(axis=0)]


    return all_searches

def region_group(frame):

    all_searches = pd.crosstab(frame["region"], frame["group"])  # таблица по всем поискам
    #sucsess_searches = pd.crosstab(df_sucsess_searches.Region, df_sucsess_searches.Group)  # таблица по удачным поискам
    all_searches = all_searches.loc[(all_searches != 0).any(axis=1)]
    all_searches = all_searches.loc[:, (all_searches != 0).any(axis=0)]
    return all_searches


def brand_group(frame):

    all_searches = pd.crosstab(frame["brand"], frame["group"])  # таблица по всем поискам
    #sucsess_searches = pd.crosstab(df_sucsess_searches.Brand, df_sucsess_searches.Group)  # таблица по удачным поискам
    all_searches = all_searches.loc[(all_searches != 0).any(axis=1)]
    all_searches = all_searches.loc[:, (all_searches != 0).any(axis=0)]
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