from elastic.elastic_queries_new_logic import Base_Elastic
from elastic.elastic_queries_new_logic import query_make4
import pandas as pd
from frame_partial_reader.frame_partial_reader import Frame_partial_reader
import json

class Sales_db_worker(Base_Elastic):

    def __init__(self,t):
        self.from_timestamp = t
        self.list_of_sales_ids={}
        #self.list_of_sales_ids2={}

    def get_query(self,q,list_of_args=list()):

        list_of_args.append(str(self.from_timestamp))

        query = q(list_of_args)
        return query

    def do_logic(self, list_of_elements):
        length = len(list_of_elements)
        for i in range(0, length, 1):
            sale = list_of_elements[i]

            #if sale["_source"]["search_uid"] not in self.list_of_sales_ids:
            self.list_of_sales_ids[sale["_source"]["search_uid"]]=1
           # else:
                #self.list_of_sales_ids2[sale["_source"]["search_uid"]] = 1#дубликат

    def do_logic_short(self, list_of_elements):
        self.do_logic(list_of_elements)

    def json_begin_file(self):
        return

    def json_end_file(self):
        return
class Serches_in_sales_chains(Base_Elastic):
    def __init__(self,t,list_of_sales_ids):
        self.from_timestamp = t
        self.list_of_sales_ids = list_of_sales_ids
        self.search_in_sale = 0
        self.search_not_in_sale = 0
        self.types_of_results = pd.DataFrame(columns=[
            'search_uid',
            'query',
            'page',
            'have_region',
            'results_groups',
            'search_results',
            'availably',
            'brand_group_have_names',
            'query_dictionary_brand_group_overlap',
            'local_store',
            'succsess_searches_count'
        ])

        #self.goods_classifier = goods_classifier
        pr = Frame_partial_reader()
        pr.read("parts_with_sg_mg.csv")

        self.goods_classifier = pr.parse_result

        self.brands_dict = pr.brands
        self.groups_dict = pr.groups
        self.searc_uids = {}

    def get_query(self,q,list_of_args=list()):

        list_of_args.append(str(self.from_timestamp))

        query = q(list_of_args)
        return query
    def query_for_searches(self,list_of_args):


        if len(list_of_args)==1:
            gt = list_of_args[0]
        else:
            gt = list_of_args[0]
        query = {
            "bool": {
                "must": [
                    {"range": {"timestamp": {"gt": gt, "lt": 1533081600}}},
                    {
                        "match": {
                            "region": "Новосибирск"
                        }
                    },
                    {
                        "exists": {"field": "search_uid"}
                    }

                ],
                "must_not": [
                    {
                        "exists": {"field": "page_url"}
                    }

                ]
            }
        }
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
                        "match": {
                            "region": "Новосибирск"
                        }
                    },


                    {
                        "exists": {"field": "results_groups"}
                    },
                    {
                        "terms": {
                            "search_uid": list(self.list_of_sales_ids)
                        }
                    },

                ]

            }
        }

        return query

    """
                        {
                            "terms": {
                                "search_uid": list(self.list_of_sales_ids)
                            }
                        },
    """
    def do_logic(self, list_of_elements):
        #todo здесь мы должны отслеживать както по всем типам событий serch в цепочках



        length = len(list_of_elements)
        for i in range(0, length, 1):

            hit = list_of_elements[i]["_source"]
            timestamp = hit["timestamp"]
            search_query = hit["search_query"]

            search_uid = hit["search_uid"]
            page = hit["page_number"]


            have_region = 0
            results_groups = 0
            search_results = 0
            availably = 0
            brand_group_have_names = 0
            query_dictionary_brand_group_overlap = 0
            local_store = 0
            succsess_searches_count = 0 #не ТСП

            # brand_from_query = self.get_from_query(search_query)

            # список сочетаний брендов-товарных групп

            if "region" in hit:
                region = hit["region"]
                have_region += 1
                #if region not in self.regions_dict:
                #    self.regions_dict[region] = len(self.regions_dict)

                length2 = len(hit["results_groups"])

                # надо узнать статистику случаев когда больше двух брендов в результате выдачи
                # надо записывать бренды

                # succsess_searches_count2 = 0
                # brands = list()
                # market_groups = list()
                brand_group_variants = {}

                empty_result = True


                if length2 == 0:
                    results_groups += 1


                for k in range(0, length2,1):  # по всем элементам results_groups, в каждом элементе один атрибут search_results
                    results_group_item = hit["results_groups"][k]

                    length3 = len(results_group_item["search_results"])

                    if length3==0:
                        search_results += 1


                    for j in range(0, length3, 1):
                        element_of_search_results = results_group_item["search_results"][j]
                        empty_result = False

                        # отсутствующая в наличии запчасть это неудачный поиск!
                        if "part" in element_of_search_results:
                            availably +=1
                            if "brand_name" in element_of_search_results and "market_group_name" in element_of_search_results:
                                brand_group_have_names += 1
                                #brand = element_of_search_results["brand_name"].replace(r"'", "").replace("\\", "")
                                #group = element_of_search_results["market_group_name"]
                                nsi = element_of_search_results["link"].split("=")[1]  # из ссылки извлекаем
                                # совпадение по бренду/товарной группе из поисковой строки
                                res = self.check_presence_in_list(search_query, nsi)

                                #if brand != "" and group != "" and res[0] == True:  # есть в списке сочетаний брендов и групп по этому номеру товара
                                if res[0] == True:  # есть в списке сочетаний брендов и групп по этому номеру товара
                                    query_dictionary_brand_group_overlap +=1

                                    if element_of_search_results["part"]["is_local_delivery"] == True:  # только с локального склада
                                        local_store+=1
                                        if element_of_search_results["part"]["is_approximate_delivery_interval"] == False:  # не ТСП и ТСП не заказывали для запроса
                                            # все кроме ТСП, свои товары
                                            succsess_searches_count += 1



            self.types_of_results.loc[len(self.types_of_results)] = [
                search_uid,
                search_query,
                page,
                have_region,
                results_groups,
                search_results,
                availably,
                brand_group_have_names,
                query_dictionary_brand_group_overlap,
                local_store,
                succsess_searches_count
            ]

    def check_presence_in_list_2(self,search_query, brand, group):
        #hex = str(hash(brand+group))
        if search_query in self.goods_classifier:
            #if brand in self.goods_classifier[search_query] and self.goods_classifier[search_query][brand]==group:#совпадает по бренду и товарной группе
            #входит ли сочетание бренд/группа в список сочетаний под этим номером
            if brand in self.brands_dict and group in self.groups_dict and (self.brands_dict[brand],self.groups_dict[group]) in self.goods_classifier[search_query]:
                return True
        return False

    def check_presence_in_list(self, search_query, nsi):
        # todo артикулы в справочнике могут не совпадать!
        # надо пробовать по разному - сырой, без пробелов, без дефисов
        # бренды в elastic не совпадают с брендами в справочнике NSI поэтому
        # надо получить по артикулу список NSI и проверить присутствие
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

        return [result, brand_group_combination]  # список совпавших будет возвращять!

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
    def do_logic_short(self, list_of_elements):
        self.do_logic(list_of_elements)

    def json_begin_file(self):
        return

    def json_end_file(self):
        return


class Explore_sales_chains():
    index = 'cart_item_event'
    timestampe_field_name = "timestamp"
    main_frame = pd.DataFrame()
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

    def query_make4(self,list_of_args):
        if len(list_of_args) == 1:
            gt = list_of_args[0]  # базовый timestamp будет добавлять
            range = {"timestamp": {"gt": gt,"lt": 1533081600}}
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
                        "match": {
                            "region": "Новосибирск"
                        }
                    }
                ]
            }
        }
        return query
    def get_sales_events(self,t):

        #search_results = Sales_db_worker(t)
        #q = self.query_make4
        #search_results.get_data(q, self.index, self.timestampe_field_name)
        #1 файл со списком id продаж, начавшихся с поиска
        #sales = pd.read_csv("sales2.csv")
        #l = search_results.list_of_sales_ids
        #with open('ids_of_sales.json', 'w') as fp:
        #    json.dump(l, fp)
        with open('ids_of_sales.json') as data_file:
            l = json.load(data_file)

        #имеем список id
        search_events_worker = Serches_in_sales_chains(t,l)#достаем все поиски у которых была продажа в конце(оформление заказа)
        q = search_events_worker.query_for_searches
        search_events_worker.get_data(q, self.index, self.timestampe_field_name)


        #search_events_worker.types_of_results

        """
        have_region,
        results_groups,
        search_results,
        availably,
        brand_group_have_names,
        query_dictionary_brand_group_overlap,
        local_store,
        succsess_searches_count
        """

        #search_results.list_of_sales_ids должен совпадать по количеству строк с сгруппированным списком поисков

        #todo сгруппировать одинаковые id и артикулы (пагинация поиска)
        #11246 выдач
        #64219 событий оформления заказа
        #self.main_frame = search_events_worker.types_of_results
        #self.collapse_rows_from_one_pagination_chain()
        #f = search_events_worker.types_of_results
        #f.to_csv("searches.csv")
        f = pd.read_csv("searches.csv")

        #сколько выдач когда нет региона
        x1 = f[f["have_region"] == 0].shape[0]
        #f[(f["have_region"] == 0) & (f[self.slice_col2] == col_name) & (f["Search_result"] == 0)]

        #сколько выдач когда есть регион
        #x2 = f[f["have_region"] > 0].shape[0]
            #из них когда results_groups > 0
        #x3 = f[f["have_region"] > 0].shape[0]
                #из них когда search_results >0
        #x5 = f[f["have_region"] > 0].shape[0]
                    #из них когда availably > 0
        x7 = f[(f["have_region"] > 0) & (f["availably"] > 0)].shape[0]
                        #из них когда brand_group_have_names > 0
        x9 = f[(f["have_region"] > 0)  & (f["availably"] > 0) & (f["brand_group_have_names"] > 0)].shape[0]
                            #из них когда query_dictionary_brand_group_overlap > 0
        x11 = f[(f["have_region"] > 0)  & (f["availably"] > 0) & (f["brand_group_have_names"] > 0) & (f["query_dictionary_brand_group_overlap"] > 0)].shape[0]
                                #из них когда local_store > 0
        x13 = f[(f["have_region"] > 0)  & (f["availably"] > 0) & (f["brand_group_have_names"] > 0) & (f["query_dictionary_brand_group_overlap"] > 0) & (f["local_store"] > 0)].shape[0]
                                    #из них когда succsess_searches_count > 0
        x15 = f[(f["have_region"] > 0)  & (f["availably"] > 0) & (f["brand_group_have_names"] > 0) & (f["query_dictionary_brand_group_overlap"] > 0) & (f["local_store"] > 0) & (f["succsess_searches_count"] > 0)].shape[0]
                                    #из них когда succsess_searches_count == 0
        x16 = f[(f["have_region"] > 0)  & (f["availably"] > 0) & (f["brand_group_have_names"] > 0) & (f["query_dictionary_brand_group_overlap"] > 0) & (f["local_store"] > 0) & (f["succsess_searches_count"] == 0)].shape[0]
                                #из них когда local_store == 0
        x14 = f[(f["have_region"] > 0)  & (f["availably"] > 0) & (f["brand_group_have_names"] > 0) & (f["query_dictionary_brand_group_overlap"] > 0) & (f["local_store"] == 0)].shape[0]
                            #из них когда query_dictionary_brand_group_overlap == 0
        x12 = f[(f["have_region"] > 0) & (f["availably"] > 0) & (f["brand_group_have_names"] > 0) & (f["query_dictionary_brand_group_overlap"] == 0)].shape[0]
                        #из них когда brand_group_have_names == 0
        x10 = f[(f["have_region"] > 0)  & (f["availably"] > 0) & (f["brand_group_have_names"] == 0)].shape[0]
                    #из них когда availably == 0
        x8 = f[(f["have_region"] > 0)  & (f["availably"] == 0)].shape[0]
                #из них когда search_results ==0
        #x6 = f[(f["have_region"] > 0)].shape[0]
            #из них когда results_groups ==0
        #x4 = f[(f["have_region"] > 0) & (f["results_groups"]==0)].shape[0]

        a = 1




#explore = Explore_sales_chains()
#explore.get_sales_events(1530403200)



main_frame = pd.read_csv('out.csv')
f = pd.read_csv('searches.csv')
finded = 0
rows = main_frame.shape[0]
for row in range(0,rows, 1):

    if f[f["search_uid"]==main_frame.iloc[row][0]].shape[0]>0:
        finded +=1

a = 1



