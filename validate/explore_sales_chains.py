from elastic.elastic_queries_new_logic import Base_Elastic
from elastic.elastic_queries_new_logic import query_make4
import pandas as pd

class Sales_db_worker(Base_Elastic):

    def __init__(self,t):
        self.from_timestamp = t
        self.list_of_sales_ids={}
        self.list_of_sales_ids2={}

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
            else:
                self.list_of_sales_ids2[sale["_source"]["search_uid"]] = 1#дубликат

    def do_logic_short(self, list_of_elements):
        self.do_logic(list_of_elements)

    def json_begin_file(self):
        return

    def json_end_file(self):
        return
class Serches_in_sales_chains(Base_Elastic):
    def __init__(self,t,list_of_sales_ids,goods_classifier,brands_dict,groups_dict):
        self.from_timestamp = t
        self.list_of_sales_ids = list_of_sales_ids

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

        self.goods_classifier = goods_classifier
        self.brands_dict = brands_dict
        self.groups_dict = groups_dict


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
                        "terms": {
                            "search_uid": list(self.list_of_sales_ids)
                        }
                    }

                ]

            }
        }

        return query

    def do_logic(self, list_of_elements):
        #todo здесь мы должны отслеживать както по всем типам событий serch в цепочках



        length = len(list_of_elements)
        for i in range(0, length, 1):

            hit = list_of_elements[i]["_source"]
            timestamp = hit["timestamp"]
            search_query = hit["search_query"]
            search_uid = hit["search_uid"]
            page = hit["page_number"]

            #счетчики для определения принадлежности выдачи к типу

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
                                brand = element_of_search_results["brand_name"].replace(r"'", "").replace("\\", "")
                                group = element_of_search_results["market_group_name"]

                                # совпадение по бренду/товарной группе из поисковой строки

                                if brand != "" and group != "" and self.check_presence_in_list(search_query, brand,group) == True:  # есть в списке сочетаний брендов и групп по этому номеру товара
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

    def check_presence_in_list(self,search_query, brand, group):
        #hex = str(hash(brand+group))
        if search_query in self.goods_classifier:
            #if brand in self.goods_classifier[search_query] and self.goods_classifier[search_query][brand]==group:#совпадает по бренду и товарной группе
            #входит ли сочетание бренд/группа в список сочетаний под этим номером
            if brand in self.brands_dict and group in self.groups_dict and (self.brands_dict[brand],self.groups_dict[group]) in self.goods_classifier[search_query]:
                return True
        return False

    def do_logic_short(self, list_of_elements):
        self.do_logic(list_of_elements)

    def json_begin_file(self):
        return

    def json_end_file(self):
        return


class Explore_sales_chains():
    index = 'cart_item_event'
    timestampe_field_name = "timestamp"

    def get_sales_events(self,t,goods_classifier,brands_dict,groups_dict):

        search_results = Sales_db_worker(t)
        q = query_make4
        search_results.get_data(q, self.index, self.timestampe_field_name)

        #имеем список id
        search_events_worker = Serches_in_sales_chains(t,search_results.list_of_sales_ids,goods_classifier,brands_dict,groups_dict)
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
        #todo надо делать обнаружение совпадения по NSI а не по брендам и группам(подключить справочник)
        #search_results.list_of_sales_ids должен совпадать по количеству строк с сгруппированным списком поисков

        #todo сгруппировать одинаковые id и артикулы (пагинация поиска)
        f = search_events_worker.types_of_results
        #сколько выдач когда нет региона
        x1 = f[(f["have_region"] == 0)].shape[0]
        #f[(f["have_region"] == 0) & (f[self.slice_col2] == col_name) & (f["Search_result"] == 0)]

        #сколько выдач когда есть регион
        x2 = f[(f["have_region"] > 0)].shape[0]
            #из них когда results_groups > 0
        x3 = f[(f["have_region"] > 0) & (f["results_groups"]>0)].shape[0]
                #из них когда search_results >0
        x5 = f[(f["have_region"] > 0) & (f["results_groups"] > 0) & (f["search_results"] > 0)].shape[0]
                    #из них когда availably > 0
        x7 = f[(f["have_region"] > 0) & (f["results_groups"] > 0) & (f["search_results"] > 0) & (f["availably"] > 0)].shape[0]
                        #из них когда brand_group_have_names > 0
        x9 = f[(f["have_region"] > 0) & (f["results_groups"] > 0) & (f["search_results"] > 0) & (f["availably"] > 0) & (f["brand_group_have_names"] > 0)].shape[0]
                            #из них когда query_dictionary_brand_group_overlap > 0
        x11 = f[(f["have_region"] > 0) & (f["results_groups"] > 0) & (f["search_results"] > 0) & (f["availably"] > 0) & (f["brand_group_have_names"] > 0) & (f["query_dictionary_brand_group_overlap"] > 0)].shape[0]
                                #из них когда local_store > 0
        x13 = f[(f["have_region"] > 0) & (f["results_groups"] > 0) & (f["search_results"] > 0) & (f["availably"] > 0) & (f["brand_group_have_names"] > 0) & (f["query_dictionary_brand_group_overlap"] > 0) & (f["local_store"] > 0)].shape[0]
                                    #из них когда succsess_searches_count > 0
        x15 = f[(f["have_region"] > 0) & (f["results_groups"] > 0) & (f["search_results"] > 0) & (f["availably"] > 0) & (f["brand_group_have_names"] > 0) & (f["query_dictionary_brand_group_overlap"] > 0) & (f["local_store"] > 0) & (f["succsess_searches_count"] > 0)].shape[0]
                                    #из них когда succsess_searches_count == 0
        x16 = f[(f["have_region"] > 0) & (f["results_groups"] > 0) & (f["search_results"] > 0) & (f["availably"] > 0) & (f["brand_group_have_names"] > 0) & (f["query_dictionary_brand_group_overlap"] > 0) & (f["local_store"] > 0) & (f["succsess_searches_count"] == 0)].shape[0]
                                #из них когда local_store == 0
        x14 = f[(f["have_region"] > 0) & (f["results_groups"] > 0) & (f["search_results"] > 0) & (f["availably"] > 0) & (f["brand_group_have_names"] > 0) & (f["query_dictionary_brand_group_overlap"] > 0) & (f["local_store"] == 0)].shape[0]
                            #из них когда query_dictionary_brand_group_overlap == 0
        x12 = f[(f["have_region"] > 0) & (f["results_groups"] > 0) & (f["search_results"] > 0) & (f["availably"] > 0) & (f["brand_group_have_names"] > 0) & (f["query_dictionary_brand_group_overlap"] == 0)].shape[0]
                        #из них когда brand_group_have_names == 0
        x10 = f[(f["have_region"] > 0) & (f["results_groups"] > 0) & (f["search_results"] > 0) & (f["availably"] > 0) & (f["brand_group_have_names"] == 0)].shape[0]
                    #из них когда availably == 0
        x8 = f[(f["have_region"] > 0) & (f["results_groups"] > 0) & (f["search_results"] > 0) & (f["availably"] == 0)].shape[0]
                #из них когда search_results ==0
        x6 = f[(f["have_region"] > 0) & (f["results_groups"] > 0) & (f["search_results"] == 0)].shape[0]
            #из них когда results_groups ==0
        x4 = f[(f["have_region"] > 0) & (f["results_groups"]==0)].shape[0]



