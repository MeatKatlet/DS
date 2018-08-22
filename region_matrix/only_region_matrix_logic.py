from elastic.elastic_queries_new_logic import Search_plots_factory
from elastic.elastic_queries_new_logic import Search_results_events
from elastic.elastic_queries_new_logic import Base_Elastic
import json
import pandas as pd
import math
import psutil
import os
import csv
import pickle
from region_matrix.Filter_fabric import Filter_fabric

def memory_usage_psutil():
    # return the memory usage in MB
    process = psutil.Process(os.getpid())
    mem = process.memory_full_info()[12] / float(2 ** 20)
    return mem
class Only_matrix_Search_results_events(Search_results_events):
    #nsi_not_in_matrix = False

    def add_matrix_dictionary_to_class(self, filter=dict):

        self.region_article ={}
        #self.region_article["Новосибирск"] = {}
        #self.region_article["Санкт-Петербург"] = {}
        self.region_nsi ={}
        """
        #self.region_nsi["Новосибирск"] ={}
        #self.region_nsi["Санкт-Петербург"] ={}
        #names=["Nom_Name","Nom_Code","Artikul","SKlad_Code","SKlad_Name","CFO_Code","CFO_Name","NomGr","MarkGr","SinMarkGr","Brand"]
        #matrix =
        matrix = pd.read_csv("matrixNom.csv", sep=';')
        #matrix = matrix[matrix["CFO_Name"]=="Новосибирск",matrix["CFO_Name"]=="Санкт-Петербург"]
        #array = ["Новосибирск", "Санкт-Петербург"]

        array = ["Новосибирск"]
        #self.matrix = matrix.loc[matrix["CFO_Name"].isin(array)]
        rows = matrix.shape[0]
        for row in range(0,rows,1):
            line = matrix.iloc[row]
            if line[6] not in self.region_article:
                self.region_article[line[6]] = {}
                self.region_article[line[6]][line[2]] = 1
                self.region_nsi[line[6]] = {}
                self.region_nsi[line[6]][line[1]] = 1
            else:
                self.region_article[line[6]][line[2]] = 1
                self.region_nsi[line[6]][line[1]] = 1


        del matrix
        a=1
        print(memory_usage_psutil())
        """
        path = "region_matrix_pure.csv"


        #todo проход по файлу ! - подготовить файл - убрать все имена! - первая колонка
        fp = open(path, encoding='utf-8')

        prev_line = ''
        for i, line in enumerate(fp):

            if line[:3] == 'NSI':

                if prev_line == "":
                    prev_line = line
                    continue
                splitted  = list(csv.reader([prev_line],delimiter=';'))
                nsi = splitted[0][0]  # фильтровать!
                article = splitted[0][1]  # фильтровать!

                region = splitted[0][2]

                if region not in filter["regions"] or article not in filter["articules"] or nsi not in filter["nsi"]:
                    prev_line = line
                    continue

                if region not in self.region_article:
                    self.region_article[region] = {}
                    self.region_article[region][article] = 1
                    self.region_nsi[region] = {}
                    self.region_nsi[region][nsi] = 1
                else:
                    self.region_article[region][article] = 1
                    self.region_nsi[region][nsi] = 1


                prev_line = line
            else:
                prev_line = prev_line + line
                prev_line = ""
                continue

        fp.close()





    def query_make3(self,list_of_args):
        if len(list_of_args) == 1:
            gt = list_of_args[0]  # базовый от 1 июля
        else:
            gt = list_of_args[0]
        query = {
            "bool": {
                "must": [
                    {"range": {"timestamp": {"gt": gt, "lt": 1533081600}}},

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
                        "exists": {"field": "results_groups.search_results.brand_name"}
                    },
                    {
                        "exists": {"field": "results_groups.search_results.market_group_name"}
                    },
                    {
                        "terms": {
                            "region": ["Новосибирск"]
                        }
                    }
                ]

            }
        }
        return query
    def check_exist_in_nsi(self,nsi,region):#todo по ключу надо чтобы было O(1)
        #l = self.matrix[(self.matrix["CFO_Name"]==region)& (self.matrix["Nom_Code"]==nsi)].shape[0]
        if nsi in self.region_nsi[region]:
            return True
        return False

    def check_variants_articule(self,search_query,region):
        raw = search_query
        without_spaces = search_query.replace(" ", "")
        without_spaces_and_defises = without_spaces.replace("-", "")
        without_dies_suffix = without_spaces_and_defises.split("#")[0]
        result = False
        q= 0
        if raw in self.region_article[region]:

            result = True
            q = raw
        elif without_spaces in self.region_article[region]:

            result = True
            q = without_spaces
        elif without_spaces_and_defises in self.region_article[region]:

            result = True
            q = without_spaces_and_defises


        elif without_dies_suffix in self.region_article[region]:

            result = True
            q = without_dies_suffix

        return [result,q]

    def do_logic(self, list_of_elements):
        # list_of_elements - это список объектов из объекта hits в результате выдачи
        # здесь будем фильтровать нужные мне результаты выдачи и сохранять во фрейм
        length = len(list_of_elements)-1
        for i in range(0, length, 1):

            hit = list_of_elements[i]["_source"]

            if "search_query" not in hit:
                continue
            search_query = hit["search_query"]
            #todo search_query надо 1 раз причесать чтобы не делать это много раз и

            search_uid = hit["search_uid"]
            #page = hit["page_number"]


            succsess_searches_count = 0


            if "region" in hit:
                region = hit["region"]
                #have_region += 1
                qres = self.check_variants_articule(search_query,region)

                if qres[0] == False:
                    continue

                self.count2 += 1#сколько запросов по товарной матрице региона?
                #23720 номеров деталей в запросах из товарной матрицы региона
                if region not in self.regions_dict:
                   self.regions_dict[region] = len(self.regions_dict)

                length2 = len(hit["results_groups"])
                #if length2 == 0:
                    #results_groups += 1

                #надо узнать статистику случаев когда больше двух брендов в результате выдачи
                #надо записывать бренды
                succsess_searches_count = 0
                #succsess_searches_count2 = 0

                brand_group_variants = {}

                empty_result = True
                test_search = False

                for k in range(0, length2, 1):#по всем элементам results_groups, в каждом элементе один атрибут search_results
                    results_group_item = hit["results_groups"][k]

                    length3 = len(results_group_item["search_results"])
                    #if length3==0:
                        #search_results += 1

                    for j in range(0, length3, 1):
                        element_of_search_results = results_group_item["search_results"][j]


                        link_parts = element_of_search_results["link"].split("=")
                        nsi = link_parts[1]  # из ссылки извлекаем
                        # совпадение по бренду/товарной группе из поисковой строки
                        if len(link_parts) > 2:
                            a = 1

                        if self.check_exist_in_nsi(nsi, region) == False:
                            continue  # пропускаем его как будто его нет, т.к. он не в матрице региона

                        empty_result = False

                        res = self.check_presence_in_list(search_query, nsi)  # [result, brand_group_combination]

                        if res[0] == True and res[1] not in brand_group_variants:
                            brand_group_variants[res[1]] = {"available":0,"carts":0,"cards":0,"preview":0}
                        if res[0] == True:

                            test_search = True
                        # отсутствующая в наличии запчасть это неудачный поиск!
                        if "part" in element_of_search_results:
                            #availably += 1

                            #brand = element_of_search_results["brand_name"]#.replace(r"'", "").replace("\\","")
                            #group = element_of_search_results["market_group_name"]


                            if res[0] == True:#есть в списке сочетаний брендов и групп по этому номеру товара
                                #query_dictionary_brand_group_overlap += 1

                                #search_result = 1  # удачный поиск
                                #cross = "cross" in element_of_search_results

                                if element_of_search_results["part"]["is_local_delivery"] == True:  # только с локального склада
                                    #local_store += 1

                                    if element_of_search_results["part"]["is_approximate_delivery_interval"] == False and self.with_tcp == False:  # не ТСП и ТСП не заказывали для запроса
                                        # все кроме ТСП, свои товары
                                        succsess_searches_count += 1

                                        #res[1] - это tuple из комбинации кодов (бренд, группа)


                                        brand_group_variants[res[1]]["available"] = 1#есть наличие в этой группе выдачи



                                    elif self.with_tcp == True:  # заказали и те и те (задача 2)

                                        succsess_searches_count += 1

                                        brand_group_variants[res[1]]["available"] = 1  # есть наличие в этой группе выдачи



                                    #else:
                                        #tcp +=1

                #15173 выдачи в которых есть хотя бы один(>0) товар из справочника NSI + с учетом пропусков товаров не из товарной матрицы региона
                if test_search:
                    self.count3 += 1

                if succsess_searches_count == 0 and empty_result == False and len(brand_group_variants)>0:#ни одного - неудачный + непустые выдачи были
                    r = self.check_variants(search_query)
                    q = r[1]
                    if r[0]:#искал то что у нас есть обычно(может быть в принципе)!
                        #либо юзер подразумевал сразу несколько сочетаний брендов и групп, которые ищет
                        if len(brand_group_variants)>1:#if len(self.goods_classifier[q][1])>1:#
#2897
                            #self.not_founded_several_brands += 1#5379 - не было совпадений в выдаче с тем что подразумевается под номером детали(совпадений с наличием?)
                            self.count +=1
                            self.founded_several_combinations[search_uid] = brand_group_variants
                        # либо одно сочетание бренда группы которое ищет
                        elif len(brand_group_variants)==1:# elif len(self.goods_classifier[q][1])==1:#т.е. то что не нашел то и записываем в ответ
                            #brand_code = list(self.goods_classifier[q][1].keys())[0][0]
                            #group_code =  list(self.goods_classifier[q][1].keys())[0][1]
                            brand_code = list(brand_group_variants.keys())[0][0]
                            group_code = list(brand_group_variants.keys())[0][1]


                            self.add_to_frame(search_uid, q, brand_code, region, group_code, 0)

                    else:#такого запроса нет в классификаторе!
                        self.not_founded_out_of_articles_range += 1#12755


                elif succsess_searches_count == 1 and len(brand_group_variants)>0:#одно совпадение но с одним и тем же


                    # либо юзер подразумевал сразу несколько сочетаний брендов и групп, которые ищет
                    if len(brand_group_variants) > 1:  #if len(self.goods_classifier[q][1])>1:#подразумевал несколько сочетаний а нашел 1 совпадение из подразумеваемых с выжачей
                        #todo какой бренд/группу брать для этого? правильно ли я сделал, ведь юзер подразумевает несколько сочетаний
                        #получается те которые нашел!, совпал 1 раз по наличию со списком подразумеваемого

                        #brand_code = list(brand_group_variants.keys())[0][0]
                        #group_code = list(brand_group_variants.keys())[0][1]
                        self.count += 1

                        #self.add_to_frame(search_uid, q, brand_code, region, group_code, 1)
                        self.founded_several_combinations[search_uid] = brand_group_variants

                    # либо одно сочетание бренда группы которое ищет
                    elif len(brand_group_variants) == 1:  #elif len(self.goods_classifier[q][0]) == 1:
                        r = self.check_variants(search_query)
                        q = r[1]
                        brand_code = list(brand_group_variants.keys())[0][0]
                        group_code = list(brand_group_variants.keys())[0][1]

                        self.add_to_frame(search_uid, q, brand_code, region, group_code, 1)

                elif succsess_searches_count > 1 and len(brand_group_variants) > 0:#несколько совпадений но с одним и тем же

                    # либо юзер подразумевал сразу несколько сочетаний брендов и групп, которые ищет
                    if len(brand_group_variants) > 1:  #if len(self.goods_classifier[q][1])>1:
                        #brand_code = list(brand_group_variants.keys())[0][0]
                        #group_code = list(brand_group_variants.keys())[0][1]
                        self.count += 1
                        #self.add_to_frame(search_uid, q, brand_code, region, group_code, 1)
                        self.founded_several_combinations[search_uid] = brand_group_variants
                    # либо одно сочетание бренда группы которое ищет
                    elif len(brand_group_variants) == 1:  #elif len(self.goods_classifier[q][1]) == 1:
                        r = self.check_variants(search_query)
                        q = r[1]
                        brand_code = list(brand_group_variants.keys())[0][0]
                        group_code = list(brand_group_variants.keys())[0][1]

                        self.add_to_frame(search_uid,q, brand_code, region, group_code, 1)


class Only_matrix_autocomplete_and_single_result_events(Only_matrix_Search_results_events):
    def __init__(self,brands_dict,groups_dict,all_searches_dict,from_timestamp,region_article,goods_classifier,regions_dict,row_count):

        self.brands_dict = brands_dict
        self.groups_dict = groups_dict
        self.all_searches_dict = all_searches_dict
        self.from_timestamp = from_timestamp
        self.region_article = region_article
        self.goods_classifier = goods_classifier
        self.regions_dict = regions_dict
        self.row_count = row_count

    def query_get_autocompletes_and_single_suggest(self, list_of_args):

        if len(list_of_args) == 1:
            gt = list_of_args[0]  # базовый от 1 июля
        else:
            gt = list_of_args[0]
        query = {
            "bool": {
                "must": [
                    {"range": {"timestamp": {"gt": gt, "lt": 1533081600}}},

                    {
                        "match": {
                            "is_internal_user": False
                        }
                    },
                    {
                        "match": {
                            "event": "product_card_view"
                        }
                    },
                    {
                        "terms": {
                            "traffic_source_type": ["Suggest","SingleSearchResult"]
                        }
                    },
                    {
                        "terms": {
                            "region": ["Новосибирск"]
                        }
                    }
                ]

            }
        }
        return query

    def do_logic(self, list_of_elements):
        length = len(list_of_elements) - 1
        for i in range(0, length, 1):
            hit = list_of_elements[i]["_source"]
            article = hit["article"] #todo для проверки артикула на вхождение в товарную матрицу регионов
            good_info = hit["product_groups_stats"]#информация по наличию товара
            region = hit["region"]
            search_uid = hit["search_uid"]
            nsi = hit["nsi"]
            qres = self.check_variants_articule(article, region)#есть артикул в этом регионе в товарной матрице

            if qres[0]:
                continue

            res = self.check_presence_in_list(article, nsi)#по nsi из справочника извлекаем бренд и группу(их коды)
            #res[1] - это tuple из комбинации кодов (бренд, группа)
            result = False
            #todo уточнить что обозначает это поле!
            if res[0]==True:
                if "product" in good_info and good_info["product"]["count"]>0:#есть товар и остатки больше нуля
                    if "delivery_types" in hit:
                        if "product" in hit["delivery_types"] and hit["delivery_types"]["product"] == "Росско" or hit["delivery_types"]["product"] == "Росско/ТСП":#не ТСП (только свои), на локальном складе не продаются ТСП
                        #значит есть в наличии!
                            result = True
                            self.add_to_frame(search_uid, article, res[1][0], region, res[1][1], 1)

                if result == False:

                    self.add_to_frame(search_uid, article, res[1][0], region, res[1][1], 0)


class Only_matrix_Resolve_Conflict_situations(Search_results_events):
    #на вход список search_uid выдач с конфликтными ситуациями
    def __init__(self,from_timestamp,brand_dict,group_dict,founded_several_combinations,all_searches_dict,region_article,goods_classifier,regions_dict,row_count):
        self.from_timestamp = from_timestamp
        self.brand_dict = brand_dict
        self.group_dict = group_dict
        self.all_searches_dict = all_searches_dict
        self.result_chains = {}
        self.founded_several_combinations = founded_several_combinations
        self.list_of_conflict_search_uids = []
        self.region_article = region_article
        self.goods_classifier = goods_classifier
        self.regions_dict = regions_dict
        self.row_count = row_count

    def query_for_search_uids(self,list_of_args):
        if len(list_of_args) == 1:
            gt = list_of_args[0]  # базовый от 1 июля
        else:
            gt = list_of_args[0]

        query = {
            "bool": {
                "must": [
                    {"range": {"timestamp": {"gt": gt, "lt": 1533081600}}},

                    {
                        "match": {
                            "is_internal_user": False
                        }
                    },
                    {
                        "terms": {
                            "search_uid": list(self.list_of_conflict_search_uids)
                        }
                    },
                    {
                        "terms": {
                            "region": ["Новосибирск"]
                        }
                    }
                ]

            }
        }
        return query

    def check_variants_articule(self,search_query,region):##TODO дублирует метод, а надо чтобьы он был у родителя!
        raw = search_query
        without_spaces = search_query.replace(" ", "")
        without_spaces_and_defises = without_spaces.replace("-", "")
        without_dies_suffix = without_spaces_and_defises.split("#")[0]
        result = False
        q= 0
        if raw in self.region_article[region]:

            result = True
            q = raw
        elif without_spaces in self.region_article[region]:

            result = True
            q = without_spaces
        elif without_spaces_and_defises in self.region_article[region]:

            result = True
            q = without_spaces_and_defises


        elif without_dies_suffix in self.region_article[region]:

            result = True
            q = without_dies_suffix

        return [result,q]

    def do_logic(self, list_of_elements):
        length = len(list_of_elements) - 1
        for i in range(0, length, 1):
            hit = list_of_elements[i]["_source"]
            search_uid = hit["search_uid"]
            timestamp = hit["timestamp"]

            if search_uid not in self.result_chains:
                self.result_chains[search_uid] = {}
            else:
                self.result_chains[search_uid][timestamp] = hit


    def resolve_dispatcher(self,conflict_search_uid_list):
        #частями проходим по списку conflict_search_uid_list, в выражение IN засовываем эту часть списка id и получаем карточки и корзины по которым был переход после выдачи
        #если в выдаче присутствует переход по 1 карточке и/или положил в корзину(с карточками или без них) + товар в корзине = товару в строке поиска по которой перешел, то это разрешение конфликта
        #как проверить какие события есть в этой цепочке? сделать запрос по всем типам событий по этому search_uid и отсортировать по timestamp
        q = self.query_for_search_uids
        l = len(conflict_search_uid_list)

        total_parts = int(math.ceil(l/10000))+1
        prev_part = 0

        for part_of_list in range(1,total_parts,1):
            to = part_of_list*10000

            self.list_of_conflict_search_uids = conflict_search_uid_list[prev_part:to]

            self.get_data(q)


            prev_part = to

        #пройтись по словарю цепочек и разрешить конфликты, составить статистику цепочек
        self.resolve()

    def resolve(self):
        #self.founded_several_combinations[search_uid]# - найденные комбинации! брендов и групп под этим id


        resolved = 0
        for search_uid, elem in self.result_chains.items():
            events = sorted(elem)#todo должно быть по возрастанию времени
            l = len(events)
            stage = 0
            not_target_event = 0
            #add_to_cart = 0
            #card_view = 0
            prev_search_query = ""

            for i in range(0,l,1):
                event = elem[events[i]]
                if event["event"] == "search":#первое событие выдачи в цепи(это оно и должно быть проблемным)
                    stage +=1
                    if stage > 1 and event["page_number"] > 1 and event["search_query"] != prev_search_query :
                        #то это странная ситуация и как-бы новыйц поиск?
                        break
                    prev_search_query = event["search_query"]
                    search_query = event["search_query"]
                    region = event["region"]
                    res = self.check_variants_articule(search_query, region)
                    search_query = res[1]

                #elif stage > 0 and event["event"] != "add_to_cart" and event["event"] != "product_card_view":
                    #not_target_event += 1
                elif stage > 0 and event["event"] == "overview_product":
                    article = event["product"]["article"]# товара который смотрят в превью
                    nsi = event["product"]["nsi"]# товара который смотрят в превью
                    res = self.check_presence_in_list(article, nsi)#[result, brand_group_combination]
                    if res[0] == True:

                        brand_code = res[1][0]#self.brand_dict[brand]
                        group_code = res[1][1]#self.group_dict[group]

                        if (brand_code, group_code) in self.founded_several_combinations[search_uid]:
                            self.founded_several_combinations[search_uid][(brand_code, group_code)]["preview"] += 1
                    else:
                        a = 1
                    #он мог смотреть те вещи которые не в наличии, при том что есть искосые в наличии?
                elif stage > 0 and event["event"] == "add_to_cart":
                    #todo он мог положить в корзину аналог, поэтому по бренду могло не совпасть!
                    #надо чтобы бренд и грцппа совпадали с брендом и грцппой поиска?
                    article = event["article"] #товара который положили в корзину
                    nsi = event["nsi"]#товара который положили в корзину
                    res = self.check_presence_in_list(article, nsi)  # [result, brand_group_combination]
                    if res[0] == True:
                        brand_code = res[1][0]#self.brand_dict[brand]
                        group_code = res[1][1]#self.group_dict[group]
                        if (brand_code,group_code) in self.founded_several_combinations[search_uid]:#в выдаче поиска есть такая группа(сочетание бренда - группы)

                            self.founded_several_combinations[search_uid][(brand_code, group_code)]["cards"] += 1
                    else:
                        a = 1

                    #add_to_cart +=1
                elif stage > 0 and event["event"] == "product_card_view":
                    #в цепочке должна быть карточка с типом - переход из выдачи
                    article = event["article"]# товара который смотрят в карточке
                    nsi = event["nsi"]# товара который смотрят в карточке

                    res = self.check_presence_in_list(article, nsi)  # [result, brand_group_combination]
                    if res[0]==True:
                        brand_code = res[1][0]#self.brand_dict[brand]
                        group_code = res[1][1]#self.group_dict[group]

                        if (brand_code, group_code) in self.founded_several_combinations[search_uid]:
                            self.founded_several_combinations[search_uid][(brand_code, group_code)]["carts"] += 1
                    else:
                        a=1#TODO чем это чревато, т.к. у меня срабатывает
                    #card_view +=1
                #или карточка товара
                #или корзина

            #проходим по
            card = 0
            cart = 0
            view = 0
            card_combination = ()
            cart_combination = ()
            view_combination = ()
            is_search_result_resolved = False
            for brand_group_combination, elem in self.founded_several_combinations[search_uid].items():

                if elem["carts"]>0:
                    cart += 1
                    cart_combination = brand_group_combination
                    cart_available = elem["available"]  # есть ли в наличии/удачный или неудачный поиск
                elif elem["cards"]>0:
                    card += 1
                    card_combination = brand_group_combination
                    card_available = elem["available"]
                elif elem["preview"]>0:
                    view += 1
                    view_combination = brand_group_combination
                    view_available = elem["available"]#есть ли в наличии
                    #todo search_query идет в фрейм не очищеным!
            if card == 1 and cart == 0:
                #разрешен
                is_search_result_resolved = True

                self.add_to_frame(search_uid, search_query, card_combination[0], region, card_combination[1], card_available)
            elif card == 0 and view == 1 and cart == 0:
                # разрешен
                is_search_result_resolved = True
                self.add_to_frame(search_uid, search_query, view_combination[0], region, view_combination[1], view_available)
            elif card == 0 and cart == 1:
                # разрешен
                is_search_result_resolved = True
                self.add_to_frame(search_uid, search_query, cart_combination[0], region, cart_combination[1], cart_available)
            elif card >= 1 and cart == 1:
                #разрешен
                is_search_result_resolved = True
                self.add_to_frame(search_uid, search_query, cart_combination[0], region, cart_combination[1], cart_available)

            if is_search_result_resolved==True:
                resolved +=1

def memory_usage_psutil():
    # return the memory usage in MB
    process = psutil.Process(os.getpid())
    mem = process.memory_full_info()[12] / float(2 ** 20)
    return mem

class Only_matrix_search_plots_factory(Search_plots_factory):
    prefix = "matrix"

    def get_main_dataframe(self, from_db=True):#получает главный фрейм из бд или файла и сохраняет его в атрибут класса, для дальнейшего доступа к нему откуда угодно


        if from_db:
            print(memory_usage_psutil())
            #todo сделать файл справочника - убрать имена!
            filter_fabric = Filter_fabric(1532084400)

            filter_fabric.get_only_used_data_from_dictionaries()
            print(memory_usage_psutil())
            search_results = Only_matrix_Search_results_events(filter=filter_fabric.filter["articules"])# searches_input_field.list_of_search_uids передавать когда понадобится фильтрация по мобытию вставки номера детали в строку поиска
            print(memory_usage_psutil())
            search_results.add_matrix_dictionary_to_class(filter=filter_fabric.filter)
            print(memory_usage_psutil())
            exit(1)
            q = search_results.query_make3
            #q = search_results.test_query
            search_results.get_data(q)

            #with open(r"search_resultst.pickle", "wb") as output_file:
                 #pickle.dump(search_results, output_file)


            tmpframe = pd.DataFrame.from_dict(search_results.all_searches_dict, orient='index',columns=['Search_uid', 'Search_query', 'brand', 'region', 'group','Search_result'])
            tmpframe.to_csv('out_tmp_' + self.prefix + '.csv', index=False)
            #search_results.types_of_results.to_csv("test_10000.csv", index=False)
            #добавляем однозначные поиски
            additional_search_results = Only_matrix_autocomplete_and_single_result_events(
                search_results.brands_dict,
                search_results.groups_dict,
                search_results.all_searches_dict,
                search_results.from_timestamp,
                search_results.region_article,
                search_results.goods_classifier,
                search_results.regions_dict,
                search_results.row_count
            )

            q = additional_search_results.query_get_autocompletes_and_single_suggest
            additional_search_results.get_data(q)
            tmpframe = pd.DataFrame.from_dict(additional_search_results.all_searches_dict, orient='index',columns=['Search_uid', 'Search_query', 'brand', 'region', 'group','Search_result'])
            tmpframe.to_csv('out_tmp_' + self.prefix + '.csv', index=False)

            #with open(r"additional_search_results.pickle", "wb") as output_file:
                 #pickle.dump(search_results, output_file)

            #разрешаем конфликты
            resolver = Only_matrix_Resolve_Conflict_situations(
                search_results.from_timestamp,
                search_results.brands_dict,
                search_results.groups_dict,
                search_results.founded_several_combinations,
                additional_search_results.all_searches_dict,
                search_results.region_article,
                search_results.goods_classifier,
                search_results.regions_dict,
                search_results.row_count
            )
            resolver.resolve_dispatcher(list(search_results.founded_several_combinations.keys()))
            #with open(r"resolver.pickle", "wb") as output_file:
                 #pickle.dump(search_results, output_file)


            #todo проверить чтобы search_results.all_searches_dict было больше чем до автокомлита!
            self.main_frame = pd.DataFrame.from_dict(resolver.all_searches_dict, orient='index',columns=['Search_uid','Search_query', 'brand', 'region', 'group', 'Search_result'])
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

            self.main_frame.to_csv('out_'+self.prefix+ '.csv', index=False)
            self.first_timestamp_from_query = search_results.first_timestamp_from_query

            with open('special_'+self.prefix+ '.json', 'w') as fp:
                json.dump(self.first_timestamp_from_query, fp)
            with open('brand_dict_'+self.prefix+ '.json', 'w') as fp:
                json.dump(self.brand_dict, fp)
            with open('group_dict_'+self.prefix+ '.json', 'w') as fp:
                json.dump(self.group_dict, fp)
            with open('region_dict_'+self.prefix+ '.json', 'w') as fp:
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

            self.main_frame = pd.read_csv('out_'+self.prefix+ '.csv')
            #self.collapse_rows_from_one_pagination_chain()
            with open('special_'+self.prefix+ '.json') as data_file:
                self.first_timestamp_from_query = json.load(data_file)
            with open('brand_dict_'+self.prefix+ '.json') as data_file:
                self.brand_dict = json.load(data_file)
            with open('group_dict_'+self.prefix+ '.json') as data_file:
                self.group_dict = json.load(data_file)
            with open('region_dict_'+self.prefix+ '.json') as data_file:
                self.region_dict = json.load(data_file)
            #+++++++++++++++++++++++++++++++++++++++++++++++
            #writer = pd.ExcelWriter('main_frame.xlsx')
            #self.main_frame.to_excel(writer, 'Sheet1')

            #writer.save()
