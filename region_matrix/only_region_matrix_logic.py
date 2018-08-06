from elastic.elastic_queries_new_logic import Search_plots_factory
from elastic.elastic_queries_new_logic import Search_results_events
import json
import pandas as pd


class Only_matrix_Search_results_events(Search_results_events):
    #nsi_not_in_matrix = False

    def add_matrix_dictionary_to_class(self):


        #names=["Nom_Name","Nom_Code","Artikul","SKlad_Code","SKlad_Name","CFO_Code","CFO_Name","NomGr","MarkGr","SinMarkGr","Brand"]
        matrix = pd.read_csv("matrixNom.csv", sep=';')
        #matrix = matrix[matrix["CFO_Name"]=="Новосибирск",matrix["CFO_Name"]=="Санкт-Петербург"]
        array = ["Новосибирск", "Санкт-Петербург"]
        self.matrix = matrix.loc[matrix["CFO_Name"].isin(array)]

        #u = matrix["Nom_Code"].unique()
        #length = len(u)
        #self.nsi_codes = dict(zip(list(u), [i for i in range(0, length)]))
        #u = matrix["Artikul"].unique()
        #length = len(u)
        #self.articules_codes = dict(zip(list(u), [i for i in range(0, length)]))

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
                            "region": ["Новосибирск", "Санкт-Петербург"]
                        }
                    }
                ]

            }
        }
        return query
    def check_exist_in_nsi(self,nsi,region):
        l = self.matrix[(self.matrix["CFO_Name"]==region)& (self.matrix["Nom_Code"]==nsi)].shape[0]
        if l>0:
            return True
        return False

    def check_variants_articule(self,search_query,region):
        raw = search_query
        without_spaces = search_query.replace(" ", "")
        without_spaces_and_defises = without_spaces.replace("-", "")
        without_dies_suffix = without_spaces_and_defises.split("#")[0]
        result = False
        q= 0

        if self.matrix[(self.matrix["CFO_Name"] == region) & (self.matrix["Artikul"] == raw)].shape[0]>0:
            result = True
            q = raw

        elif self.matrix[(self.matrix["CFO_Name"] == region) & (self.matrix["Artikul"] == without_spaces)].shape[0]>0:
            result = True
            q = without_spaces

        elif self.matrix[(self.matrix["CFO_Name"] == region) & (self.matrix["Artikul"] == without_spaces_and_defises)].shape[0]>0:
            result = True
            q = without_spaces_and_defises

        elif self.matrix[(self.matrix["CFO_Name"] == region) & (self.matrix["Artikul"] == without_dies_suffix)].shape[0]>0:
            result = True
            q = without_dies_suffix

        return [result,q]

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

            succsess_searches_count = 0  # не ТСП

            #brand_from_query = self.get_from_query(search_query)

            #список сочетаний брендов-товарных групп

            if "region" in hit:
                region = hit["region"]
                #have_region += 1
                qres = self.check_variants_articule(search_query,region)

                if qres[0] == False:
                    continue
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
                            link_parts = element_of_search_results["link"].split("=")
                            nsi = link_parts[1] #из ссылки извлекаем
                            #совпадение по бренду/товарной группе из поисковой строки
                            if len(link_parts)>2:
                                a=1

                            if self.check_exist_in_nsi(nsi,region)==False:

                                continue#пропускаем его как будто его нет

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
                                            brand_group_variants[res[1]] = 1
                                        else:
                                            brand_group_variants[res[1]] += 1



                                    elif self.with_tcp == True:  # заказали и те и те (задача 2)

                                        succsess_searches_count += 1


                                        if res[1] not in brand_group_variants:
                                            brand_group_variants[res[1]] = 1
                                        else:
                                            brand_group_variants[res[1]] += 1

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
#2897
                            self.not_founded_several_brands += 1#5379 - не было совпадений в выдаче с тем что подразумевается под номером детали(совпадений с наличием?)
                        # либо одно сочетание бренда группы которое ищет
                        elif len(self.goods_classifier[q][1])==1:#т.е. то что не нашел то и записываем в ответ
                            brand_code = list(self.goods_classifier[q][1].keys())[0][0]
                            group_code =  list(self.goods_classifier[q][1].keys())[0][1]
                            #brand_code = list(self.goods_classifier[search_query][1].keys())[0][0]
                            #group_code = list(self.goods_classifier[search_query][1].keys())[0][1]

                            self.add_to_frame(search_uid, q, brand_code, region, group_code, 0)

                    else:#такого запроса нет в классификаторе!
                        self.not_founded_out_of_articles_range += 1#12755


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
                        #todo какой бренд группу брать?268 таких
                        #self.founded_several_combinations += 1#46 таких
                        self.founded_several_combinations[search_uid] = 1#пагинация должна уменьшить количество конфликтных ситуаций
                        #удалять строку из фрейма если из-за пагинации там уже что-то есть
                        #if (self.all_searches['Search_uid'] == search_uid).any() == True:
                            #self.all_searches.drop(self.all_searches[self.all_searches['Search_uid'] == search_uid].index,inplace=True)


                #статистику по одиночным посчитаем по self.all_searches


class Only_matrix_search_plots_factory(Search_plots_factory):
    prefix = "matrix"

    def get_main_dataframe(self, from_db=True):#получает главный фрейм из бд или файла и сохраняет его в атрибут класса, для дальнейшего доступа к нему откуда угодно


        if from_db:

            search_results = Only_matrix_Search_results_events()# searches_input_field.list_of_search_uids передавать когда понадобится фильтрация по мобытию вставки номера детали в строку поиска
            search_results.add_matrix_dictionary_to_class()
            q = search_results.query_make3
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
            #todo из фрейма фильтровать ьлдбко наши бренды!