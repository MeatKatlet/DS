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
#import searches.searches
#from searches import searches
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
            query = self.get_query(q, [str(last_tsmp)])

            body = {
                "size": size,
                "query": query,
                "sort": [{timestampe_field_name: {"order": "asc"}}]}

            query = json.dumps(body)

            response = requests.post(elastic_url, data=query, verify=False, headers=headers)
            #time.sleep(5)
            if (response.text.find("502 Bad Gateway") > 0):#response.status_code == 502
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
        self.not_succsess2 = 0
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
                succsess_searches_count2 = 0
                brands = list()
                market_groups = list()
                empty_result = True

                for k in range(0, length2, 1):#по всем элементам results_groups, в каждом элементе один атрибут search_results
                    results_group_item = hit["results_groups"][k]

                    length3 = len(results_group_item["search_results"])

                    for j in range(0, length3, 1):
                        element_of_search_results = results_group_item["search_results"][j]
                        empty_result = False

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
                        """
                     elif  "part" in element_of_search_results and ("brand_name" in element_of_search_results or "market_group_name" in element_of_search_results):
                         succsess_searches_count2 += 1
                         
                             if element_of_search_results["part"]["is_local_delivery"] == True:  # только с локального склада
                                
                                if element_of_search_results["part"]["is_approximate_delivery_interval"] == False and self.with_tcp == False:  # не ТСП и ТСП не заказывали для запроса
                                    


                                elif self.with_tcp == True:  # заказали и те и те (задача 2)
                                    succsess_searches_count2+=1
                            """

                #keys = list(brands.keys())
                #keys2 = list(market_groups.keys())
                unique_brands = set(brands)
                unique_market_groups = set(market_groups)

                #if succsess_searches_count2 > 0:
                    #self.not_succsess2 += 1

                if succsess_searches_count == 0 and empty_result == False:#ни одного - неудачный
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

        length = search_uids_list.shape[0]

        self.search_uids_list = dict(zip(list(search_uids_list), [i for i in range(0,length)]))
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
            if sale["_source"]["search_uid"] in self.search_uids_list:
                self.sales.loc[len(self.sales)] = [sale["_source"]["search_uid"],sale["_source"]["timestamp"],1]
            else:
                self.sales_without_searches.loc[len(self.sales_without_searches)] = [sale["_source"]["search_uid"],sale["_source"]["timestamp"],1]


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







class Search_plots_factory():

    def __init__(self):
        self.index = 'cart_item_event'
        self.timestampe_field_name = "timestamp"

        self.main_frame = pd.DataFrame()

        self.slice_region_brand = pd.DataFrame()
        self.slice_region_group = pd.DataFrame()
        self.slice_brand_group = pd.DataFrame()

        self.not_succsess_searches = 0


    def get_main_dataframe(self, from_db=True):#получает главный фрейм из бд или файла и сохраняет его в атрибут класса, для дальнейшего доступа к нему откуда угодно

        #todo пропустить весь главный фрейм через фильтр с нашими брендами!, там написаны в ручную имена товарных групп, надо как-то это делать вручную!
        #пока оставим как есть все!
        # todo надо будет делать чтобы уже сохраненные данные в файле не запрашивались, видимо time stamp, надо сохранить timestamp последнего документа!

        if from_db:

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
            search_results.get_data(q, self.index, self.timestampe_field_name)


            self.main_frame = search_results.all_searches

            self.not_succsess_searches = search_results.not_succsess

            special = pd.DataFrame(columns=["Data"])
            special.loc[len(special)] = [self.not_succsess_searches]
            special.to_csv('special.csv')#сохранить в файл

            self.draw_statistics(search_results)


            self.main_frame.to_csv('out.csv')

        else:
            special = pd.read_csv('special.csv')
            self.not_succsess_searches = special.iloc[0][1]

            self.main_frame = pd.read_csv('out.csv')


    def create_crosstab(self,f,field_to_save_slice,frame):#этот метод нужен чтобы делать кросстаб независимо от хитмапов
        slice = getattr(self, field_to_save_slice)
        if slice.shape[0] == 0 and slice.shape[1] == 0:
            slice = f(frame)#разрез
            setattr(self, field_to_save_slice, slice)

        return slice

    def create_heatmap(self,plot_heatmap,f,plot_name, plot_size,field_to_save_slice):#по параметрам будем делать heatmap


        df = self.create_frame_for_plot(f,field_to_save_slice)

        plot_heatmap(df, plot_name, plot_size)


    def draw_statistics(self,search_results):
        s = self.main_frame.shape
        print(s[0])  # количество удачных поисков всего

        a1 = search_results.searches_not_standart.groupby('Timestamp', as_index=False)['Brand'].count()
        a2 = search_results.searches_not_standart.groupby('Timestamp', as_index=False)['Group'].count()

        r = pd.merge(a1, a2)
        r.to_csv('two_or_more_brands_or_groups_in_serch_results.csv')

        # r.groupby("Timestamp",'Brand','Group', as_index=False).count()#смотрим сколько каких сочетаний бренда и групп есть
        # дальше на основе этого будем смотреть что делать дальше!

    def create_frame_for_plot(self,f,field_to_save_slice):

        sliced_searches = self.create_crosstab(f, field_to_save_slice, self.main_frame)#разрез

        sum = sliced_searches.values.sum()

        percentage_of_sucsess = pd.DataFrame(index=sliced_searches.index, columns=sliced_searches.columns.values)

        s = sliced_searches.shape
        for i in range(0, s[0], 1):  # по строкам
            row = []
            # row_name = all_searches.index[i]
            for j in range(0, s[1], 1):  # по столбцам

                row.append(self.fill_cell_value(sliced_searches,i,j,[sum,field_to_save_slice]))

            percentage_of_sucsess.iloc[i] = row

            # ++++++++++++++++++++
            # иатрицу конверсии в продажи считаем
            # заодно проверим согласованноссть с поисками
            # сначала отфильтровать поиски завершившиеся продажей Sales==1,
            # потом на основе этого сделать кросстаб по разрезам,
            # пройтись по всем поискам по координатам поисков завершенных продажей в каждой ячейке посчитать % удачных поисков завершенных продажей, идти по поисковой матрице , если нет индекса в продажной то поиски в ячейке без продаж!

        return percentage_of_sucsess

    def fill_cell_value(self, sliced_searches,i,j,list_additional_params=list()):
        sum = list_additional_params[0]
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

        percentage_in_cell = (sliced_searches.iloc[i][j] * 100) / (sum + self.not_succsess_searches)
        # if percentage_in_cell>0:
        #   percentage_in_cell = 1

        return percentage_in_cell

class Sales_plots_factory(Search_plots_factory):

    def __init__(self,search_plots_factory,from_db):

        # сначала отфильтровать поиски завершившиеся продажей Sales==1,
        self.main_frame = search_plots_factory.main_frame

        #if 'Sale' not in search_plots_factory.main_frame.columns:
        #1530813106 - это самый ранний timestamp
        if from_db:
            # делаем запрос на продажи
            sales = Search_sales(self.main_frame["Search_uid"], self.main_frame.iloc[0][2])#ранний timestamp записали
            q = query_make4
            sales.get_data(q, search_plots_factory.index, search_plots_factory.timestampe_field_name,size=1000)

            #проверить на уникальность все значения индекса!
            l1 = len(self.main_frame['Search_uid'].unique())
            l2 = len(sales.sales['Search_uid'].unique())
            if l1 != self.main_frame.shape[0] or l2 != sales.sales.shape[0]:
                exit(1)

            self.main_frame.set_index('Search_uid')
            sales.sales.set_index('Search_uid')

            self.frame_searches_with_sales = pd.concat([self.main_frame, sales.sales], axis=1, sort=False)
            self.frame_searches_with_sales.fillna(0)#в файл
            self.frame_searches_with_sales.to_csv("frame_searches_with_sales.csv")

            self.sales_without_searches = sales.sales_without_searches#в файл
            self.sales_without_searches.to_csv("sales_without_searches.csv")
        else:
            self.frame_searches_with_sales = pd.read_csv('frame_searches_with_sales.csv')
            self.sales_without_searches = pd.read_csv('sales_without_searches.csv')




        self.frame_searches_with_sales_only = self.frame_searches_with_sales.query("Sale == 1")#чтобы это здесь было нужно чтобы в файле уже было проведено объединение


        # cross tab срез по 3 м ситуациям, только для поисков завершенных продажей
        self.slice_region_brand_2 = pd.DataFrame()
        self.slice_region_group_2 = pd.DataFrame()
        self.slice_brand_group_2 = pd.DataFrame()


    def get_statistics_of_serches_and_sales(self):
        #% поисков с продажами - мы сравниваем только эту часть по продажам
        #% продаж без поисков
        #% поисков без продаж
        #% не было выдачи + не было продажи(в корзину положили но не купили?)

        #пока только 3 типа
        without_sales = self.main_frame.query("Sale == 0")
        all_chains = self.sales_without_searches.shape[0] + without_sales.shape[0] + self.frame_searches_with_sales_only.shape[0]

        percent1 = (self.sales_without_searches.shape[0]*100)/all_chains #% продаж без поисков.
        percent2 = (without_sales.shape[0]*100)/all_chains#% поисков без продаж
        percent3 = (self.frame_searches_with_sales_only.shape[0]*100)/all_chains#% поисков с продажами

        labels = 'продаж без поисков', 'поисков без продаж', 'поисков с продажами'
        fracs = [percent1, percent2, percent3]

        plt.pie(fracs, labels=labels, autopct='%1.1f%%', shadow=False)
        plt.title('Виды цепочек действий пользователей')
        plt.show()

        print([self.sales_without_searches.shape[0],without_sales.shape[0],self.frame_searches_with_sales_only])
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


    def create_heatmap(self,plot_heatmap, f, plot_name, plot_size, field_to_save_slice):  # по параметрам будем делать heatmap
        #переопредедяем этот метод чтобы иметь отдельный кросстабы по срезам! они нужны для расчетов %
        #можно и делать по региону - бренду запросы в self.frame_searches_with_sales_only  для расчета % в функции fill_cell_value(для подсчета количества поисков в ячейке завершенных продажей)- но это может быть дольше

        self.create_crosstab(f, field_to_save_slice + "_2",self.frame_searches_with_sales_only)  # делаем слайс по поискам завершенным продажей

        df = self.create_frame_for_plot(f, field_to_save_slice)

        #todo можно удалить созданный field_to_save_slice + "_2" чтобы память не занимал

        plot_heatmap(df, plot_name, plot_size)


    def fill_cell_value(self, sliced_searches, i, j, list_additional_params=list()):#логика для каждой ячейки будет описана здесь!
        row_name = sliced_searches.index[i]
        col_name = sliced_searches.columns[j]

        slice = getattr(self, list_additional_params[1]+"_2")#получаем разрез с поисками завершенными продажей из этого класса

        percentage_of_saled_searches_in_cell = 0

        if row_name in slice.index and col_name in slice.columns:
            if sliced_searches.iloc[i][j] == 0:#срез где все поиски!
                percentage_of_saled_searches_in_cell = 0

                assert (slice.loc[row_name][col_name] == 0),"Slice "+str(list_additional_params[1])+"_2"+" Not equal values in crosstab of all searches and in crosstab of serches ended with sales"
            else:
                percentage_of_saled_searches_in_cell = (slice.loc[row_name][col_name]*100) / sliced_searches.iloc[i][j]

                assert (slice.loc[row_name][col_name] != 0), "Slice " + str(list_additional_params[1]) + "_2" + " Not equal values in crosstab of all searches and in crosstab of serches ended with sales"

        return percentage_of_saled_searches_in_cell

        #сверяем каждую матрицу с разрезом по удачным поискам с соответствующей матрицей поисков завершенных продажей!
        #тут нужен проход по всем ячейкам фрейма с поиском и проверка существования соответствующих ячеек во фрейме с поисками!


        #для сравнения поисков с проажами, надо проходя по фрейму с продажами , проверять существование ячейки в фрейме поисков, и проверять что значение по этому индексу >0
        #так мы проверим только часть поисков завершенных продажей, остальные без продаж, значит не проверены



def region_brand(search_results):

    all_searches = pd.crosstab(search_results.Region, search_results.Brand)  # таблица по всем поискам
    #sucsess_searches = pd.crosstab(df_sucsess_searches.Region, df_sucsess_searches.Brand)  # таблица по удачным поискам
    #writer = pd.ExcelWriter('output.xlsx')
    #all_searches.to_excel(writer, 'Sheet1')

    #writer.save()

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