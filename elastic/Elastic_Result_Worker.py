import requests
import json
import pandas as pd
from time import gmtime, strftime
import time
from datetime import datetime

class Elastic_Result_Worker():

    def __init__(self,search_plots_factory,curent_interval_timestamp):
        self.elastic_adress = "http://bielastic.rossko.local:9200/"
        self.elastic_index = "prod_i_m-"
        self.searches_statistics = pd.DataFrame()
        self.curent_interval_timestamp = curent_interval_timestamp

        self.region_dict = list(search_plots_factory.region_dict)
        self.brand_dict = list(search_plots_factory.brand_dict)
        self.group_dict = list(search_plots_factory.group_dict)


    def get_elastic_data_presentation(self,input_frame):

        total_searches = input_frame.groupby(['region', 'brand', 'group'])['Search_result'].agg('count').to_frame()#сколько всего поисков
        sucsess_searches = input_frame[input_frame["Search_result"] == 1].groupby(['region', 'brand', 'group'])['Search_result'].agg('count').to_frame()#сколько всего удачных поисков
        sucsess_searches_sales = input_frame[(input_frame["Search_result"] == 1) & (input_frame["Sale"] == 1)].groupby(['region', 'brand', 'group'])['Search_result'].agg('count').to_frame()  # сколько всего удачных поисков завершенных продажей


        df3 = pd.merge(total_searches, sucsess_searches, left_index=True,right_index=True,how='left')
        self.searches_statistics = pd.merge(df3, sucsess_searches_sales, left_index=True,right_index=True,how='left')
        self.searches_statistics.fillna(0, inplace=True)
        self.searches_statistics.columns = ["Total_searches", "Sucsess_searches", "Sales_from_sucsess_searches"]
        self.searches_statistics.reset_index(inplace=True)





    def walk_by_result_frame(self,i):
        #a = 1
        region_brand = self.searches_statistics.groupby(['region', 'brand'], as_index=False).agg('sum')

        # todo можно пройтись по всем строкам и сохранить каждую в отдельный документ
        # todo можно создать агрегат по региону-бренду, региону-товарной группе
        rows = region_brand.shape[0]
        # todo sort by region!
        region_brand.sort_values(by=['region'])
        prev_region = ""
        doc = {}
        i = i
        for row in range(0, rows, 1):
            line = region_brand.iloc[row]

            region = self.region_dict[int(line["region"])]
            brand = self.brand_dict[int(line["brand"])]
            total = int(line["Total_searches"])
            sucsess = int(line["Sucsess_searches"])
            sold = int(line["Sales_from_sucsess_searches"])

            #if prev_region != region:
            doc = {}

            doc["@region"] = region
            doc["brand"] = brand
            doc["group"] = "-"
            doc["total_searches"] = total
            doc["sucsess_searches"] = sucsess
            doc["solded_searches"] = sold


            self.insert_document_to_elastic(doc, i)

            i += 1

            #doc = {}
            #doc["region"] = region
            ##doc["brands"] = []
            #i += 1
            #prev_region = region


            # todo сделать его в один документ! регион-бренд - 3 числа, протестировать потом nested документы и будет ли виден атрибут регион в родительском контейнере?
            #doc["brands"].append({"brand_name": brand, "total": total, "sucsess": sucsess, "sold": sold})


        region_group = self.searches_statistics.groupby(['region', 'group'], as_index=False).agg('sum')
        region_group.sort_values(by=['region'])



        rows = region_group.shape[0]
        doc = {}
        i = i
        for row in range(0, rows, 1):
            line = region_group.iloc[row]

            region = self.region_dict[int(line["region"])]
            group = self.group_dict[int(line["group"])]
            total = int(line["Total_searches"])
            sucsess = int(line["Sucsess_searches"])
            sold = int(line["Sales_from_sucsess_searches"])

            # if prev_region != region:
            doc = {}

            doc["@region"] = region
            doc["brand"] = "-"
            doc["group"] = group
            doc["total_searches"] = total
            doc["sucsess_searches"] = sucsess
            doc["solded_searches"] = sold

            self.insert_document_to_elastic(doc, i)
            i += 1

        return i

    """
     def generate_bulk_insert_string(self,document_to_insert):
        doc_string = json.dumps(document_to_insert)
        
        {"index": {}}
        {"name": "john doe", "age": 25}
        {"index": {}}
        {"name": "mary smith", "age": 32}
       



        return bulk_part
        
        def bulk_insert_document_to_elastic(self,query_string):
        current_day = strftime("%Y.%m.%d", gmtime())


        headers = {'Accept': 'application/json', 'Content-type': 'application/json'}
        #elastic_url = 'http://roesportal.rossko.local:80/' + self.index + '-*/_search/?size=' + str(size) + '&pretty'
        #elastic_url = 'http://elasticsearch.rossko.local:9200/' + self.index + '-*/_search/?size=' + str(size) + '&pretty'
        elastic_url = "http://elasticsearch.rossko.local:9200/inventory_management-"+current_day+"/doc/_bulk"


        while True:
            response = requests.post(elastic_url, data=query_string, verify=False, headers=headers)
            if (response.status_code == 502):  # , response.text.find("502 Bad Gateway") > 0
                print("Bad Gateway... REPEAT")
                continue
            else:

                break
    """


    def insert_document_to_elastic(self, doc, id):
        doc["@timestamp"] = self.curent_interval_timestamp
        query_string = json.dumps(doc)
        #current_day = "2018.08.18" strftime("%Y.%m.%d", gmtime())
        current_day = datetime.utcfromtimestamp(int(self.curent_interval_timestamp)/1000).strftime('%Y.%m.%d')


        headers = {'Accept': 'application/json', 'Content-type': 'application/json'}

        elastic_url = self.elastic_adress+self.elastic_index + current_day + "/doc/"+str(id)


        while True:
            response = requests.post(elastic_url, data=query_string, verify=False, headers=headers)
            if (response.status_code == 502):  # , response.text.find("502 Bad Gateway") > 0
                print("Bad Gateway... REPEAT")
                continue
            else:

                break

