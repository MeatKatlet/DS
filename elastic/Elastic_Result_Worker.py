import requests
import json
import pandas as pd
from time import gmtime, strftime

class Elastic_Result_Worker():

    def __init__(self,url):
        self.elastic_adress = url
        self.searches_statistics = pd.DataFrame()

    def get_elastic_data_presentation(self,input_frame):
        #Search_uid,Search_query,region,brand,group,Search_result,Search_uid_sales,Sale
        # todo создать фрейм в котором будут сгруппированы строки по городу/бренду группе / будет колонка всег опоисков/ удачных поисков и колонка споисками закончеными продажей
        #tmp = input_frame.groupby(['region','brand','group'], as_index=False)['Search_result'].agg('count')# количество поисков всего в каждой группе
        #input_frame[input_frame["Search_result"]==1].groupby(['region','brand','group'], as_index=False)[]

        total_searches = input_frame.groupby(['region', 'brand', 'group'])['Search_result'].agg('count').to_frame()#сколько всего поисков
        sucsess_searches = input_frame[input_frame["Search_result"] == 1].groupby(['region', 'brand', 'group'])['Search_result'].agg('count').to_frame()#сколько всего удачных поисков
        sucsess_searches_sales = input_frame[(input_frame["Search_result"] == 1) & (input_frame["Sale"] == 1)].groupby(['region', 'brand', 'group'])['Search_result'].agg('count').to_frame()  # сколько всего удачных поисков завершенных продажей


        df3 = pd.merge(total_searches, sucsess_searches, left_index=True,right_index=True,how='left')
        self.searches_statistics = pd.merge(df3, sucsess_searches_sales, left_index=True,right_index=True,how='left')
        self.searches_statistics.fillna(0, inplace=True)
        self.searches_statistics.columns = ["Total_searches", "Sucsess_searches", "Sales_from_sucsess_searches"]
        self.searches_statistics.reset_index(inplace=True)

        #input_frame["Search_result"][input_frame["Search_result"]==1] = "Удачный"
        #input_frame["Search_result"][input_frame["Search_result"]==0] = "НеУдачный"
        #input_frame["Sale"][input_frame["Sale"]==1] = "Продажа"
        #input_frame["Sale"][input_frame["Sale"]==0] = "НетПродажы"

        #df2 = input_frame.groupby(['region','brand','group', 'Search_result','Sale']).size().reset_index(name='count')

        #df3 = input_frame.groupby(['region','brand','group', 'Search_result','Sale']).size().unstack(fill_value=0)
        self.walk_by_result_frame()


    def walk_by_result_frame(self):
        #a = 1
        region_brand = self.searches_statistics.groupby(['region', 'brand'], as_index=False).agg('sum')

        # todo можно пройтись по всем строкам и сохранить каждую в отдельный документ
        # todo можно создать агрегат по региону-бренду, региону-товарной группе
        rows = region_brand.shape[0]
        # todo sort by region!

        prev_region = ""
        doc = {}
        i = 0
        for row in range(0, rows, 1):
            line = region_brand.iloc[row]

            region = line[0]  # todo по справочнику получить имя региона!
            brand = line[1]  # todo по справочнику получить имя группы!
            total = line[3]
            sucsess = line[4]
            sold = line[5]

            if prev_region != region:
                self.insert_document_to_elastic(json.dumps(doc), i)
                doc = {}
                doc["region"] = region
                doc["brands"] = []
                i += 1
                prev_region = region

            # todo сделать его в один документ! регион-бренд - 3 числа, протестировать потом nested документы и будет ли виден атрибут регион в родительском контейнере?
            doc["brands"].append({"brand_name": brand, "total": total, "sucsess": sucsess, "sold": sold})

        # todo тоже самое проделать с группами!
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


    def insert_document_to_elastic(self, query_string, id):
        current_day = strftime("%Y.%m.%d", gmtime())

        headers = {'Accept': 'application/json', 'Content-type': 'application/json'}
        # elastic_url = 'http://roesportal.rossko.local:80/' + self.index + '-*/_search/?size=' + str(size) + '&pretty'
        # elastic_url = 'http://elasticsearch.rossko.local:9200/' + self.index + '-*/_search/?size=' + str(size) + '&pretty'
        elastic_url = "http://elasticsearch.rossko.local:9200/inventory_management-" + current_day + "/doc/"+str(id)


        while True:
            response = requests.post(elastic_url, data=query_string, verify=False, headers=headers)
            if (response.status_code == 502):  # , response.text.find("502 Bad Gateway") > 0
                print("Bad Gateway... REPEAT")
                continue
            else:

                break





frame_all_searches_with_sales = pd.read_csv("../searches/frame_all_searches_with_sales_matrix.csv")

w = Elastic_Result_Worker("")
w.get_elastic_data_presentation(frame_all_searches_with_sales)