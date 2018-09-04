import numpy as np
#import matplotlib
#import matplotlib.pyplot as plt
import warnings
#import pandas as pd
import elastic.elastic_queries_new_logic
#from elastic.elastic_queries import Search_plots_factory
#from elastic.elastic_queries import Sales_plots_factory
from matplotlib.patches import Rectangle
warnings.filterwarnings('ignore')
#from searches.elastic_queries import Base_Elastic
#from searches.elastic_queries import Searches_in_input_field_event
#from searches.elastic_queries import Search_results_events

#from searches.elastic_queries import run_logic
#from searches.elastic_queries import region_brand
#from searches.region_matrix import only_region_matrix_logic
#import searches.region_matrix.only_region_matrix_logic
import psutil
import os
import region_matrix.only_region_matrix_logic
from elastic.Elastic_Result_Worker import Elastic_Result_Worker
import time
import json

def main():

    with open('brand_dict_matrix.json') as data_file:
        brand_dict = json.load(data_file)
    with open('group_dict_matrix.json') as data_file:
        group_dict = json.load(data_file)
    with open('region_dict_matrix.json') as data_file:
        region_dict = json.load(data_file)

    curent_interval_timestamp = 1531912200#todo сделать по параметру?
    i = 0
    id = 1
    while i<288: #1 день

        search_plots_factory = region_matrix.only_region_matrix_logic.Only_matrix_search_plots_factory()

        res = search_plots_factory.get_main_dataframe(brands=brand_dict,groups=group_dict,regions=region_dict,start=curent_interval_timestamp,from_db=True)#надо сделать чтобы лидо запрос из бд, и результатом будет фрейм, который мы сохраняем в файл, либо считываем из файла фрейм
        if res == False:
            #i += 1
            curent_interval_timestamp += 300
            continue

        sales_plots_factory = elastic.elastic_queries_new_logic.Sales_plots_factory(search_plots_factory.filter_regions,curent_interval_timestamp,search_plots_factory, True)

        elastic_worker = Elastic_Result_Worker(curent_interval_timestamp)
        elastic_worker.get_elastic_data_presentation(sales_plots_factory.frame_all_searches_with_sales)

        #id = elastic_worker.walk_by_result_frame(id)
        #a = 1
        #i +=1
        curent_interval_timestamp += 300

if __name__== "__main__":
  main()






