import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import warnings
import pandas as pd
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
def plot_heatmap():
    return



def main():

    search_plots_factory = region_matrix.only_region_matrix_logic.Only_matrix_search_plots_factory()

    search_plots_factory.get_main_dataframe(from_db=True)#надо сделать чтобы лидо запрос из бд, и результатом будет фрейм, который мы сохраняем в файл, либо считываем из файла фрейм




    sales_plots_factory = elastic.elastic_queries_new_logic.Sales_plots_factory(search_plots_factory, True)



    a = 1

if __name__== "__main__":
  main()






