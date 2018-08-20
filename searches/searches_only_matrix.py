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
    #todo надо доделать в get_main_dataframe сохранени/восстановление timestamp
    search_plots_factory.get_main_dataframe(from_db=True)#надо сделать чтобы лидо запрос из бд, и результатом будет фрейм, который мы сохраняем в файл, либо считываем из файла фрейм
    #по каждому сочетанию сделать heatmap


    """
    #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    f = elastic.elastic_queries_new_logic.region_brand
    search_plots_factory.slice_col1 = "region"
    search_plots_factory.slice_col2 = "brand"
    search_plots_factory.create_heatmap(plot_heatmap, f, '% удачных поисков. Регион vs Бренд', (31, 22))

    # ++++++++++++++++++++++
    f = elastic.elastic_queries_new_logic.region_group
    search_plots_factory.slice_col1 = "region"
    search_plots_factory.slice_col2 = "group"
    search_plots_factory.create_heatmap(plot_heatmap, f, '% удачных поисков. Регион vs Товарная группа', (90, 22))

    # +++++++++++++++++++++++++++++++++
    f = elastic.elastic_queries_new_logic.brand_group
    search_plots_factory.slice_col1 = "brand"
    search_plots_factory.slice_col2 = "group"
    search_plots_factory.create_heatmap(plot_heatmap, f, '% удачных поисков, Бренд vs Товарная группа', (90, 44))
    """


    #теперь построим хитмапы на по % поисков завершенных продажами в 3 разрезах тоже

    sales_plots_factory = elastic.elastic_queries_new_logic.Sales_plots_factory(search_plots_factory, True)

    sales_plots_factory.get_statistics_of_serches_and_sales()
    f = elastic.elastic_queries_new_logic.region_brand
    sales_plots_factory.slice_col1 = "region"
    sales_plots_factory.slice_col2 = "brand"
    sales_plots_factory.create_heatmap(plot_heatmap,f,'% поисков завершенных продажей. Регион vs Бренд',(30, 22))#это % из удачных поисков завершенных продажей
    f = elastic.elastic_queries_new_logic.region_group
    sales_plots_factory.slice_col1 = "region"
    sales_plots_factory.slice_col2 = "group"
    sales_plots_factory.create_heatmap(plot_heatmap,f, '% поисков завершенных продажей. Регион vs Товарная группа',(90, 22))
    f = elastic.elastic_queries_new_logic.brand_group
    sales_plots_factory.slice_col1 = "brand"
    sales_plots_factory.slice_col2 = "group"
    sales_plots_factory.create_heatmap(plot_heatmap,f,'% поисков завершенных продажей. Бренд vs Товарная группа',(90, 44))

    a = 1

if __name__== "__main__":
  main()






