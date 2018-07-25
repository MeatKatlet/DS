import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import warnings
import pandas as pd
import elastic.elastic_queries_copy
#from elastic.elastic_queries import Search_plots_factory
#from elastic.elastic_queries import Sales_plots_factory
from matplotlib.patches import Rectangle
warnings.filterwarnings('ignore')



search_plots_factory = elastic.elastic_queries_copy.Search_plots_factory()

search_plots_factory.get_main_dataframe(False)#надо сделать чтобы лидо запрос из бд, и результатом будет фрейм, который мы сохраняем в файл, либо считываем из файла фрейм



sales_plots_factory = elastic.elastic_queries_copy.Sales_plots_factory(search_plots_factory, True)

sales_plots_factory.get_statistics_of_serches_and_sales()