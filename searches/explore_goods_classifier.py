import numpy as np
import warnings
import pandas as pd
import elastic.elastic_queries_copy
#from elastic.elastic_queries import Search_plots_factory
#from elastic.elastic_queries import Sales_plots_factory
warnings.filterwarnings('ignore')


"""
fp = open('parts_with_mng.csv')
for i, line in enumerate(fp):
    if i == 2279:
        a = 1
        break
fp.close()
"""
cols = ['GUID', 'Brand', 'Oemnumber', 'Name', 'MNG_ID', 'MNG_Name', 'Unnamed: 6', 'Unnamed: 7', 'Unnamed: 8', 'Unnamed: 9', 'Unnamed: 10', 'Unnamed: 11', 'Unnamed: 12', 'Unnamed: 13', 'Unnamed: 14', 'Unnamed: 15', 'Unnamed: 16', 'Unnamed: 17', 'Unnamed: 18', 'Unnamed: 19', 'Unnamed: 20', 'Unnamed: 21', 'Unnamed: 22', 'Unnamed: 23', 'Unnamed: 24', 'Unnamed: 25', 'Unnamed: 26', 'Unnamed: 27', 'Unnamed: 28', 'Unnamed: 29', 'Unnamed: 30', 'Unnamed: 31', 'Unnamed: 32', 'Unnamed: 33', 'Unnamed: 34', 'Unnamed: 35', 'Unnamed: 36', 'Unnamed: 37', 'Unnamed: 38', 'Unnamed: 39', 'Unnamed: 40', 'Unnamed: 41']

goods_classifier = pd.read_csv('parts_with_mng.csv', skiprows=1000, names=cols, nrows=3000,sep=';')

a = 1


