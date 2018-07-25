import numpy as np
import warnings
import pandas as pd
import elastic.elastic_queries_copy
#from elastic.elastic_queries import Search_plots_factory
#from elastic.elastic_queries import Sales_plots_factory
warnings.filterwarnings('ignore')



fp = open('parts_with_mng.csv',encoding='utf-8')
#n = 0
no_brand_or_group = 0
parse_result = {}
dublicates2 = 0
dublicates = 0
prev_line = ''
for i, line in enumerate(fp):

    if line[:3] =='NSI':#если первая позиция в строуке NSI то предыдущая строка закончилась

        splitted = prev_line.split(";")
        #n += 1
        oemnumber = splitted[2]
        brand = splitted[1]
        group = splitted[5]

        if brand != brand or group != group:  # проверка на Nan
            no_brand_or_group += 1
            continue

        # hex = str(hash(brand+group))
        if oemnumber not in parse_result:
            # d = dict()
            # d['brand'] = brand
            # d['group'] =  group
            parse_result[oemnumber] = {}
            parse_result[oemnumber][brand] = group
        elif oemnumber in parse_result:

            if brand not in parse_result[oemnumber] or (brand in parse_result[oemnumber] and parse_result[oemnumber][brand] != group):  # такого бренда нет и мы хотим добаватьь еще один или бренд есть но хотим добавить еще одну группу к нему self.parse_result[oemnumber]["2"] = 1
                dublicates2 += 1  # хотим добавить еще один бренд или под существующим брендом еще одну товарную группу

        else:
            dublicates += 1

        prev_line = line

    else:
        prev_line = prev_line+line

fp.close()

cols = ['GUID', 'Brand', 'Oemnumber', 'Name', 'MNG_ID', 'MNG_Name', 'Unnamed: 6', 'Unnamed: 7', 'Unnamed: 8', 'Unnamed: 9', 'Unnamed: 10', 'Unnamed: 11', 'Unnamed: 12', 'Unnamed: 13', 'Unnamed: 14', 'Unnamed: 15', 'Unnamed: 16', 'Unnamed: 17', 'Unnamed: 18', 'Unnamed: 19', 'Unnamed: 20', 'Unnamed: 21', 'Unnamed: 22', 'Unnamed: 23', 'Unnamed: 24', 'Unnamed: 25', 'Unnamed: 26', 'Unnamed: 27', 'Unnamed: 28', 'Unnamed: 29', 'Unnamed: 30', 'Unnamed: 31', 'Unnamed: 32', 'Unnamed: 33', 'Unnamed: 34', 'Unnamed: 35', 'Unnamed: 36', 'Unnamed: 37', 'Unnamed: 38', 'Unnamed: 39', 'Unnamed: 40', 'Unnamed: 41']

goods_classifier = pd.read_csv('parts_with_mng.csv', skiprows=1000, names=cols, nrows=3000,sep=';')

a = 1


