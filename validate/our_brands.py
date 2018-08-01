import pandas as pd
import json
import collections
#todo надо все бренды из списка приводить к верхнему регистру! + так же это делать в коде считающим поиски(из документов)!

goods_classifier = pd.read_csv("../searches/parts_with_sg_mg.csv", sep=';', names=["NSI", "Brand", "oemnumber", "name", "number", "group"])




with open('brand_dict.json') as data_file:
    brand_dict = json.load(data_file)
list1 = list(brand_dict)
new_dict = collections.OrderedDict()
for i in range(0,len(brand_dict),1):
    new_dict[list1[i].upper()] = i

result = {}
not_searched_result = {}
special_result = {}

df = pd.read_excel('our_brands.xlsx')
l = df.shape[0]
for row in range(0,l,1):
    raw_brand = df.iloc[row][1].upper()

    if raw_brand.find("(") != -1:
        special_result[raw_brand] = {}
    elif raw_brand in new_dict:
        result[raw_brand] = {}
    else:
        not_searched_result[raw_brand] = {}

with open('our_brand_dict.json', 'w') as fp:
    json.dump(result, fp)

with open('our_brand_not_searched_result_dict2.json', 'w') as fp:
    json.dump(not_searched_result, fp)






