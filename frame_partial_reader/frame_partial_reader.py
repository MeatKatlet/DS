import numpy as np
import warnings
import pandas as pd
import json
import collections
import csv

class Frame_partial_reader():

    def __init__(self,filter=dict):
        self.parse_result = {}
        self.dublicates = 0
        self.dublicates2 = 0
        self.no_brand_or_group = 0
        self.brands = collections.OrderedDict()
        self.groups = collections.OrderedDict()
        self.articles = collections.OrderedDict()

        self.nsi_dict = pd.DataFrame(columns=["NSI","Article","Brand", "Group"])

        self.filter = filter

    def read4(self, path):
        goods_classifier = pd.read_csv(path, sep=';', names=["NSI", "Brand", "oemnumber", "name", "number", "group"])
        rows = goods_classifier.shape[0]
        for row in range(0,rows,1):
            r = goods_classifier.iloc[row]
            continue
            oemnumber = goods_classifier.iloc[row][2]
            brand = goods_classifier.iloc[row][1]
            group = goods_classifier.iloc[row][5]
            if brand != brand or group != group:  # проверка на Nan
                self.no_brand_or_group += 1
                continue
            if brand not in self.brands:
                # todo что делать с апострофом? в поиске выдача может быть вместе с ним
                brand = brand.replace(r"'", "").replace("\\", "")
                self.brands[brand] = len(self.brands)
            gs = group.strip()
            if gs not in self.groups:
                self.groups[gs] = len(self.groups)

            if oemnumber not in self.parse_result:

                self.parse_result[oemnumber] = {}
                self.parse_result[oemnumber][(self.brands[brand], self.groups[gs])] = 0  # в виде чисел будет ключ (код бренда, код группы)

            elif oemnumber in self.parse_result:
                self.parse_result[oemnumber][(self.brands[brand], self.groups[gs])] = 0  # в виде чисел будет ключ (код бренда, код группы)
            else:
                self.dublicates += 1

    def read_2(self, path):  # это для фреймов больше 1000
        #todo проверитиь уникальность кодов нси
        goods_classifier = pd.read_csv(path, sep=';', names=["NSI", "Brand", "oemnumber", "name", "number", "group"])
        fp = open(path, encoding='utf-8')
        # n = 0
        # no_brand_or_group = 0
        # parse_result = {}
        """
                parse_result[0][NSI 1] = id1 - номер соответствующего сочетания бренд/группа для этого NSI
                parse_result[0][NSI 2] = id2

                parse_result[1][id1] = (код бренда, код группы)
                parse_result[1][id2] = (код бренда, код группы)
                """
        # dublicates2 = 0
        # dublicates = 0
        prev_line = ''
        for i, line in enumerate(fp):

            if line[:3] == 'NSI':  # если первая позиция в строуке NSI то предыдущая строка закончилась

                #splitted = prev_line.split(";")
                if (prev_line.count('"') % 2) != 0:
                    prev_line = prev_line.replace(r'"', "")

                splitted  = list(csv.reader([prev_line],delimiter=';'))
                # n += 1
                nsi = splitted[0][1]
                oemnumber = splitted[0][2]
                brand = splitted[0][1]
                group = splitted[0][5]

                if brand != brand or group != group:  # проверка на Nan
                    self.no_brand_or_group += 1
                    continue

                # hex = str(hash(brand + group))

                if brand not in self.brands:
                    # todo что делать с апострофом? в поиске выдача может быть вместе с ним
                    #brand = brand.replace(r"'", "").replace("\\", "")
                    self.brands[brand] = len(self.brands)
                gs = group.strip()
                if gs not in self.groups:
                    self.groups[gs] = len(self.groups)

                if oemnumber not in self.articles:
                    self.articles[oemnumber] = len(self.articles)


                self.nsi_dict.loc[len(self.nsi_dict)] = [nsi,self.articles[oemnumber],self.brands[brand], self.groups[gs]]

                prev_line = line

            else:
                prev_line = prev_line + line

        fp.close()

        self.nsi_dict = self.nsi_dict.groupby(["NSI", "Brand", "oemnumber", "name", "number", "group"],as_index=False)


    def read(self, path):  # это для фреймов больше 1000

            #goods_classifier = pd.read_csv(path, sep=';',names=["NSI", "Brand", "oemnumber", "name", "number", "group"])
            fp = open(path, encoding='utf-8')

            """
                    parse_result[0][NSI 1] = id1 - номер соответствующего сочетания бренд/группа для этого NSI
                    parse_result[0][NSI 2] = id2

                    parse_result[1][id1] = (код бренда, код группы)
                    parse_result[1][id2] = (код бренда, код группы)
                    """
            # dublicates2 = 0
            # dublicates = 0
            prev_line = ''
            for i, line in enumerate(fp):

                if line[:3] == 'NSI':  # если первая позиция в строуке NSI то предыдущая строка закончилась

                    # splitted = prev_line.split(";")
                    #if (prev_line.count('"') % 2) != 0:
                    #    prev_line = prev_line.replace(r'"', "")
                    if prev_line=="":
                        prev_line = line
                        continue
                    splitted = list(csv.reader([prev_line], delimiter=';'))
                    # n += 1
                    nsi = splitted[0][0]#фильтровать!
                    oemnumber = splitted[0][2]#фильтровать!
                    brand = splitted[0][1]
                    group = splitted[0][3]



                    if oemnumber not in self.filter:#todo очистить заранее все артикулы!
                        prev_line = line
                        continue


                    if brand != brand or group != group:  # проверка на Nan
                        self.no_brand_or_group += 1
                        prev_line = line
                        continue

                    # hex = str(hash(brand + group))

                    if brand not in self.brands:
                        # todo что делать с апострофом? в поиске выдача может быть вместе с ним
                        # brand = brand.replace(r"'", "").replace("\\", "")
                        self.brands[brand] = len(self.brands)
                    gs = group.strip()
                    if gs not in self.groups:
                        self.groups[gs] = len(self.groups)

                    if oemnumber not in self.parse_result:
                        # d = dict()
                        # d['brand'] = brand
                        # d['group'] = group
                        self.parse_result[oemnumber] = [collections.OrderedDict(),collections.OrderedDict()]
                        #key = len(self.parse_result[oemnumber][0])
                        self.parse_result[oemnumber][0][nsi] = 0 #порядковый номер ключа (бренд, группа)
                        #self.parse_result[oemnumber][1] = collections.OrderedDict()
                        self.parse_result[oemnumber][1][(self.brands[brand],self.groups[gs])] = 0  # в виде чисел будет ключ (код бренда, код группы)

                    elif oemnumber in self.parse_result:
                        #key = len(self.parse_result[oemnumber][0])
                        if (self.brands[brand], self.groups[gs]) in self.parse_result[oemnumber][1]:
                            l = list(self.parse_result[oemnumber][1].keys())

                            ordered_numbers = dict(zip(l, range(len(l))))
                            position_of_key_in_list = ordered_numbers[(self.brands[brand], self.groups[gs])]

                        else:
                            position_of_key_in_list = len(self.parse_result[oemnumber][1])
                            self.parse_result[oemnumber][1][(self.brands[brand], self.groups[gs])] = 0  # в виде чисел будет ключ (код бренда, код группы)


                        self.parse_result[oemnumber][0][nsi] = position_of_key_in_list#порядковый номер ключа (бренд, группа)


                    else:
                        self.dublicates += 1

                    prev_line = line

                else:
                    prev_line = prev_line + line

            fp.close()



        # with open('numbers_dictionary.json', 'w') as fp:
        #   json.dump(self.parse_result, fp)

    """
    

    def read3(self,path):#это для фреймов больше 1000
        cols = ['GUID', 'Brand', 'Oemnumber', 'Name', 'MNG_ID', 'MNG_Name', 'Unnamed: 6', 'Unnamed: 7', 'Unnamed: 8',
                'Unnamed: 9', 'Unnamed: 10', 'Unnamed: 11', 'Unnamed: 12', 'Unnamed: 13', 'Unnamed: 14', 'Unnamed: 15',
                'Unnamed: 16', 'Unnamed: 17', 'Unnamed: 18', 'Unnamed: 19', 'Unnamed: 20', 'Unnamed: 21', 'Unnamed: 22',
                'Unnamed: 23', 'Unnamed: 24', 'Unnamed: 25', 'Unnamed: 26', 'Unnamed: 27', 'Unnamed: 28', 'Unnamed: 29',
                'Unnamed: 30', 'Unnamed: 31', 'Unnamed: 32', 'Unnamed: 33', 'Unnamed: 34', 'Unnamed: 35', 'Unnamed: 36',
                'Unnamed: 37', 'Unnamed: 38', 'Unnamed: 39', 'Unnamed: 40', 'Unnamed: 41']

        goods_classifier = pd.read_csv(path, nrows=1000, names=cols, sep=';', header=None,error_bad_lines=False)

        skip = 1000
        while goods_classifier.shape[0]==1000:

            self.parse_part(goods_classifier)

            goods_classifier = pd.read_csv(path, skiprows = skip, names=cols, nrows = 1000, sep=';',header=None,error_bad_lines=False)
            skip+=1000
    def parse_part(self,goods_classifier):
        rows = goods_classifier.shape[0]
        for row in range(0,rows,1):
            oemnumber = goods_classifier.iloc[row][2]
            brand = goods_classifier.iloc[row][1]
            group = goods_classifier.iloc[row][5]

            if brand !=brand or group != group:#проверка на Nan
                self.no_brand_or_group += 1
                continue

            #hex = str(hash(brand+group))
            if oemnumber not in self.parse_result:
                #d = dict()
                #d['brand'] = brand
                #d['group'] =  group
                self.parse_result[oemnumber] = {}
                self.parse_result[oemnumber][brand] = group
            elif oemnumber in self.parse_result:

                if brand not in self.parse_result[oemnumber] or (brand in self.parse_result[oemnumber] and self.parse_result[oemnumber][brand]!=group):#такого бренда нет и мы хотим добаватьь еще один или бренд есть но хотим добавить еще одну группу к нему
                    self.parse_result[oemnumber]["2"] = 1
                    self.dublicates2 += 1#хотим добавить еще один бренд или под существующим брендом еще одну товарную группу

            else:
                self.dublicates += 1


    def parse_part2(self,goods_classifier):
        rows = goods_classifier.shape[0]
        for row in range(0,rows,1):
            oemnumber = goods_classifier.iloc[row][2]
            brand = goods_classifier.iloc[row][1]
            group = goods_classifier.iloc[row][5]

            if brand !=brand or group != group:#проверка на Nan
                self.no_brand_or_group += 1
                continue

            hex = str(hash(brand+group))
            if oemnumber not in self.parse_result:
                d = dict()
                d['brand'] = brand
                d['group'] =  group
                self.parse_result[oemnumber] = {}
                self.parse_result[oemnumber][hex] = d
            elif oemnumber in self.parse_result and hex not in self.parse_result[oemnumber]:

                d = dict()
                d['brand'] = brand
                d['group'] = group
                self.parse_result[oemnumber][hex] = d
            else:
                self.dublicates += 1
    """


#под одним номером(артикулом) могут быть разные бренды и разные маркетинговые группы!