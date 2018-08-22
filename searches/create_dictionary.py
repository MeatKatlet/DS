import pandas as pd
import csv

#todo очистить справочник от грязных артикулов
globvar = 0

def clean_articul(articul):
    global globvar
    raw = articul
    if articul != articul:
       return articul
    without_spaces = raw.replace(" ", "")
    without_spaces_and_defises = without_spaces.replace("-", "")
    pure = without_spaces_and_defises.split("#")[0]
    globvar += 1
    return pure
"""
df = pd.read_csv("matrixNom.csv", sep=';')

df.drop(columns=['Nom_Name', 'SKlad_Code',"SKlad_Name","CFO_Code","NomGr","MarkGr","SinMarkGr","Brand"], axis=1, inplace=True)

df['Artikul'] = df['Artikul'].apply(clean_articul)

df.to_csv("region_matrix_pure.csv",sep=';', index=False,header=False)

a = 1

"""
all_searches_dict = {}

fp = open("matrixNom.csv", encoding='utf-8')
row_count = 0
prev_line = ''
i2 = 0
for i, line in enumerate(fp):
    #не верная!
    if line.find('NSI') != -1:
    #if line[:3] == 'NSI':  # если первая позиция в строуке NSI то предыдущая строка закончилась
        if i2==0:
            i2 += 1
            prev_line = line
            continue


        if prev_line == "":
            prev_line = line
            continue
        prev_line = prev_line.replace("\n", "")
        prev_line = prev_line[prev_line.find('NSI'):]
        splitted = list(csv.reader([prev_line], delimiter=';'))

        nsi = splitted[0][0]  # фильтровать!
        oemnumber = splitted[0][1]  # фильтровать!
        region = splitted[0][5]
        oemnumber = clean_articul(oemnumber)
        all_searches_dict[row_count] = [
            nsi,
            oemnumber,
            region
        ]
        row_count +=1
        prev_line = line

    else:
        prev_line = prev_line + line

fp.close()

tmpframe = pd.DataFrame.from_dict(all_searches_dict, orient='index')
tmpframe.to_csv("region_matrix_pure.csv",sep=';', index=False,header=False)


df = pd.read_csv("parts_with_sg_mg.csv", sep=';',header=None)

df.drop(df.columns[[3,4]], axis=1, inplace=True)

df[df.columns[2]] = df[df.columns[2]].apply(clean_articul)

df.to_csv("nsi_goods_classifier_pure.csv",sep=';', index=False,header=False)

a = 1

