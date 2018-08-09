# a stacked bar plot with errorbars
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import json





#N = 5
#menMeans = [20, 35, 30, 35, 27]
#womenMeans = [25, 32, 34, 20, 25]

#ind = np.arange(N)    # the x locations for the groups
#width = 0.35       # the width of the bars: can also be len(x) sequence

result_df = pd.DataFrame(columns=[i for i in range(0,30)])
with open('brand_dict.json') as data_file:
    brand_dict = json.load(data_file)
with open('group_dict.json') as data_file:
    group_dict = json.load(data_file)
with open('region_dict.json') as data_file:
    region_dict = json.load(data_file)
main_frame = pd.read_csv('out_matrix.csv')
#slice = pd.read_excel("slice_sales_absolute_brand_group_matrix.xlsx")
frame_all_searches_with_sales = pd.read_csv("frame_all_searches_with_sales_matrix.csv")
group_dict_list = list(group_dict)
#df.sum(axis=1)
df = main_frame[(main_frame["region"]==1)&(main_frame["Search_result"]==1)].groupby(["brand"], as_index=False)['Search_result'].agg('count')
#df.sort_values(by=['Search_result'])
sorteddf = df.sort_values(by='Search_result', ascending=False)
top_brands_in_city = sorteddf.iloc[0:10,:]
brands = top_brands_in_city.shape[0]

for i in range(0,10):
    print(list(brand_dict)[top_brands_in_city.iloc[i][0]])

for i in range(0,brands):
    brand_n = top_brands_in_city.iloc[i][0]
    #по всем товарным группам в этом бренде идем
    #slice.loc[brand]
    #brand_n = brand_dict[brand]
    df1 = main_frame[(main_frame["region"] == 1) & (main_frame["Search_result"]==1) & (main_frame["brand"] == brand_n)].groupby(["group"], as_index=False)['Search_result'].agg('count')
    sorteddf2 = df1.sort_values(by=['Search_result'], ascending=False)
    top_groups_in_brand = sorteddf2.iloc[0:10, :]

    row = {}
    for i2 in range(0,top_groups_in_brand.shape[0]):
        group_n = top_groups_in_brand.iloc[i2][0]
        #group_n = group_dict[group]

        sucsess_in_group_total = top_groups_in_brand.iloc[i2]['Search_result']
        ended_with_sale = frame_all_searches_with_sales[(frame_all_searches_with_sales["region"]==1)&(frame_all_searches_with_sales["Search_result"]==1) & (frame_all_searches_with_sales["Sale"]==1) & (frame_all_searches_with_sales["brand"]==brand_n) & (frame_all_searches_with_sales["group"]==group_n)].shape[0]
        without_sale = sucsess_in_group_total-ended_with_sale

        row[len(row)] = ended_with_sale
        #row.append()
        row[len(row)] = without_sale
        #row.append()
        row[len(row)] = group_dict_list[group_n]
        #row.append(group_dict_list[group_n])#имя группы

    #result_df.loc[len(result_df)] = row
    result_df = result_df.append(row, ignore_index=True)
result_df.fillna(0, inplace=True)
#строка -бренд по 3 колонки на товарную группу, 1 - число с продажей, 2 - число с без продажи, 3 - имя группы
#узнать топ список брендов
#result_df.to_excel("result.xlsx")
#result_df.to_csv("result.csv")

#----------------------------------------------
def autolabel(rects,labels):
    # attach some text labels
    #for i in range(0, len(labels)):
    i = 0
    for rect in rects:
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width() / 2, height + .05, labels[i], ha = 'center', va='bottom')
        i += 1

#проход по столбцу по всем брендам
x = [i for i in range(0, len(top_brands_in_city))]
width = 0.8 # width of the bars
fig, ax = plt.subplots()

#for triple in range(0,result_df.shape[1],3):

c = 0
for col in range(0,result_df.shape[1]):
    if c == 2:
        c = 0
        continue
    freq = []
    labels = []
    for row in range(0, result_df.shape[0]):
        if c == 0 or c == 1:
            val = result_df.iloc[row][col]
            freq.append(val)
            if c == 0:
                labels.append(str(val) + ", " + str(result_df.iloc[row][col + 2]))
            elif c == 1:
                labels.append(str(val) + ", " + str(result_df.iloc[row][col + 1]))

    rects1 = ax.bar(x, freq, width, color='r')

    autolabel(rects1, labels)

    c += 1


#ax.set_ylim(0,450)
ax.set_ylabel('Frequency')
ax.set_title('Insert Title Here')
ax.set_xticks(np.add(x,(width/2))) # set the position of the x ticks
ax.set_xticklabels(list(top_brands_in_city))
plt.show()
"""

узнать в этих брендах товарные группы
это эксель файл по удачным поискам в абсолютных величинах
идем по колонкам - товарным группам первый цикл
второй цикл внутри по брендам в строку заполняем все бренды в одной товарной группе

имеем общее число из него надо вычесть в и получить число продаж в этом бренде и в этой товарной группе - запрос во фрейм

menMeans - Товарная группа с пере
нужно получить


по колонкам результирующего фрейма пойдем и будем каждую колонку добавлять как сисок, имена брендов это будет индекс фреймаили доп колонка

p1 = plt.bar(ind, menMeans, width, color='#d62728')
p2 = plt.bar(ind, womenMeans, width,bottom=menMeans)

plt.ylabel('Scores')
plt.title('Scores by group and gender')
plt.xticks(ind, ('G1', 'G2', 'G3', 'G4', 'G5'))
plt.yticks(np.arange(0, 81, 10))
plt.legend((p1[0], p2[0]), ('Men', 'Women'))

plt.show()
"""