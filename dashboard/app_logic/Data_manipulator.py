from datetime import datetime
import numpy as np
import pandas as pd
import operator

class Data_manipulator():

    def calc_filtered(self,aggregations_sucsess,aggregations_total):
        #проийтись посчитать сумму по стобцу временного интервала п овсем группам(самому крайнему группируемому полю)
        #пройтись 1 раз и сложить в память этот столбец, отсортировать, далее идти п онему второй раз и считать % + считать нарастающий процент и когда он превысит 95 то прекратить, остановиться и
        graph_data = []
        length = len(aggregations_sucsess["3"]["buckets"])  # это группы, ряды групп по которым аггрегировали


        time_values = []
        buckets_in_serie = len(aggregations_sucsess["3"]["buckets"][0]["2"]["buckets"])  # количество временных интервалов в ряду
        y = np.zeros(shape=(length, buckets_in_serie))


        for time_bucket_i in range(0,buckets_in_serie):
            d = {}
            total = 0
            for group_i in range(0, length):
                if group_i==0:
                    time = aggregations_sucsess["3"]["buckets"][group_i]["2"]["buckets"][time_bucket_i]["key"]
                    time_values.append(datetime.utcfromtimestamp(time/1000).strftime('%H:%M'))

                total_sum_over_time_period = aggregations_total["3"]["buckets"][group_i]["2"]["buckets"][time_bucket_i]["1"]["value"]
                #time_bucket.append(total_sum_over_time_period)
                d[group_i] = total_sum_over_time_period
                total += total_sum_over_time_period

            if total == 0:
                continue

            #time_bucket = sorted(time_bucket, reverse=True)
            #time_bucket = sorted(d.items(), key=operator.itemgetter(0), reverse=True)#sort by value in desc keeping keys order!

            time_bucket = [(k, d[k]) for k in sorted(d, key=d.get, reverse=True)]

            cumulative_percent = 0

            for i in range(0,len(time_bucket)):

                val_container = time_bucket[i]
                val = val_container[1]

                group_percent = (val * 100)/total

                cumulative_percent += group_percent
                if cumulative_percent > 95:
                    break
                #в нужную группу по нужному периоду кладем значение деленного удачных на всего
                if val == 0:
                    y[val_container[0]][time_bucket_i] = 1
                else:
                    y[val_container[0]][time_bucket_i] = 1-(aggregations_sucsess["3"]["buckets"][val_container[0]]["2"]["buckets"][time_bucket_i]["1"]["value"] / val)
        #если в матрице есть все строки нулевые то эти группы мы не показываем!

        for group_i in range(0,y.shape[0]):

            if np.any(y[group_i]):#если вся строка не нулевая
                graph_data.append({
                    "x": np.asarray(time_values),
                    "y": y[group_i],
                    "mode": 'lines',
                    "name": aggregations_sucsess["3"]["buckets"][group_i]["key"]
                })

        return graph_data

    def calc_ratio(self,aggregations_sucsess,aggregations_total):

        graph_data = []
        length = len(aggregations_sucsess["3"]["buckets"])  # это группы, ряды групп по которым аггрегировали

        time_values = []

        for group_i in range(0, length):
            group = aggregations_sucsess["3"]["buckets"][group_i]
            #result[group["key"]] = {}
            x = []
            y = []

            l2 = len(group["2"]["buckets"])  # это временной ряд

            for time_bucket_i in range(0, l2):
                time_bucket = group["2"]["buckets"][time_bucket_i]
                if group_i==0:
                    time_values.append(datetime.utcfromtimestamp(time_bucket["key"]/1000).strftime('%H:%M'))
                total = aggregations_total["3"]["buckets"][group_i]["2"]["buckets"][time_bucket_i]["1"]["value"]

                if total==0:
                    y.append(1)
                else:

                    y.append(1-(time_bucket["1"]["value"]/total))

            x = time_values

            graph_data.append({
                "x": np.asarray(x),
                "y": np.asarray(y),
                "mode": 'lines',
                "name": group["key"]
            })


        return graph_data

    def calc_absolute(self, aggregations_sucsess, aggregations_total):

        graph_data = []
        length = len(aggregations_sucsess["3"]["buckets"])  # это группы, ряды групп по которым аггрегировали

        time_values = []

        for group_i in range(0, length):
            group = aggregations_sucsess["3"]["buckets"][group_i]
            # result[group["key"]] = {}
            x = []
            y1 = []
            y2 = []

            l2 = len(group["2"]["buckets"])  # это временной ряд

            for time_bucket_i in range(0, l2):
                time_bucket = group["2"]["buckets"][time_bucket_i]
                if group_i == 0:
                    time_values.append(datetime.utcfromtimestamp(time_bucket["key"] / 1000).strftime('%H:%M'))
                total = aggregations_total["3"]["buckets"][group_i]["2"]["buckets"][time_bucket_i]["1"]["value"]

                y1.append(total)#всего поисков
                y2.append(time_bucket["1"]["value"])#удачных поисков



            x = time_values

            graph_data.append({
                "x": np.asarray(x),
                "y": np.asarray(y1),
                "mode": 'lines',
                "name": group["key"]+". Всего поисков"
            })

            graph_data.append({
                "x": np.asarray(x),
                "y": np.asarray(y2),
                "mode": 'lines',
                "name": group["key"]+". Удачных поисков"
            })


        return graph_data

    def calc_diff(self,aggregations_sucsess,aggregations_total):

        graph_data = []
        length = len(aggregations_sucsess["3"]["buckets"])  # это группы, ряды групп по которым аггрегировали

        time_values = []

        for group_i in range(0, length):
            group = aggregations_sucsess["3"]["buckets"][group_i]
            #result[group["key"]] = {}
            x = []
            y = []

            l2 = len(group["2"]["buckets"])  # это временной ряд

            for time_bucket_i in range(0, l2):
                time_bucket = group["2"]["buckets"][time_bucket_i]
                if group_i==0:
                    time_values.append(datetime.utcfromtimestamp(time_bucket["key"]/1000).strftime('%H:%M'))
                total = aggregations_total["3"]["buckets"][group_i]["2"]["buckets"][time_bucket_i]["1"]["value"]

                if total==0:
                    y.append(1)
                else:

                    y.append(total - time_bucket["1"]["value"])

            x = time_values

            graph_data.append({
                "x": np.asarray(x),
                "y": np.asarray(y),
                "mode": 'lines',
                "name": group["key"]
            })


        return graph_data
