from datetime import datetime
import numpy as np
import pandas as pd
import operator
import plotly.graph_objs as go
class Data_manipulator():
    def __init__(self,brands,groups,group_fields):
        self.brands = list(brands)
        self.groups = list(groups)
        self.group_fields = group_fields

    def get_serie_name(self,key):
        if len(self.group_fields)==1:
            if self.group_fields[0]=="br":

                return self.brands[int(key)]

            elif self.group_fields[0]=="gr":
                return self.groups[int(key)]

        elif len(self.group_fields) == 2:
            if self.group_fields[0]=="br":

                r = self.brands[int(key)]
            r += ". "
            if self.group_fields[1]=="gr":
                r += self.groups[int(key)]
            return r

    def calc_absolute_filtered(self, aggregations_sucsess, aggregations_total):

        graph_data = []
        length = len(aggregations_sucsess["3"]["buckets"])  # это группы, ряды групп по которым аггрегировали
        if length==0:
            graph_data.append({
                "x": np.asarray(0),
                "y": np.asarray(0),
                "mode": 'lines',
                "name": "Нет данных"
            })
            return graph_data

        buckets_in_serie = len(aggregations_sucsess["3"]["buckets"][0]["2"]["buckets"])
        y = np.zeros(shape=(length, buckets_in_serie), dtype=float)
        y2 = np.zeros(shape=(length, buckets_in_serie), dtype=float)

        time_values = []
        major = {}
        for group_i in range(0, length):
            group = aggregations_sucsess["3"]["buckets"][group_i]
            # result[group["key"]] = {}
            #x = []
            #y1 = []
            #y2 = []


            l2 = len(group["2"]["buckets"])  # это временной ряд

            for time_bucket_i in range(0, l2):
                time_bucket = group["2"]["buckets"][time_bucket_i]
                if group_i == 0:
                    time_values.append(datetime.utcfromtimestamp(time_bucket["key"]/1000).strftime('%Y:%m:%d:%H:%M'))
                total = aggregations_total["3"]["buckets"][group_i]["2"]["buckets"][time_bucket_i]["1"]["value"]
                sucsess = aggregations_sucsess["3"]["buckets"][group_i]["2"]["buckets"][time_bucket_i]["1"]["value"]

                #y1.append(total)#всего поисков
                y[group_i][time_bucket_i] = total
                if total==0:

                    y2[group_i][time_bucket_i] = np.nan
                else:
                    y2[group_i][time_bucket_i] = 1 - (sucsess / total)
                #y2.append(time_bucket["1"]["value"])#удачных поисков
            #sum_over_request_period = sum(y[group_i])



            major[group_i] = sum(y[group_i])

        #total = sum(major)
        series = [(k, major[k]) for k in sorted(major, key=major.get, reverse=True)]
        l2 = len(series)
        #cumulative_percent = 0
        for i in range(0, l2):
            val_container = series[i]
            #val = val_container[1]

            #group_percent = (val * 100) / total
            #cumulative_percent += group_percent

            graph_data.append({
                "x": np.asarray(time_values),
                "y": np.asarray(y2[val_container[0]]),
                "mode": 'lines',
                "name": self.get_serie_name(aggregations_sucsess["3"]["buckets"][val_container[0]]["key"])
            })
            if i ==10:
                break
            #if cumulative_percent > 95:
                #break


        return graph_data


    def calc_filtered(self,aggregations_sucsess,aggregations_total):
        #проийтись посчитать сумму по стобцу временного интервала п овсем группам(самому крайнему группируемому полю)
        #пройтись 1 раз и сложить в память этот столбец, отсортировать, далее идти п онему второй раз и считать % + считать нарастающий процент и когда он превысит 95 то прекратить, остановиться и
        graph_data = []
        length = len(aggregations_sucsess["3"]["buckets"])  # это группы, ряды групп по которым аггрегировали
        if length==0:
            graph_data.append({
                "x": np.asarray(0),
                "y": np.asarray(0),
                "mode": 'lines',
                "name": "Нет данных"
            })
            return graph_data


        time_values = []
        buckets_in_serie = len(aggregations_sucsess["3"]["buckets"][0]["2"]["buckets"])  # количество временных интервалов в ряду
        y = np.empty(shape=(length, buckets_in_serie),dtype=float)
        #y[:] = np.nan
        y.fill(np.nan)

        for time_bucket_i in range(0,buckets_in_serie):
            d = {}
            total = 0
            for group_i in range(0, length):
                if group_i==0:
                    time = aggregations_total["3"]["buckets"][group_i]["2"]["buckets"][time_bucket_i]["key"]
                    time_values.append(datetime.utcfromtimestamp(time/1000).strftime('%Y:%m:%d:%H:%M'))

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
            l2 = len(time_bucket)
            for i in range(0,l2):

                val_container = time_bucket[i]
                val = val_container[1]

                group_percent = (val * 100)/total


                #в нужную группу по нужному периоду кладем значение деленного удачных на всего

                #y[val_container[0]][time_bucket_i] = (aggregations_sucsess["3"]["buckets"][val_container[0]]["2"]["buckets"][time_bucket_i]["1"]["value"] / val)
                y[val_container[0]][time_bucket_i] = val

                cumulative_percent += group_percent
                if cumulative_percent > 95:
                    break
        #если в матрице есть все строки нулевые то эти группы мы не показываем!

        for group_i in range(0,y.shape[0]):

            if np.isnan(y[group_i]).all()==False:#np.any(y[group_i]):#если вся строка NAN
                #y[group_i][np.isnan(y[group_i])] = 0
                trace_val = go.Scatter(
                    x=np.asarray(time_values),
                    y=y[group_i],
                    mode='lines',
                    name=aggregations_sucsess["3"]["buckets"][group_i]["key"]
                )
                graph_data.append(trace_val)

        return graph_data
        #todo фильтровать по самым 95% неудачным из самых искомых!

    def calc_ratio(self,aggregations_sucsess,aggregations_total):

        graph_data = []
        length = len(aggregations_sucsess["3"]["buckets"])  # это группы, ряды групп по которым аггрегировали
        if length==0:
            graph_data.append({
                "x": np.asarray(0),
                "y": np.asarray(0),
                "mode": 'lines',
                "name": "Нет данных"
            })
            return graph_data

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
                    time_values.append(datetime.utcfromtimestamp(time_bucket["key"]/1000).strftime('%Y:%m:%d:%H:%M'))
                total = aggregations_total["3"]["buckets"][group_i]["2"]["buckets"][time_bucket_i]["1"]["value"]

                if total==0:
                    y.append(1)
                else:

                    y.append(1-(time_bucket["1"]["value"]/total))



            graph_data.append({
                "x": np.asarray(time_values),
                "y": np.asarray(y),
                "mode": 'lines',
                "name": group["key"]
            })


        return graph_data

    def calc_absolute(self, aggregations_sucsess, aggregations_total):

        graph_data = []
        length = len(aggregations_sucsess["3"]["buckets"])  # это группы, ряды групп по которым аггрегировали
        if length==0:
            graph_data.append({
                "x": np.asarray(0),
                "y": np.asarray(0),
                "mode": 'lines',
                "name": "Нет данных"
            })
            return graph_data

        time_values = []
        major = {}
        buckets_in_serie = len(aggregations_sucsess["3"]["buckets"][0]["2"]["buckets"])
        y = np.zeros(shape=(length, buckets_in_serie), dtype=float)
        y1 = np.zeros(shape=(length, buckets_in_serie), dtype=float)
        y2 = np.zeros(shape=(length, buckets_in_serie), dtype=float)

        for group_i in range(0, length):
            group = aggregations_sucsess["3"]["buckets"][group_i]
            # result[group["key"]] = {}
            x = []
            #y1 = []
            #y2 = []

            l2 = len(group["2"]["buckets"])  # это временной ряд

            for time_bucket_i in range(0, l2):
                time_bucket = group["2"]["buckets"][time_bucket_i]
                if group_i == 0:
                    time_values.append(datetime.utcfromtimestamp(time_bucket["key"] / 1000).strftime('%Y:%m:%d:%H:%M'))
                total = aggregations_total["3"]["buckets"][group_i]["2"]["buckets"][time_bucket_i]["1"]["value"]

                #y1.append(total)#всего поисков
                #y2.append(time_bucket["1"]["value"])#удачных поисков
                y[group_i][time_bucket_i] = total
                if total==0:
                    y1[group_i][time_bucket_i] = np.nan
                    y2[group_i][time_bucket_i] = np.nan

                else:

                    y1[group_i][time_bucket_i] = total#всего поисков
                    y2[group_i][time_bucket_i] = time_bucket["1"]["value"]#удачных поисков

            major[group_i] = sum(y[group_i])
            #x = time_values



        series = [(k, major[k]) for k in sorted(major, key=major.get, reverse=True)]
        l2 = len(series)
        # cumulative_percent = 0
        for i in range(0, l2):
            val_container = series[i]
            # val = val_container[1]

            # group_percent = (val * 100) / total
            # cumulative_percent += group_percent


            graph_data.append({
                "x": np.asarray(time_values),
                "y": np.asarray(y1[val_container[0]]),
                "mode": 'lines',
                "name": self.get_serie_name(aggregations_sucsess["3"]["buckets"][val_container[0]]["key"]) + ". Всего поисков"
            })

            graph_data.append({
                "x": np.asarray(time_values),
                "y": np.asarray(y2[val_container[0]]),
                "mode": 'lines',
                "name": self.get_serie_name(aggregations_sucsess["3"]["buckets"][val_container[0]]["key"]) + ". Удачных поисков"
            })
            if i == 10:
                break


        return graph_data

    def calc_diff(self,aggregations_sucsess,aggregations_total):

        graph_data = []
        length = len(aggregations_sucsess["3"]["buckets"])  # это группы, ряды групп по которым аггрегировали

        time_values = []
        major = {}
        if length==0:
            graph_data.append({
                "x": np.asarray(0),
                "y": np.asarray(0),
                "mode": 'lines',
                "name": "Нет данных"
            })
            return graph_data

        buckets_in_serie = len(aggregations_sucsess["3"]["buckets"][0]["2"]["buckets"])
        y = np.zeros(shape=(length, buckets_in_serie), dtype=float)
        y1 = np.zeros(shape=(length, buckets_in_serie), dtype=float)



        for group_i in range(0, length):
            group = aggregations_sucsess["3"]["buckets"][group_i]
            #result[group["key"]] = {}
            #x = []
            #y = []

            l2 = len(group["2"]["buckets"])  # это временной ряд

            for time_bucket_i in range(0, l2):
                time_bucket = group["2"]["buckets"][time_bucket_i]
                if group_i==0:
                    time_values.append(datetime.utcfromtimestamp(time_bucket["key"]/1000).strftime('%Y:%m:%d:%H:%M'))
                total = aggregations_total["3"]["buckets"][group_i]["2"]["buckets"][time_bucket_i]["1"]["value"]

                y1[group_i][time_bucket_i] = total  # всего поисков
                if total==0:
                    #y.append(0)
                    y[group_i][time_bucket_i] = np.nan
                else:

                    #y.append(total - time_bucket["1"]["value"])

                    y[group_i][time_bucket_i] = total - time_bucket["1"]["value"]

            major[group_i] = sum(y1[group_i])


            #x = time_values

        series = [(k, major[k]) for k in sorted(major, key=major.get, reverse=True)]
        l2 = len(series)
        # cumulative_percent = 0
        for i in range(0, l2):
            val_container = series[i]
            # val = val_container[1]

            # group_percent = (val * 100) / total
            # cumulative_percent += group_percent
            graph_data.append({
                "x": np.asarray(time_values),
                "y": np.asarray(y[val_container[0]]),
                "mode": 'lines',
                "name": self.get_serie_name(aggregations_sucsess["3"]["buckets"][val_container[0]]["key"])
            })


            if i == 10:
                break

        return graph_data
