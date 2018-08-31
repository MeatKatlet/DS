from datetime import datetime
import numpy as np

class Data_manipulator():

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
