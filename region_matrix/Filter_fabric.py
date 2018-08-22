from elastic.elastic_queries_new_logic import Base_Elastic

class Filter_fabric(Base_Elastic):

    def __init__(self,start_of_5_minutes_interval):
        self.from_timestamp = start_of_5_minutes_interval
        self.to_timestamp = self.from_timestamp + 300#5 минут

        self.filter = {}
        self.filter["articules"] = {}
        self.filter["regions"]  = {}
        self.filter["nsi"] = {}


    def get_query(self, q, list_of_args=list()):
        list_of_args.append(str(self.from_timestamp))

        query = q(list_of_args)
        return query

    def query(self,list_of_args):

        if len(list_of_args) == 1:
            gt = list_of_args[0]  # базовый от 1 июля
        else:
            gt = list_of_args[0]

        query = {
            "bool": {
                "must": [
                    {"range": {"timestamp": {"gt": gt, "lt": self.to_timestamp}}},

                    {
                        "match": {
                            "is_internal_user": False
                        }
                    },
                    {
                        "match": {
                            "event": "search"
                        }
                    },

                    {
                        "exists": {"field": "results_groups.search_results.brand_name"}
                    },
                    {
                        "exists": {"field": "results_groups.search_results.market_group_name"}
                    }

                ]

            }
        }

        return query


    def get_only_used_data_from_dictionaries(self):#вызывается извне
        q = self.query
        self.get_data(q)

    def do_logic_short(self, list_of_elements):
        self.do_logic(list_of_elements)
    def do_logic(self, list_of_elements):

        length = len(list_of_elements) - 1
        for i in range(0, length, 1):

            hit = list_of_elements[i]["_source"]
            # timestamp = hit["timestamp"]


            if "search_query" not in hit:
                continue
            #todo очистить артикул
            raw = hit["search_query"]
            without_spaces = raw.replace(" ", "")
            without_spaces_and_defises = without_spaces.replace("-", "")
            pure = without_spaces_and_defises.split("#")[0]


            self.filter["articules"][pure] = 1

            if "region" in hit:
                region = hit["region"]
                self.filter["regions"][region] = 1

                length2 = len(hit["results_groups"])

                for k in range(0, length2, 1):#по всем элементам results_groups, в каждом элементе один атрибут search_results
                    results_group_item = hit["results_groups"][k]

                    length3 = len(results_group_item["search_results"])


                    for j in range(0, length3, 1):
                        element_of_search_results = results_group_item["search_results"][j]


                        link_parts = element_of_search_results["link"].split("=")
                        nsi = link_parts[1]  # из ссылки извлекаем
                        # совпадение по бренду/товарной группе из поисковой строки

                        if len(link_parts) > 2:
                            a = 1

                        self.filter["nsi"][nsi] = 1