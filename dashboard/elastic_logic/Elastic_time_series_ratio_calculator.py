from elastic.elastic_queries_new_logic import Base_Elastic

class Elastic_time_series_ratio_calculator(Base_Elastic):

    def __init__(
            self,
            from_timestamp="1531932754757",
            to_timestamp = "1531950998617",
            filtered_regions = list(),
            filtered_brand = list(),
            filtered_groups = list(),
            field_for_grouping = "group",
            metric_field = "sucsess_searches",
            interval_to_aggregate="5m"

    ):

        self.from_timestamp = from_timestamp*1000
        self.to_timestamp = to_timestamp*1000
        self.filtered_regions = filtered_regions
        self.filtered_brand = filtered_brand
        self.filtered_groups = filtered_groups

        self.field_for_grouping = field_for_grouping

        self.metric_field = metric_field

        self.interval_to_aggregate = interval_to_aggregate

        self.index = "prod_i_m"
        self.data_elastic_url = "http://bielastic.rossko.local:9200/"

        self.result = {}

    def get_query(self, q, list_of_args=list()):
        #list_of_args.append(str(self.from_timestamp))

        query = q(list_of_args)
        return query

    def query_for_searches(self,list_of_args):
        query = {
            "bool": {
                "must": [
                    {"range":{"@timestamp":{"gte":str(self.from_timestamp),"lte":str(self.to_timestamp),"format":"epoch_millis"}}}


                ]
            }
        }

        if len(self.filtered_regions)>0:
            query["bool"]["must"].append(
                {
                        "terms": {
                            "@region": self.filtered_regions
                        }
                }
            )
        if len(self.filtered_brand)>0:
            query["bool"]["must"].append(
                {
                        "terms": {
                            "brand": self.filtered_brand
                        }
                }
            )

        if len(self.filtered_groups)>0:
            query["bool"]["must"].append(
                {
                        "terms": {
                            "group": self.filtered_groups
                        }
                }
            )

        return query

    #группирует по товарной группе!
    def get_aggs(self):
        agg = {
            "3": {
                "terms": {
                    "field": str(self.field_for_grouping),
                    "size": 500,
                    "order": {"_term": "desc"},
                    "min_doc_count": 1
                },
                "aggs": {
                    "2": {
                        "date_histogram": {
                            "interval": str(self.interval_to_aggregate),
                            "field": "@timestamp",
                            "min_doc_count": 0,
                            "extended_bounds": {"min": str(self.from_timestamp), "max": str(self.to_timestamp)},
                            "format": "epoch_millis"
                        },
                        "aggs": {
                            "1": {
                                "sum": {
                                    "field": self.metric_field,
                                    "missing": 0
                                }
                            }
                        }
                    }
                }
            }
        }
        return agg

    #def do_logic_short(self, aggregations):
        #self.do_logic(list_of_elements)

    def do_logic(self, aggregations):
        self.result = aggregations


