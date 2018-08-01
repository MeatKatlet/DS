from xml.etree import ElementTree as ET
import datetime
import time



class Availebness_in_1c():

    def __init__(self):


        self.presence_in_1c = {}
        self.dublicate = 0

    def parse_xml_fill_dictionary(self,path):
        xml = ET.parse(path)#vigruzka.xml
        root_element = xml.getroot()

        current_date = root_element.get("ДатаВыгрузки")

        for child in root_element:#по элементам - склад

            goods_in_store = iter(list(child))#итератор по тегам товар!
            for good in goods_in_store:
                nsi_attr = good.get("nsi_attr")
                article_attr = good.get("article_attr")
                brand_attr = good.get("brand_attr")
                group_attr = good.get("group_attr")

                if nsi_attr not in self.presence_in_1c:
                    self.presence_in_1c[nsi_attr] = []
                    # self.presence_in_1c[nsi_attr]["brand"] = brand_attr
                    # self.presence_in_1c[nsi_attr]["group"] = brand_attr
                    self.presence_in_1c[nsi_attr] = []

                else:
                    self.dublicate +=1


                intervals_of_good = iter(list(good))
                for interval in intervals_of_good:
                    start = interval.get("start")
                    end = interval.get("end")

                    if end == "0001-01-01T00:00:00":#это значит по текущую дату
                        end = current_date#дата на момент выгрузки


                    end_timestamp = int(time.mktime(datetime.datetime.strptime(end, "%Y-%m-%dT%H:%M:%S").timetuple()))
                    start_timestamp = int(time.mktime(datetime.datetime.strptime(start, "%Y-%m-%dT%H:%M:%S").timetuple()))

                    self.presence_in_1c[nsi_attr].append([start_timestamp, end_timestamp])

    def check_good_availible(self,timestamp,nsi_list):
        t = len(nsi_list)
        r = False
        for i in range(0,t):
            nsi = nsi_list[i]
            if nsi in self.presence_in_1c:
                intervals = len(self.presence_in_1c[nsi])
                for key in range(0,intervals):
                    start = self.presence_in_1c[nsi][key][0]
                    end = self.presence_in_1c[nsi][key][1]
                    if timestamp<end and timestamp > start:
                        return True

        return r

"""
1.надо переделать функцию проверяющую присутствие в выдаче искомого товара, проверка должа идти по NSI, надо извлекать NSI из каждой строчки товара и проверять его присутствие в списке NSI извлеченном из запроса-артикула

2. надо добавлять к результирующей таблице(таблице до момента слияния строк по пагинации) колонку timestamp(выдача на момент времени),
 колонку  validate_result в уже объединенную таблицу(1/0) 1 - это значит проверенное наличие хотя бы одного товара из выдачи(серии страниц выдачи).
Будем проходить по таблице до объединения и ставить результат в таблицу после объединения!

3. Проверять cross tab будем проходя по каждой ячейке - берем бренд - товарную группу, или просто бренд- отправляем эти параметры в запрос в main_frame с колонкой validate, будем вычислять проценты и сравнивать 

"""
