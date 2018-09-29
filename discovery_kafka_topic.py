#!/usr/bin/python
# -*- coding: utf-8 -*-
# @Time    : 2018/9/28 10:21
# @Author  : JY.Liu
# @Site    : http://github.com/lh1993
# @Mail    ：lhln0119@163.com
# @File    : kafka_topic_monitor.py
# @Software: PyCharm

import requests
import json
from lxml import etree
from fake_useragent import UserAgent


class Kafka_topic():

    def __init__(self):
        self.ua = UserAgent()
        self.headers = {
            "Referer": "http://xxx.xxx.com/clusters/xxx",
            "User-Agent": self.ua.chrome}
        self.topic_metrics_dict = {}
        self.topic_metrics_list = []

    def get_topic_list(self):
        """获取Topic列表"""
        url = "http://xxx.xx.com/clusters/xxx/topics"
        res = requests.get(url=url, headers=self.headers)
        html = etree.HTML(res.text)
        # print(type(html))
        # print html.xpath('//a/@href')
        topic_list = html.xpath('//td[@class="\n    \n"]/a/text()')
        # print topic_list
        for topic_name in topic_list:
            topic_dict = {}
            topic_dict["{#TOPIC_NAME}"] = topic_name
            topic_dict["{#Bytes_rejected}"] = "Bytes rejected /sec"
            topic_dict["{#Failed_fetch}"] = "Failed fetch request /sec"
            topic_dict["{#Failed_produce}"] = "Failed produce request /sec"
            self.topic_metrics_list.append(topic_dict)

        return self.topic_metrics_list

    def main(self):
        """生成json格式数据"""
        self.topic_metrics_dict['data'] = self.get_topic_list()
        print type(json.dumps(self.topic_metrics_dict))

if __name__ == "__main__":
    kafka = Kafka_topic()
    kafka.main()
