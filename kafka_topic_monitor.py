#!/usr/bin/python
# -*- coding: utf-8 -*-
# @Time    : 2018/9/28 10:21
# @Author  : JY.Liu
# @Site    : http://github.com/lh1993
# @Mail    ：lhln0119@163.com
# @File    : kafka_topic_monitor.py
# @Software: PyCharm

import sys
import requests
from lxml import etree
from fake_useragent import UserAgent


class Kafka_topic():

    def __init__(self):
        self.ua = UserAgent()
        self.headers = {
            "Referer": "http://xxx.xxx.com/clusters/xxx",
            "User-Agent": self.ua.chrome}

    def get_topic_list(self):
        """获取Topic列表"""
        url = "http://xxx.xxx.com/clusters/xxx/topics"
        res = requests.get(url=url, headers=self.headers)
        html = etree.HTML(res.text)
        # print html.xpath('//a/@href')
        topic_list = html.xpath('//td[@class="\n    \n"]/a/text()')

        return topic_list

    def get_topic_metrics(self, topic_name):
         """获取Topic Manager中metrics信息"""
        topic_metrics = {}
        url = "http://xxx.xxx.com/clusters/xxx/topics/%s" % topic_name
        res = requests.get(url=url, headers=self.headers)
        html = etree.HTML(res.text)

        bytes_rejected = html.xpath(
            '//html/body/div[2]/div/div/div[2]/div[1]/div[2]/table/tbody/tr[7]/td[1]/text()')
        bytes_rejected_mean = html.xpath(
            '//html/body/div[2]/div/div/div[2]/div[1]/div[2]/table/tbody/tr[7]/td[2]/span/text()')
        bytes_rejected_min1 = html.xpath(
            '//html/body/div[2]/div/div/div[2]/div[1]/div[2]/table/tbody/tr[7]/td[3]/span/text()')
        bytes_rejected_min5 = html.xpath(
            '//html/body/div[2]/div/div/div[2]/div[1]/div[2]/table/tbody/tr[7]/td[4]/span/text()')
        bytes_rejected_min15 = html.xpath(
            '//html/body/div[2]/div/div/div[2]/div[1]/div[2]/table/tbody/tr[7]/td[5]/span/text()')

        failed_fetch_request = html.xpath(
            '//html/body/div[2]/div/div/div[2]/div[1]/div[2]/table/tbody/tr[9]/td[1]/text()')
        failed_fetch_mean = html.xpath(
            '//html/body/div[2]/div/div/div[2]/div[1]/div[2]/table/tbody/tr[9]/td[2]/span/text()')
        failed_fetch_min1 = html.xpath(
            '//html/body/div[2]/div/div/div[2]/div[1]/div[2]/table/tbody/tr[9]/td[3]/span/text()')
        failed_fetch_min5 = html.xpath(
            '//html/body/div[2]/div/div/div[2]/div[1]/div[2]/table/tbody/tr[9]/td[4]/span/text()')
        failed_fetch_min15 = html.xpath(
            '//html/body/div[2]/div/div/div[2]/div[1]/div[2]/table/tbody/tr[9]/td[5]/span/text()')

        failed_produce_request = html.xpath(
            '//html/body/div[2]/div/div/div[2]/div[1]/div[2]/table/tbody/tr[11]/td[1]/text()')
        failed_produce_mean = html.xpath(
            '//html/body/div[2]/div/div/div[2]/div[1]/div[2]/table/tbody/tr[11]/td[2]/span/text()')
        failed_produce_min1 = html.xpath(
            '//html/body/div[2]/div/div/div[2]/div[1]/div[2]/table/tbody/tr[11]/td[3]/span/text()')
        failed_produce_min5 = html.xpath(
            '//html/body/div[2]/div/div/div[2]/div[1]/div[2]/table/tbody/tr[11]/td[4]/span/text()')
        failed_produce_min15 = html.xpath(
            '//html/body/div[2]/div/div/div[2]/div[1]/div[2]/table/tbody/tr[11]/td[5]/span/text()')

        topic_metrics["topic_name"] = topic_name
        topic_metrics[bytes_rejected[0]] = [bytes_rejected_mean[0],
                                                 bytes_rejected_min1[0],
                                                 bytes_rejected_min5[0],
                                                 bytes_rejected_min15[0]]
        topic_metrics[failed_fetch_request[0]] = [failed_fetch_mean[0],
                                                       failed_fetch_min1[0],
                                                       failed_fetch_min5[0],
                                                       failed_fetch_min15[0]]
        topic_metrics[failed_produce_request[0]] = [failed_produce_mean[0],
                                                         failed_produce_min1[0],
                                                         failed_produce_min5[0],
                                                         failed_produce_min15[0]]

        return topic_metrics

    def get_all_topic_metrics(self):
        """所有Topic metrics信息汇总成一个列表"""
        all_topic_metrics = []
        topic_list = self.get_topic_list()
        for topic_name in topic_list:
            topic_metrics =  self.get_topic_metrics(topic_name)
            all_topic_metrics.append(topic_metrics)

        return  all_topic_metrics

    def main(self, topic, item):
        """监控指定Topic的某项指标"""
        topic_metrics_list = self.get_all_topic_metrics()
        for topic_name in topic_metrics_list:
            if topic_name['topic_name'] == topic:
                if topic_name[item] != ['0.00', '0.00', '0.00', '0.00']:
                    print 0
                else:
                    print 1

if __name__ == "__main__":
    topic = sys.argv[1]
    item = sys.argv[2]
    kafka = Kafka_topic()
    kafka.main(topic, item)
