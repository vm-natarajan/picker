import re
import scrapy
import pandas as pd
import datetime


class NseSpider(scrapy.Spider):
    name = 'nse_spider'
    file_name = 'nse_spider.csv'

    def start_requests(self):
        f = open(self.file_name, "w+")
        f.close()
        domain_name = 'finance.yahoo.com'
        stock_details = pd.read_csv("resources/stock_details.csv")
        for stock_detail in range(len(stock_details) - 1):
            stock_id = stock_details['stock_id'][stock_detail]
            start_date = int(datetime.datetime.strptime(stock_details['start_date'][1], '%d-%b-%y').timestamp())
            end_date = int(datetime.datetime.strptime(stock_details['end_date'][2], '%d-%b-%y').timestamp())
            url_template = f'https://{domain_name}/quote/{stock_id}.NS/history?period1={start_date}&period2={end_date}'
            yield scrapy.Request(url=url_template, callback=self.parse)

    def parse(self, response):
        security_id = None
        security_name = re.search('\((\w+).NS\)', response.css("div[id$='QuoteHeader'] h1::text").get())
        if security_name:
            security_id = security_name.group(1)
        rows = response.xpath("//table[@*='historical-prices']//tbody//tr")
        for row in rows:
            yield {'security': security_id,
                   'date': row.xpath('td[1]//text()').extract_first(),
                   'close': row.xpath('td[5]//text()').extract_first(),
                   'volume': row.xpath('td[7]//text()').extract_first()
                   }