# -*- coding: utf-8 -*-
import scrapy
import time
import re
import hashlib

class HacknewsSpider(scrapy.Spider):
    name = "hacknews"
    allowed_domains = ["https://news.ycombinator.com/"]
    start_urls = (
        'https://news.ycombinator.com/',
    )

    def parse(self, response):

        baseUrl="https://news.ycombinator.com/"
        news_list=response.css("td.title")
        for news in news_list:
            title=news.css('a[class=storylink]::text').extract_first()
            if (title is not None and len(title)>0 and title!='More'):

                url=news.css('a[class=storylink]::attr(href)').extract_first()
                if url is not None:
                    if url[0:4]!='http':
                        url=baseUrl+url
                        
                    yield {'title':title,"url":url,'create_at':time.time(),"status":0,"hash":hashlib.md5(url.encode('utf-8')).hexdigest()}

        next_page=response.xpath("//a[re:test(@class,'morelink')]//@href").extract_first()
        if next_page is not None:
            next_page=baseUrl+next_page
            next_page=response.urljoin(next_page)
            yield scrapy.Request(next_page,self.parse,dont_filter=True)
