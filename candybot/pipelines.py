# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from twisted.enterprise import adbapi
from scrapy.utils.project import get_project_settings

settings=get_project_settings()

class CandybotPipeline(object):

    insert_sql="""insert into `news.ycombinator.com` (%s) values ( %s )"""
    select_sql="""select 1 from `news.ycombinator.com` where hash = %s """

    def __init__(self):

        dbargs=settings.get('DB_CONNECT')
        db_server=settings.get('DB_SERVER')

        self.dbpool = adbapi.ConnectionPool(db_server, **dbargs) ##???? **dbargs

    def __del__(self):
        self.dbpool.close()


    def process_item(self, item, spider):
        if self.data_is_found(item):
            return "Data found skip"
        else:
            return self.insert_data(item,self.insert_sql)

    def data_is_found(self,item):
        found=self.dbpool.runQuery(self.select_sql , (item['hash'],))
        if found is None:
            return False
        else:
            return True

    def insert_data(self,item,insert):

        keys=item.keys()
        fields=u','.join(keys)
        qm=u','.join([u'%s']*len(keys))
        sql=insert %(fields,qm)

        data=[item[k] for k in keys]
        return self.dbpool.runOperation(sql,data)
