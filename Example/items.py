# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

    
class secondclass(scrapy.Item):
    
    
    id1=scrapy.Field()
    conm=scrapy.Field()
    gvkey=scrapy.Field()
    ticker=scrapy.Field()
    cusip9digit=scrapy.Field()
    cusip=scrapy.Field()
    loc=scrapy.Field()


class quotestut(scrapy.Item):
    id1=scrapy.Field()
    conm=scrapy.Field()
    gvkey=scrapy.Field()
    ticker=scrapy.Field()
    cusip9digit=scrapy.Field()
    cusip=scrapy.Field()
    loc=scrapy.Field()
    # define the fields for your item here like:
    # name = scrapy.Field()

    

    
    
    
