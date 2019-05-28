"""Scraps website to find all announcements"""

import scrapy
from scrapy.crawler import CrawlerProcess


class AnnounceSpider(scrapy.Spider):
    """Spider"""

    name = "announce_spider"

    def start_requests(self):
        urls = ["https://www.otodom.pl/wynajem/lokal/dolnoslaskie/?search%5Bdescription%5D=1#form"]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse_given_query)

    def parse_given_query(self, response):
        pages.append(response.url)
        offers = response.xpath("//article[@data-featured-name='listing_no_promo']/div[@class='offer-item-details']")
        for offert in offers:
            offert_id = offert.xpath("./header/div/a/@data-id").extract_first()
            area = float(offert.xpath("./ul/li[@class='hidden-xs offer-item-area']/text()").
                         extract_first().split(" ")[0].replace(",", "."))
            price_per_meter = float(offert.xpath("./ul/li[@class='hidden-xs offer-item-price-per-m']/text()").
                                    extract_first().split(" ")[0].replace(",", "."))
            location = offert.xpath("./header/p/text()").extract_first().split(": ")[1].split(",")[0]
            offers_dict[offert_id] = {
                "price per meter": price_per_meter,
                "area": area,
                "location": location
            }

        next_link = response.xpath("//li[@class='pager-next']/a/@href").extract_first()
        yield response.follow(url=next_link, callback=self.parse_given_query)


pages = list()
offers_dict = dict()

process = CrawlerProcess()
process.crawl(AnnounceSpider)
process.start()

print(len(offers_dict))
for i in offers_dict:
    print(offers_dict[i])
