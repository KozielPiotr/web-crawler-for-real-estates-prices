"""Scraps website to find all announcements"""

import os
import scrapy
import pandas as pd
from scrapy.crawler import CrawlerProcess
from datetime import datetime


class OtodomSpider(scrapy.Spider):
    """Spider"""

    name = "announce_spider"

    def start_requests(self):
        urls = [
            "https://www.otodom.pl/wynajem/lokal/dolnoslaskie/?search%5Bdescription%5D=1&search%5Border%5D=created_at_first%3Adesc",
            "https://www.otodom.pl/wynajem/lokal/kujawsko-pomorskie/?search%5Bdescription%5D=1&search%5Border%5D=created_at_first%3Adesc",
            "https://www.otodom.pl/wynajem/lokal/lodzkie/?search%5Bdescription%5D=1&search%5Border%5D=created_at_first%3Adesc",
            "https://www.otodom.pl/wynajem/lokal/lubelskie/?search%5Bdescription%5D=1&search%5Border%5D=created_at_first%3Adesc",
            "https://www.otodom.pl/wynajem/lokal/lubuskie/?search%5Bdescription%5D=1&search%5Border%5D=created_at_first%3Adesc",
            "https://www.otodom.pl/wynajem/lokal/malopolskie/?search%5Bdescription%5D=1&search%5Border%5D=created_at_first%3Adesc",
            "https://www.otodom.pl/wynajem/lokal/mazowieckie/?search%5Bdescription%5D=1&search%5Border%5D=created_at_first%3Adesc",
            "https://www.otodom.pl/wynajem/lokal/opolskie/?search%5Bdescription%5D=1&search%5Border%5D=created_at_first%3Adesc",
            "https://www.otodom.pl/wynajem/lokal/podkarpackie/?search%5Bdescription%5D=1&search%5Border%5D=created_at_first%3Adesc",
            "https://www.otodom.pl/wynajem/lokal/podlaskie/?search%5Bdescription%5D=1&search%5Border%5D=created_at_first%3Adesc",
            "https://www.otodom.pl/wynajem/lokal/pomorskie/?search%5Bdescription%5D=1&search%5Border%5D=created_at_first%3Adesc",
            "https://www.otodom.pl/wynajem/lokal/slaskie/?search%5Bdescription%5D=1&search%5Border%5D=created_at_first%3Adesc",
            "https://www.otodom.pl/wynajem/lokal/swietokrzyskie/?search%5Bdescription%5D=1&search%5Border%5D=created_at_first%3Adesc",
            "https://www.otodom.pl/wynajem/lokal/warminsko-mazurskie/?search%5Bdescription%5D=1&search%5Border%5D=created_at_first%3Adesc",
            "https://www.otodom.pl/wynajem/lokal/wielkopolskie/?search%5Bdescription%5D=1&search%5Border%5D=created_at_first%3Adesc",
            "https://www.otodom.pl/wynajem/lokal/zachodniopomorskie/?search%5Bdescription%5D=1&search%5Border%5D=created_at_first%3Adesc"
        ]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse_given_query)

    def parse_given_query(self, response):
        offers = response.xpath("//article[@data-featured-name='listing_no_promo']/div[@class='offer-item-details']")
        for offer in offers:
            offer_id = "OTO_" + offer.xpath("./header/div/a/@data-id").extract_first()
            area = float(offer.xpath("./ul/li[@class='hidden-xs offer-item-area']/text()").
                         extract_first().split(" ")[0].replace(",", "."))
            try:
                price_per_meter = float(offer.xpath("./ul/li[@class='hidden-xs offer-item-price-per-m']/text()").
                                        extract_first().split(" ")[0].replace(",", "."))
            except AttributeError:
                price_per_meter = None
            location = offer.xpath("./header/p/text()").extract_first().split(": ")[1].split(",")[0]
            region = response.xpath("//h1[@class='query-text-h1']/strong/text()").extract_first().\
                split(" ")[-1]

            offers_dict["offer_id"].append(offer_id)
            offers_dict["area"].append(area)
            offers_dict["price_per_meter"].append(price_per_meter)
            offers_dict["location"].append(location)
            offers_dict["region"].append(region)

        next_link = response.xpath("//li[@class='pager-next']/a/@href").extract_first()
        yield response.follow(url=next_link, callback=self.parse_given_query)


spiders = [OtodomSpider]
offers_dict = {
    "offer_id": [],
    "area": [],
    "price_per_meter": [],
    "location": [],
    "region": []
    }


def get_data(scrapper):
    process = CrawlerProcess()
    process.crawl(scrapper)
    process.start()


for spider in spiders:
    get_data(spider)

df = pd.DataFrame(offers_dict)
regions_df = df.groupby(by="region").mean()
locations_df = df.groupby(by="location").mean()

if not os.path.isdir("csv"):
    os.mkdir("csv")

df.to_csv("csv/base.csv", sep=";")


r_names = []
for i in df["region"]:
    if i not in r_names:
        r_names.append(i)

regional_dict = {
    "region": [],
    "area": [],
    "price_per_meter": []
}

# creates df with median area and price for each region
for r_name in r_names:
    regional_dict["region"].append(r_name)
    regional_dict["area"].append(df[(df.region == r_name)]["area"].median())
    regional_dict["price_per_meter"].append(df[(df.region == r_name)]["price_per_meter"].median())

regional_df = pd.DataFrame(regional_dict)
regional_df.to_csv("csv/regional.csv", sep=";", index=False)

# creates df with median area and price for each city in region (separate df for each region)
for r_name in r_names:
    r_locations = []
    for l_name in df[df.region == r_name]["location"]:
        if l_name not in r_locations:
            r_locations.append(l_name)
    location_dict = {
        "location": [],
        "area": [],
        "price_per_meter": []
    }
    for l_name in r_locations:
        location_dict["location"].append(l_name)
        location_dict["area"].append(df[(df.location == l_name)]["area"].median())
        location_dict["price_per_meter"].append(df[(df.location == l_name)]["price_per_meter"].median())
    location_df = pd.DataFrame(location_dict)
    location_df.to_csv("csv/{}.csv".format(r_name), sep=";", index=False)
