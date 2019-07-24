import scrapy
import pandas as pd
from scrapy.selector import HtmlXPathSelector
from urllib.parse import urljoin

def get_username():
    df = pd.read_excel("Kor_streamer_List_Final.xlsx")
    df['ID'] = df['ID'].astype(str)
    df = df.sort_values(by=['ID'])
    return ["https://twitchtracker.com/"+x for x in df['ID'].tolist()]


class TwitchStreamCrawler(scrapy.Spider):
    name = "twitch-stream"

    def start_requests(self):
        urls = get_username()
        return [scrapy.Request(url=url+"/streams", callback=self.parse) for url in urls[3000:3200]]
#        return [scrapy.Request(url="https://twitchtracker.com/00000__/streams", callback=self.parse), \
#                scrapy.Request(url="https://twitchtracker.com/0000isname/streams", callback=self.parse)]

    def parse(self, response):
        stream_urls = response.xpath('//*[@id="streams"]/tbody/tr/td/a/@href').extract()
        for url in stream_urls:
            yield scrapy.Request(urljoin(response.url, url), callback=self.parse_streams)

    def parse_streams(self, response):
        if response.status != 200:
            item['streamer'] = response.url.split("https://twitchtracker.com/")[1].split("/")[0]
            item['stream_url'] = response.url
            item['status'] = response.status
            yield item
        else:
            for detail in response.css('#content-wrapper > div:nth-child(5) > section > div:nth-child(1) > div'):
                item = {}
                item['streamer'] = response.url.split("https://twitchtracker.com/")[1].split("/")[0]
                item['stream_url'] = response.url
                item['stream_started'] = detail.css('div:nth-child(1) > div:nth-child(1) > div > div.stats-value::text').extract_first()
                item['stream_finished'] = detail.css('div:nth-child(1) > div:nth-child(2) > div > div.stats-value::text').extract_first()
                item['stream_duration'] = detail.css('div:nth-child(2) > div:nth-child(1) > div > div.stats-value::text').extract_first()
                item['stream_hours_watched'] = detail.css('div:nth-child(2) > div:nth-child(2) > div > div.stats-value::text').extract_first()
                item['stream_avg_viewers'] = detail.css('div:nth-child(3) > div:nth-child(1) > div > div.stats-value::text').extract_first()
                item['stream_max_viewers'] = detail.css('div:nth-child(3) > div:nth-child(2) > div > div.stats-value::text').extract_first()
                item['stream_followers'] = detail.css('div:nth-child(4) > div:nth-child(1) > div > div.stats-value > span::text').extract_first()
                item['stream_total_followers'] = detail.css('div:nth-child(4) > div:nth-child(2) > div > div.stats-value::text').extract_first()
                item['stream_uniq_views'] = detail.css('div:nth-child(5) > div:nth-child(1) > div > div.stats-value > span::text').extract_first()
                item['stream_total_uniq_views'] = detail.css('div:nth-child(5) > div:nth-child(2) > div > div.stats-value ::text').extract_first()
                item['stream_total_uniq_views'] = detail.css('div:nth-child(5) > div:nth-child(2) > div > div.stats-value ::text').extract_first()
# #content-wrapper > div:nth-child(5) > section > div:nth-child(1) > div > div:nth-child(5) > div:nth-child(1) > div > div.stats-value > span
# #content-wrapper > div:nth-child(5) > section > div:nth-child(1) > div > div.stats-bar.stats-bar-twitchL > div.stats-value > a
                item['stream_played_game'] = detail.css('div.stats-bar.stats-bar-twitchL > div.stats-value > a ::text').extract_first()
                item['stream_games'] = []
                for game in response.css('#content-wrapper > div:nth-child(5) > section.games-stats > div > div'):
                    temp = {}
                    temp['game_title'] = game.css('div > div.card-relics > div.card-title::text').extract_first()
                    temp['game_date'] = game.css('div > div.card-relics > div.card-meta.stats-label.hidden-xxs::text').extract_first()
                    temp['game_avg_viewers'] = game.css('div > div.card-relics > div:nth-child(3) > div:nth-child(1) > div.stats-avg_viewers > div.stats-value.stats-value-sm::text').extract_first()
                    item['stream_games'].append(temp)
                yield item

class TwitchSubscribers(scrapy.Spider):
    name = 'twitch-subs'

    def start_requests(self):
        return [scrapy.http.Request(url=start_url+'/subscribers') for start_url in get_username()]

    def parse(self, response):
        subs = []
        for row in response.css('#subscribers > tbody > tr'):
            temp = {}
            temp['streamer'] = response.url.split("https://twitchtracker.com/")[1].split("/")[0]
            temp['month'] = row.css('td.to-monthyear::attr(data-order)').extract_first()
            temp['total'] = row.css('td:nth-child(2) > span::text').extract_first()
            temp['prime'] = row.css('td:nth-child(3) > span::text').extract_first()
            temp['tier1'] = row.css('td:nth-child(4) > span::text').extract_first()
            temp['tier2'] = row.css('td:nth-child(5) > span::text').extract_first()
            temp['tier3'] = row.css('td:nth-child(6) > span::text').extract_first()
            temp['unshared'] = row.css('td:nth-child(7) > span::text').extract_first()
            temp['gifted'] = row.css('td:nth-child(8) > span::text').extract_first()

            yield temp


class TwitchStats(scrapy.Spider):
    name = 'twitch-stats'

    def start_requests(self):
        return [scrapy.http.Request(url=start_url+'/statistics') for start_url in get_username()]

    def parse(self, response):
        subs = []
        #table-statistics > tbody
        #table-fv > tbody
#        for row in response.css('#table-statistics > tbody > tr'):
#        #content-wrapper > div.container
#            temp = {}
#            temp['streamer'] = response.url.split("https://twitchtracker.com/")[1].split("/")[0]
#            #table-statistics > tbody > tr:nth-child(1) > td.sorting_1
#            temp['month'] = row.css('td.to-monthyear::attr(data-order)').extract_first()
#            temp['avg_viewers'] = row.css('td:nth-child(2) > span::text').extract_first()
#            temp['broadcast_time'] = row.css('td:nth-child(3) > span::text').extract_first()
#            temp['hours_watched'] = row.css('td:nth-child(4) > span::text').extract_first()
#            temp['max_viewers'] = row.css('td:nth-child(5) > span::text').extract_first()
#
#            yield temp

        for row in response.css('#table-fv > tbody > tr'):
            temp = {}
            temp['streamer'] = response.url.split("https://twitchtracker.com/")[1].split("/")[0]
            temp['month'] = row.css('td.to-monthyear::attr(data-order)').extract_first()
            temp['followers_gained'] = row.css('td:nth-child(2) > span::text').extract_first()
            temp['followers'] = row.css('td:nth-child(2) > small > span::text').extract_first()
            temp['followers_per_hour'] = row.css('td:nth-child(3) > span::text').extract_first()
            temp['views_gained'] = row.css('td:nth-child(4) > span::text').extract_first()
            temp['views'] = row.css('td:nth-child(4) > small > span::text').extract_first()
            temp['views_per_hour'] = row.css('td:nth-child(5) > span::text').extract_first()

            yield temp
