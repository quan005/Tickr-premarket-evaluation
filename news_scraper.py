import pandas as pd
import yfinance as yf
import re
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
import urllib.request
from nltk.sentiment.vader import SentimentIntensityAnalyzer


class NewsScraper:
    def __init__(self, ticker=None):

        self.ticker = ticker.upper()
        self.finwiz_news = []
        self.marketwatch_news = []
        self.barron_news = []
        self.seekingalpha_news = []
        self.benzinga_news = []
        self.all_news = []
        self.score = ''
        self.catalyst_weight = 0
        self.company_name = ''
        self.get_date = datetime.now(timezone.utc) - timedelta(days=3)
        self.cover_date = f'{self.get_date.month}-{self.get_date.day}-{self.get_date.year}'

    def getCompanyName(self):

        if self.ticker == 'SPY':
            self.company_name = 'SPDR S&P 500'
            return self.company_name

        elif self.ticker == 'AAPL':
            self.company_name = 'Apple'
            return self.company_name

        elif self.ticker == 'NVDA':
            self.company_name = 'Nvidia'
            return self.company_name

        elif self.ticker == 'MSFT':
            self.company_name = 'Microsoft'
            return self.company_name

        elif self.ticker == 'NFLX':
            self.company_name = 'Netflix'
            return self.company_name

        elif self.ticker == 'FB':
            self.company_name = 'Facebook'
            return self.company_name

        elif self.ticker == 'DKNG':
            self.company_name = 'Draftking'
            return self.company_name

        elif self.ticker == 'AMD':
            self.company_name = 'Advance Micro Devices'
            return self.company_name

        elif self.ticker == 'GOOG':
            self.company_name = 'Google'
            return self.company_name

        elif self.ticker == 'TSLA':
            self.company_name = 'Tesla'
            return self.company_name

        elif self.ticker == 'BABA':
            self.company_name = 'Alibaba'
            return self.company_name

        elif self.ticker == 'NKE':
            self.company_name = 'Nike'
            return self.company_name

        elif self.ticker == 'AMZN':
            self.company_name = 'Amazon'
            return self.company_name

        elif self.ticker == 'WMT':
            self.company_name = 'Walmart'
            return self.company_name

        elif self.ticker == 'FL':
            self.company_name = 'Foot Locker'
            return self.company_name

        elif self.ticker == 'EBAY':
            self.company_name = 'Ebay'
            return self.company_name

        elif self.ticker == 'SHOP':
            self.company_name = 'Shopify'
            return self.company_name

        elif self.ticker == 'CMG':
            self.company_name = 'Chipotle'
            return self.company_name

        elif self.ticker == 'BAC':
            self.company_name = 'Bank Of America'
            return self.company_name

        elif self.ticker == 'JPM':
            self.company_name = 'JPMorgan Chase'
            return self.company_name

        elif self.ticker == 'ZM':
            self.company_name = 'Zoom'
            return self.company_name

        elif self.ticker == 'AAL':
            self.company_name = 'American Airlines'
            return self.company_name

        elif self.ticker == 'DIS':
            self.company_name = 'Disney'
            return self.company_name

        elif self.ticker == 'BA':
            self.company_name = 'Boeing '
            return self.company_name

        elif self.ticker == 'ROKU':
            self.company_name = 'Roku'
            return self.company_name

        elif self.ticker == 'MU':
            self.company_name = 'Micron Technology'
            return self.company_name

        elif self.ticker == 'BYND':
            self.company_name = 'Beyond Meat'
            return self.company_name

        elif self.ticker == 'PYPL':
            self.company_name = 'Paypal'
            return self.company_name

        elif self.ticker == 'DOCU':
            self.company_name = 'Docusign'
            return self.company_name

        elif self.ticker == 'PTON':
            self.company_name = 'Peloton Interactive'
            return self.company_name

        elif self.ticker == 'EA':
            self.company_name = 'Electronic Arts'
            return self.company_name

        elif self.ticker == 'PFE':
            self.company_name = 'Pfizer'
            return self.company_name

        elif self.ticker == 'BAC':
            self.company_name = 'Bank of America Corp'
            return self.company_name

        elif self.ticker == 'CGC':
            self.company_name = 'Canopy Growth Corp'
            return self.company_name

        elif self.ticker == 'CRM':
            self.company_name = 'Salesforce.com'
            return self.company_name

        elif self.ticker == 'SQ':
            self.company_name = 'Square'
            return self.company_name

        elif self.ticker == 'LCID':
            self.company_name = 'Lucid Group'
            return self.company_name

        info = yf.Ticker(self.ticker)
        name = info.info['shortName']
        split_name = re.findall(r'\s|,|[^,\s]+', name)

        self.company_name = split_name[0]

    def getFinwizNews(self):

        finwiz_url = 'https://finviz.com/quote.ashx?t='
        url = finwiz_url + self.ticker
        if len(self.company_name) == 0:
            self.getCompanyName()
        news_tables = {}
        count = 0

        req = urllib.request.Request(
            url=url, headers={'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246'})
        resp = urllib.request.urlopen(url=req, timeout=5)
        html = BeautifulSoup(resp, features="lxml")
        news_table = html.find(id='news-table')
        news_tables = news_table.findAll('tr')
        company_name_search = re.compile(r'\b%s\b' % self.company_name, re.I)
        company_ticker_search = re.compile(r'\b%s\b' % self.ticker, re.I)

        while count < len(news_tables):
            if news_tables[count].a:
                text = news_tables[count].a.text
                print(type(text))
                print(text)
                date_scrape = news_tables[count].td.text.split()

                if len(date_scrape) == 1:
                    time = date_scrape[0]
                else:
                    date_string = date_scrape[0]
                    datetime_parse = datetime.strptime(date_string, '%b-%d-%y')
                    new_tz = datetime_parse.astimezone(timezone.utc)
                    date = f'{datetime_parse.month}-{datetime_parse.day}-{datetime_parse.year}'
                    time = date_scrape[1]

                if company_name_search.search(text) != None or company_ticker_search.search(text) != None:

                    if new_tz >= self.get_date:
                        self.catalyst_weight += 0.5
                        self.finwiz_news.append(
                            [self.ticker, date, time, text])
                        self.all_news.append([self.ticker, date, time, text])

                    count += 1

                else:
                    count += 1
            else:
                count += 1

        # print(self.finwiz_news)
        # print(self.catalyst_weight)
        return self.finwiz_news

    def getMarketwatchNews(self):

        marketwatch_url = 'https://www.marketwatch.com/mw_news_sitemap.xml'
        if len(self.company_name) == 0:
            self.getCompanyName()
        news_tables = {}

        req = urllib.request.Request(url=marketwatch_url, headers={
                                     'user-agent': 'Googlebot-News'})
        resp = urllib.request.urlopen(req)
        lxml = BeautifulSoup(resp, features="lxml")
        news_tables = lxml.find_all('n:news')
        company_name_search = re.compile(r'\b%s\b' % self.company_name, re.I)
        company_ticker_search = re.compile(r'\b%s\b' % self.ticker, re.I)

        for news in news_tables:
            title = news.find('n:title').get_text()
            date_scrape = news.find('n:publication_date').get_text()
            datetime_parse = datetime.fromisoformat(date_scrape)
            date = f'{datetime_parse.month}-{datetime_parse.day}-{datetime_parse.year}'
            time = f'{datetime_parse.hour}:{datetime_parse.minute}'
            stock_tickers = ''

            if news.find('n:stock_tickers'):
                stock_tickers = news.find('n:stock_tickers').get_text()

            # print(title)
            # print(company_name_search.search(title))
            # print(company_ticker_search.search(title))

            if company_name_search.search(title) != None or company_ticker_search.search(title) != None:
                if datetime_parse >= self.get_date:
                    self.catalyst_weight += 0.5
                    self.marketwatch_news.append(
                        [self.ticker, date, time, title])
                    self.all_news.append([self.ticker, date, time, title])
            else:
                pass

        # print(self.marketwatch_news)
        return self.marketwatch_news

    def getBarronNews(self):

        barron_url = 'https://www.barrons.com/bol_news_sitemap.xml'
        if len(self.company_name) == 0:
            self.getCompanyName()
        news_tables = {}

        req = urllib.request.Request(url=barron_url, headers={
                                     'user-agent': 'Googlebot-News'})
        resp = urllib.request.urlopen(req)
        lxml = BeautifulSoup(resp, features="lxml")
        news_tables = lxml.find_all('news:news')
        company_name_search = re.compile(r'\b%s\b' % self.company_name, re.I)
        company_ticker_search = re.compile(r'\b%s\b' % self.ticker, re.I)

        for news in news_tables:
            title = news.find('news:title').get_text()
            date_scrape = news.find('news:publication_date').get_text()
            datetime_parse = datetime.strptime(
                date_scrape, '%Y-%m-%dT%H:%M:%S%z')
            date = f'{datetime_parse.month}-{datetime_parse.day}-{datetime_parse.year}'
            time = f'{datetime_parse.hour}:{datetime_parse.minute}'
            stock_tickers = ''

            if news.find('news:stock_tickers'):
                stock_tickers = news.find('news:stock_tickers').get_text()

            if company_name_search.search(title) != None or company_ticker_search.search(title) != None:
                if datetime_parse >= self.get_date:
                    self.catalyst_weight += 0.5
                    self.barron_news.append([self.ticker, date, time, title])
                    self.all_news.append([self.ticker, date, time, title])
            else:
                pass

        # print(self.barron_news)
        return self.barron_news

    def getSeekingalphaNews(self):

        seekingalpha_url = 'https://seekingalpha.com/sitemap_news.xml'
        if len(self.company_name) == 0:
            self.getCompanyName()
        news_tables = {}

        req = urllib.request.Request(url=seekingalpha_url, headers={
                                     'user-agent': 'Googlebot-News'})
        resp = urllib.request.urlopen(req)
        lxml = BeautifulSoup(resp, features="lxml")
        news_tables = lxml.find_all('news:news')
        company_name_search = re.compile(r'\b%s\b' % self.company_name, re.I)
        company_ticker_search = re.compile(r'\b%s\b' % self.ticker, re.I)

        for news in news_tables:
            title = news.find('news:title').get_text()
            date_scrape = news.find('news:publication_date').get_text()
            datetime_parse = datetime.strptime(
                date_scrape, '%Y-%m-%dT%H:%M:%S%z')
            date = f'{datetime_parse.month}-{datetime_parse.day}-{datetime_parse.year}'
            time = f'{datetime_parse.hour}:{datetime_parse.minute}'
            stock_tickers = ''

            if news.find('news:stock_tickers'):
                stock_tickers = news.find('news:stock_tickers').get_text()

            if company_name_search.search(title) != None or company_ticker_search.search(title) != None:
                if datetime_parse >= self.get_date:
                    self.catalyst_weight += 0.5
                    self.seekingalpha_news.append(
                        [self.ticker, date, time, title])
                    self.all_news.append([self.ticker, date, time, title])
            else:
                pass

        # print(self.seekingalpha_news)
        return self.seekingalpha_news

    def getBenzingaNews(self):

        benzinga_url = 'https://www.benzinga.com/googlenews.xml'
        if len(self.company_name) == 0:
            self.getCompanyName()
        news_tables = {}

        req = urllib.request.Request(url=benzinga_url, headers={
                                     'user-agent': 'bingbot'})
        resp = urllib.request.urlopen(req)
        lxml = BeautifulSoup(resp, features="lxml")
        news_tables = lxml.find_all('news:news')
        company_name_search = re.compile(r'\b%s\b' % self.company_name, re.I)
        company_ticker_search = re.compile(r'\b%s\b' % self.ticker, re.I)

        for news in news_tables:
            title = news.find('news:title').get_text()
            date_scrape = news.find('news:publication_date').get_text()
            datetime_parse = datetime.strptime(
                date_scrape, '%Y-%m-%dT%H:%M:%S%z')
            date = f'{datetime_parse.month}-{datetime_parse.day}-{datetime_parse.year}'
            time = f'{datetime_parse.hour}:{datetime_parse.minute}'
            stock_tickers = ''

            if news.find('news:stock_tickers'):
                stock_tickers = news.find('news:stock_tickers').get_text()

            if company_name_search.search(title) != None or company_ticker_search.search(title) != None:
                if datetime_parse >= self.get_date:
                    self.catalyst_weight += 0.5
                    self.benzinga_news.append([self.ticker, date, time, title])
                    self.all_news.append([self.ticker, date, time, title])
            else:
                pass

        # print(self.benzinga_news)
        # print(self.catalyst_weight)
        return self.benzinga_news

    def getCompanyAnalysis(self):
        pass

    def getSentimentAnalysis(self, list):

        headline_posiive_words = {
            'event': 20,
            'host': 20,
            'crushes': 10,
            'cruises': 10,
            'leads': 10,
            'outperforms': 10,
            'outperformed': 10,
            'beat': 5,
            'beats': 5,
            'misses': -5,
            'missed': -5,
            'slipping': -5,
            'slipped': -5,
            'trouble': -10,
            'erases': -5,
            'falls': -100,
            'Undervalued': 50,
            'split': 10,
            'splits': 10,
            'launches': 10,
            'launched': 10,
            'rolls': 10,
            'rolled': 10,
            'cheaper': 10,
            'gains': 10,
            'gained': 10,
        }
        analyzer = SentimentIntensityAnalyzer()
        analyzer.lexicon.update(headline_posiive_words)

        columns = ['Ticker', 'Date', 'Time', 'Headline']
        news = pd.DataFrame.from_records(list, columns=columns)
        scores = news['Headline'].apply(analyzer.polarity_scores).tolist()
        df_scores = pd.DataFrame(scores)
        news = news.join(df_scores, rsuffix='_right')

        if len(news) == 0:
            return

        negative_score = sum(news['neg'])
        positve_score = sum(news['pos'])

        if negative_score > positve_score:
            self.score = 'Negative'
        elif positve_score > negative_score:
            self.score = 'Positive'
        else:
            self.score = 'Neutral'

        return self.score

    def startNewsAnalyzer(self):

        self.getCompanyName()
        self.getFinwizNews()
        self.getMarketwatchNews()
        self.getSeekingalphaNews()
        self.getBarronNews()
        self.getBenzingaNews()

        sentiment = self.getSentimentAnalysis(self.all_news)

        if self.catalyst_weight >= 3 and self.catalyst_weight < 7:
            return {
                'sentiment_analysis': sentiment,
                'score': 1
            }
        elif self.catalyst_weight >= 7 and self.catalyst_weight < 12:
            return {
                'sentiment_analysis': sentiment,
                'score': 2
            }
        elif self.catalyst_weight >= 12:
            return {
                'sentiment_analysis': sentiment,
                'score': 3
            }
        else:
            return {
                'sentiment_analysis': sentiment,
                'score': 0
            }
