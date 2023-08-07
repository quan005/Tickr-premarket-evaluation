import yfinance as yf
import pandas as pd
import requests
from datetime import datetime, timedelta
from yahooquery import Screener

class Scanner:
    def __init__(self):
        self.small_caps = self.getSmallCaps()
        self.mid_caps = self.getMidCaps()
        self.large_caps = self.getLargeCaps()

    def getSmallCaps(self):
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_600_companies'
        html = requests.get(url).text
        tables = pd.read_html(html)
        sp500_table = tables[0]
        stock_symbols = sp500_table['Symbol'].tolist()

        return stock_symbols
    
    def getMidCaps(self):
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_400_companies'
        html = requests.get(url).text
        tables = pd.read_html(html)
        sp500_table = tables[0]
        stock_symbols = sp500_table['Symbol'].tolist()

        return stock_symbols
    
    def getLargeCaps(self):
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        html = requests.get(url).text
        tables = pd.read_html(html)
        sp500_table = tables[0]
        stock_symbols = sp500_table['Symbol'].tolist()

        return stock_symbols
    
    def filterSmallCaps(self):
        small_caps_string = " ".join(self.small_caps)
        small_caps_data = yf.download(small_caps_string, period="1d", threads=True)

        filtered_symbols = []
        for symbol in small_caps_data.columns.levels[1]:
            close_prices = small_caps_data['Close', symbol]
            if close_prices.iloc[-1] >= 2 and close_prices.iloc[-1] <= 20:
                filtered_symbols.append(symbol)

        return filtered_symbols

    def smallCapMomentum(self):
        small_caps = self.filterSmallCaps()

        for symbol in small_caps:
            stock = yf.Ticker(symbol)

            stock_info = stock.info
            # print('stock_info', stock_info)
            stock_float = stock_info.get('floatShares')

            # if stock_float == None or stock_float > 20000000:
            #     continue

            stock_previous_close = stock_info.get('regularMarketPreviousClose')
            stock_open = stock_info.get('regularMarketOpen')

            print(f'open: {stock_open} - close: {stock_previous_close}')

            if stock_previous_close == None or stock_open == None:
                stock_gap_percentage = 0
            else:
                stock_gap_percentage = ((stock_previous_close - stock_open) / ((stock_previous_close + stock_open) / 2)) * 100

            print('stock_gap_percentage', stock_gap_percentage)
            
            if stock_gap_percentage <= -7 or stock_gap_percentage >= 7:
                print('symbol', symbol)

                stock_history = stock.history(period="max", interval="1m")
                # print('stock history', stock_history)
            
        pass

    def smallCapTopGappers(self):
        # stock that are gapping up more than 7%
        # or gapping down more than 7%
        # lower float has bigger moves - float less than 20000000
        # price between 2 - 20 dollars
        pass
    
    def smallCapTopRelativeVolume(self):
        pass
    
    def smallCapTopRsi(self):
        # used for reversals
        pass

    def afterHoursTopGainers(self):
        # only works after 4pm eastern time
        pass

if __name__ == '__main__':
    scanner = Scanner()
    print('smallCapTopGappers: ', scanner.smallCapMomentum())