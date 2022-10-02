import json
from uuid import uuid4
from typing import OrderedDict
from datetime import datetime, timedelta
from configparser import ConfigParser
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from get_token import TokenInitiator
from historical_analysis import HistoricalAnalysis
from news_scraper import NewsScraper
from py_trader import PyTrader
from indicators import Indicators


class Pre_Market:
    def __init__(self):
        config = ConfigParser()
        config.read('configs.ini')
        self.CLIENT_ID = config.get('main', 'CLIENT_ID')
        self.REDIRECT_URI = config.get('main', 'REDIRECT_URI')
        self.ACCOUNT_ID = config.get('main', 'ACCOUNT_ID')
        self.BOOTSTRAP_SERVER = config.get('main', 'BOOTSTRAP_SERVER')
        self.SCHEMA_REGISTRY_URL = config.get('main', 'SCHEMA_REGISTRY_URL')
        self.TOKEN = None
        self.small_watchlist = ['AMD', 'NKE', 'AAL', 'TEVA', 'UAA', 'SOFI',
                                'SPY', 'PFE', 'BAC', 'CCL', 'AAPL']
        self.medium_watchlist = ['AMD', 'NKE', 'AAL', 'TEVA', 'UAA', 'SOFI', 'SPY', 'PFE', 'BAC', 'CCL', 'AAPL',
                                 'MSFT', 'CRM', 'BABA', 'PYPL', 'DKNG', 'WMT', 'JPM', 'DIS', 'PBR', 'UAL', 'RIVN', 'FCX', 'MRO', 'MU']
        self.large_watchlist = ['AMD', 'NKE', 'AAL', 'TEVA', 'UAA', 'SOFI', 'SPY', 'PFE', 'BAC', 'CCL', 'AAPL', 'MSFT', 'CRM', 'BABA',
                                'PYPL', 'DKNG', 'WMT', 'JPM', 'DIS', 'PBR', 'UAL', 'RIVN', 'FCX', 'MRO', 'MU', 'ROKU', 'SQ', 'NFLX', 'TSLA', 'AMZN', 'SHOP', 'ZM', 'NVDA', 'LCID']
        self.xtra_large_watchlist = ['AMD', 'NKE', 'AAL', 'TEVA', 'UAA', 'SOFI', 'SPY', 'PFE', 'BAC', 'CCL', 'AAPL', 'MSFT', 'CRM', 'BABA', 'PYPL', 'DKNG',
                                     'WMT', 'JPM', 'DIS', 'PBR', 'UAL', 'RIVN', 'FCX', 'MRO', 'MU', 'ROKU', 'SQ', 'NFLX', 'TSLA', 'AMZN', 'SHOP', 'ZM', 'NVDA', 'LCID', 'GOOG', 'FL', 'EBAY', 'CMG', 'BA', 'MU', 'BYND', 'DOCU']
        self.limit = 0
        self.opportunities = []

    def start_pre_market_analysis(self):

        # initiate and get token
        getToken = TokenInitiator()
        getToken.get_access_token()

        # set token to self.TOKEN
        with open("token.json") as token_json:
            data = json.load(token_json)
            self.TOKEN = {
                "access_token": data["access_token"],
                "refresh_token": data["refresh_token"],
                "scope": data["scope"],
                "expires_in": data["expires_in"],
                "refresh_token_expires_in": data["refresh_token_expires_in"],
                "token_type": data["token_type"],
                "access_token_expires_at": data["access_token_expires_at"],
                "refresh_token_expires_at": data["refresh_token_expires_at"],
                "logged_in": data["logged_in"],
                "access_token_expires_at_date": data["access_token_expires_at_date"],
                "refresh_token_expires_at_date": data["refresh_token_expires_at_date"],
            }

        # get credential path
        config = ConfigParser()
        config.read('configs.ini')
        credential_path = config.get('main', 'CREDENTIALS_PATH')

        # initialize bot
        bot = PyTrader(
            client_id=self.CLIENT_ID,
            redirect_uri=self.REDIRECT_URI,
            credentials_path=credential_path,
            trading_account=self.ACCOUNT_ID,
            paper_trading=False
        )

        # get account info
        account_info = bot.session.get_accounts(account=self.ACCOUNT_ID)

        # get available cash, and initialize budget
        available_cash = account_info['securitiesAccount']['currentBalances']['cashAvailableForTrading']

        # set budget
        if available_cash >= 500 and available_cash < 1500:
            budget = .9 * available_cash
        elif available_cash >= 1500 and available_cash < 5715:
            budget = .45 * available_cash
        elif available_cash >= 5715:
            budget = .35 * available_cash
        else:
            self.send_account_low_event('Not Enough Capital')
            return 'Not Enough Capital'

        # based on budget determine which watchlist to use and total opportunity's limit aka self.limit
        if budget >= 500 and budget < 1000:
            self.limit = 2
            watchlist = self.small_watchlist
        elif budget >= 1000 and budget < 5000:
            self.limit = 6
            watchlist = self.medium_watchlist
        elif budget >= 5000 and budget < 15000:
            self.limit = 10
            watchlist = self.large_watchlist
        elif budget >= 15000:
            self.limit = 10
            watchlist = self.xtra_large_watchlist
        else:
            watchlist = None

        # create a portfolio
        bot_portfolio = bot.create_portfolio()

        # create a temporary list to hold opportunities
        temp_opportunity_list = []

        # start premarket analysis for each ticker in the watchlist:
        for i in watchlist:

            # create an object to store the new opportunity
            new_opportunity = {}

            # add ticker and score to new_opportunity object
            new_opportunity['Symbol'] = i
            new_opportunity['Score'] = 0
            new_opportunity['Limit'] = self.limit
            new_opportunity['Budget'] = budget // self.limit
            new_opportunity['Client Id'] = self.CLIENT_ID
            new_opportunity['Account Id'] = self.ACCOUNT_ID

            # inititialize NewsScraper ie. ns = NewsScraper('aapl')
            ns = NewsScraper(i)

            # start news_analyzer ie. ns.startNewsAnalyzer() and apply result to score
            news_analyzer = ns.startNewsAnalyzer()
            new_opportunity['Sentiment'] = news_analyzer['sentiment_analysis']
            new_opportunity['Score'] = (
                new_opportunity['Score'] + news_analyzer['score'])

            # initialize HistoricalAnalysis ie. ha = HistoricalAnalysis('aapl')
            print('symbol', new_opportunity['Symbol'])
            ha = HistoricalAnalysis(i)

            # start historical_analysis ie. ha.start_historical_analysis() and apply result to score
            history_analyzer = ha.start_historical_analysis()
            new_opportunity['Score'] = (
                new_opportunity['Score'] + history_analyzer)

            # add the position the the current company
            bot_portfolio.add_position(
                symbol=i,
                asset_type='equity'
            )

            # Grab historical prices, first define the start date and end date.
            start_date = datetime.today()

            # weekly end date
            weekly_end_date = start_date - timedelta(weeks=105)

            # grab historical prices using the weekly interval
            weekly_historical_prices = bot.grab_single_historical_prices(
                symbol=i,
                start=weekly_end_date,
                end=start_date,
                period_type='month',
                bar_size=1,
                bar_type='daily'
            )

            # convert historical prices to a pandas dataframe using the stock frame
            weekly_stock_frame = bot.create_stock_frame(
                data=weekly_historical_prices['aggregated']
            )

            # create weekly indicator object
            weekly_stock_indicator_client = Indicators(
                price_data_frame=weekly_stock_frame)

            # add weekly key levels
            new_opportunity['Key Levels'] = weekly_stock_indicator_client.s_r_levels(
                weekly_stock_frame.frame)

            # thirty minute end date
            thirty_minute_end_date = start_date - timedelta(weeks=10)

            # grab thirty minute historical prices using the 30 min interval
            thirty_minute_historical_prices = bot.grab_single_historical_prices(
                symbol=i,
                start=thirty_minute_end_date,
                end=start_date,
                bar_size=30,
                bar_type='minute'
            )

            # convert thirty minute historical prices to a pandas dataframe using the stock frame
            thirty_minute_stock_frame = bot.create_stock_frame(
                data=thirty_minute_historical_prices['aggregated']
            )

            # create thirty minute indicator object
            thirty_minute_indicator_client = Indicators(
                price_data_frame=thirty_minute_stock_frame)

            # add the thirty minute 200 ema to the thirty minuteindicator
            thirty_minute_indicator_client.ema(period=200)

            # create pandas series for both the thirty minute 200 ema and the thirty minute close price from the thirty minute historical prices stock frame
            thirty_minute_ema200 = thirty_minute_stock_frame.frame['ema']
            thirty_minute_close = thirty_minute_stock_frame.frame['close']

            # check the ema 200
            thirty_minute_ema200_analysis = thirty_minute_indicator_client.ema200(
                {
                    'current price': {
                        'ema200': thirty_minute_ema200[-1],
                        'close': thirty_minute_close[-1]
                    }
                }
            )

            # check the pattern analysis
            thirty_minute_pattern_analysis = thirty_minute_indicator_client.candle_pattern_check(
                thirty_minute_stock_frame.frame)

            # combine weekly key levels with 30 min key levels
            new_key_levels = thirty_minute_indicator_client.s_r_levels(
                thirty_minute_stock_frame.frame) + new_opportunity['Key Levels']

            # remove duplicates
            no_duplicates = list(OrderedDict.fromkeys(new_key_levels))

            # add key levels and support and resisitance
            new_opportunity['Key Levels'] = no_duplicates
            new_opportunity['Support Resistance'] = thirty_minute_indicator_client.get_support_resistance(
                new_opportunity['Key Levels'], thirty_minute_close)

            print('Symbol = ', new_opportunity['Symbol'])
            print('Key Levels = ', new_opportunity['Key Levels'])
            print('Support Resistance = ',
                  new_opportunity['Support Resistance'])

            # five minute end date
            five_minute_end_date = start_date - timedelta(weeks=209)

            # grab five minute historical prices using the 5 min interval
            five_minute_historical_prices = bot.grab_single_historical_prices(
                symbol=i,
                start=five_minute_end_date,
                end=start_date,
                bar_size=5,
                bar_type='minute'
            )

            # convert five minute historical prices to a pandas dataframe using the stock frame
            five_minute_stock_frame = bot.create_stock_frame(
                data=five_minute_historical_prices['aggregated']
            )

            # create thirty minute indicator object
            five_minute_indicator_client = Indicators(
                price_data_frame=five_minute_stock_frame)

            # get demand zones using the five minute stock frame
            demand_zones = five_minute_indicator_client.get_demand_zones(
                dataframe=five_minute_stock_frame.frame, ticker=i)

            new_opportunity['Demand Zones'] = demand_zones

            # get supply zones using the five minute stock frame
            supply_zones = five_minute_indicator_client.get_supply_zones(
                dataframe=five_minute_stock_frame.frame, ticker=i)

            new_opportunity['Supply Zones'] = supply_zones

            # if ema 200 check returns 'ABOVE' and self.limit is > 0 then append opportunity object to the opportunities array
            if thirty_minute_ema200_analysis == 'ABOVE':
                new_opportunity['Score'] = new_opportunity['Score'] + 3

            # check if new analyzer catalyst is >= 6, if so append opportunity object to the opportunities array
            if thirty_minute_pattern_analysis == 'BULLISH' or thirty_minute_pattern_analysis == 'BEARISH':
                new_opportunity['Score'] = new_opportunity['Score'] + 2

            # appen new opportunity to the temp opportunity list
            temp_opportunity_list.append(new_opportunity)

            # remove the current ticker from the position
            bot_portfolio.remove_position(
                symbol=i
            )

        # sorted = [
        #     {
        #         'Symbol': 'NKE',
        #         'Score': 8,
        #         'Limit': 6,
        #         'Budget': 203.0,
        #         'Client Id': 'GYNQGLGLPINXJNWLKFZM48NQ1C86KS8D',
        #         'Account Id': '277420951',
        #         'Sentiment': 'Negative',
        #         'Key Levels': [118.47, 85.32, 84.41, 83.66, 82.5, 179.1],
        #         'Support Resistance': {
        #             'resistance': 83.66,
        #             'support': 82.5
        #         },
        #         'Demand Zones': [
        #             (83.96, 84.73, '2022-09-30 14:05:00'),
        #             (82.5, 83.2233, '2022-09-30 13:40:00'),
        #             (101.53, 103.825, '2022-06-29 13:45:00'),
        #             (106.88, 108.7, '2022-06-28 13:30:00'),
        #             (106.1201, 106.65, '2022-06-17 13:30:00'),
        #             (107.21, 107.9862, '2022-05-12 19:55:00'),
        #             (119.31, 123.35, '2022-03-16 13:30:00'),
        #             (131.15, 132.07, '2022-03-01 20:55:00')
        #         ],
        #         'Supply Zones': [
        #             [85.89, 85.3867, '2022-09-30 14:40:00'],
        #             [102.92, 102.49, '2022-06-29 13:30:00'],
        #             [107.085, 106.584, '2022-06-28 14:15:00'],
        #             [111.49, 110.96, '2022-06-28 13:40:00'],
        #             [115.13, 113.24, '2022-05-06 13:30:00']
        #         ]
        #     },
        #     {
        #         'Symbol': 'CCL',
        #         'Score': 7,
        #         'Limit': 6,
        #         'Budget': 203.0,
        #         'Client Id': 'GYNQGLGLPINXJNWLKFZM48NQ1C86KS8D',
        #         'Account Id': '277420951',
        #         'Sentiment': 'Negative',
        #         'Key Levels': [11.38, 11.12, 10.46, 9.53, 8.78, 7.36, 7.01, 31.52, 11.36, 10.47, 9.52, 8.73],
        #         'Support Resistance': {
        #             'resistance': 7.36,
        #             'support': 7.01
        #         },
        #         'Demand Zones': [
        #             (7.12, 7.6381, '2022-09-30 18:55:00'),
        #             (7.31, 7.92, '2022-09-30 14:55:00'),
        #             (7.01, 7.575, '2022-09-30 14:25:00'),
        #             (9.54, 10.0428, '2022-09-28 15:40:00'),
        #             (9.43, 9.98, '2022-09-28 15:05:00'),
        #             (9.4008, 9.96, '2022-09-28 14:20:00'),
        #             (9.13, 9.75, '2022-09-28 13:35:00'),
        #             (9.09, 9.6, '2022-09-27 19:40:00'),
        #             (9.05, 9.56, '2022-09-27 16:20:00'),
        #             (9.255, 9.835, '2022-09-27 14:25:00'),
        #             (9.22, 9.85, '2022-09-27 13:55:00'),
        #             (8.88, 9.7, '2022-09-27 13:30:00'),
        #             (8.98, 9.505, '2022-09-26 18:45:00'),
        #             (8.94, 9.4999, '2022-09-26 16:50:00'),
        #             (8.9, 9.49, '2022-09-26 13:30:00')
        #         ],
        #         'Supply Zones': [
        #             [7.23, 6.77, '2022-09-30 19:20:00'],
        #             [7.3, 6.8201, '2022-09-30 17:30:00'],
        #             [7.43, 6.91, '2022-09-30 15:50:00'],
        #             [7.68, 7.18, '2022-09-30 15:15:00'],
        #             [7.82, 7.3179, '2022-09-30 13:55:00'],
        #             [8.5, 7.949999999999999, '2022-09-30 13:30:00'],
        #             [9.255, 8.755, '2022-09-29 16:55:00'],
        #             [9.415, 8.915, '2022-09-29 16:10:00'],
        #             [9.4199, 8.91, '2022-09-29 14:10:00'],
        #             [9.66, 9.11, '2022-09-29 13:30:00'],
        #             [9.7, 9.195, '2022-09-28 16:45:00'],
        #             [9.0897, 8.5865, '2022-09-27 18:10:00'],
        #             [9.15, 8.645, '2022-09-27 16:35:00'],
        #             [9.42, 8.905, '2022-09-27 15:20:00'],
        #             [9.3288, 8.815, '2022-09-26 14:25:00']
        #         ]
        #     },
        #     {
        #         'Symbol': 'AAPL',
        #         'Score': 7,
        #         'Limit': 6,
        #         'Budget': 203.0,
        #         'Client Id': 'GYNQGLGLPINXJNWLKFZM48NQ1C86KS8D',
        #         'Account Id': '277420951',
        #         'Sentiment': 'Positive',
        #         'Key Levels': [176.15, 142.55, 141.43, 140.5, 138.0, 182.94, 143.45, 142.53, 141.55, 140.54, 139.48, 138.47, 137.41, 136.55, 135.45, 134.52, 133.52, 107.32],
        #         'Support Resistance': {
        #             'resistance': 140.5,
        #             'support': 138.0
        #         },
        #         'Demand Zones': [(141.57, 142.72, '2022-09-30 14:30:00'), (140.61, 141.36, '2022-09-30 13:40:00'), (145.16, 145.669, '2022-09-28 14:10:00'), (150.56, 153.24, '2022-09-27 13:30:00'), (150.3, 151.08, '2022-09-26 18:45:00'), (150.04, 150.6, '2022-09-26 18:00:00'), (149.64, 150.16, '2022-09-26 13:30:00'), (154.69, 155.6, '2022-09-21 18:30:00'), (156.14, 156.7411, '2022-09-20 15:45:00'), (153.08, 153.9, '2022-09-20 13:30:00'), (149.49, 150.5499, '2022-09-19 13:50:00'), (148.47, 149.46, '2022-09-16 14:10:00'), (153.0, 153.74, '2022-09-15 17:00:00'), (154.03, 155.1881, '2022-09-14 13:40:00'), (157.32, 160.1, '2022-09-12 13:30:00')],
        #         'Supply Zones': [[145.55, 144.9804, '2022-09-29 13:40:00'], [148.37, 147.14, '2022-09-28 13:30:00'], [153.25, 152.72, '2022-09-27 15:10:00'], [153.81, 153.42, '2022-09-27 14:00:00'], [151.93, 151.42, '2022-09-26 16:20:00'], [153.76, 153.74, '2022-09-22 19:50:00'], [155.94, 155.395, '2022-09-21 19:35:00'], [153.2, 152.7, '2022-09-15 14:50:00'], [155.675, 155.03, '2022-09-08 16:00:00'], [155.3, 154.76, '2022-09-06 14:00:00'], [158.94, 158.36, '2022-08-31 19:50:00'], [160.23, 159.6401, '2022-08-30 14:30:00'], [162.43, 162.025, '2022-07-29 15:20:00'], [150.49, 149.97, '2022-07-18 17:30:00'], [148.67, 149.8401, '2022-07-15 13:35:00']]
        #     },
        #     {
        #         'Symbol': 'UAA',
        #         'Score': 6,
        #         'Limit': 6,
        #         'Budget': 203.0,
        #         'Client Id': 'GYNQGLGLPINXJNWLKFZM48NQ1C86KS8D',
        #         'Account Id': '277420951',
        #         'Sentiment': 'Positive',
        #         'Key Levels': [10.465, 10.16, 9.38, 8.53, 7.63, 6.86, 6.38, 27.28, 11.32, 10.48, 9.43, 8.58, 7.66],
        #         'Support Resistance': {
        #             'resistance': 6.86,
        #             'support': 6.38
        #         },
        #         'Demand Zones': [(6.38, 6.9925, '2022-09-30 13:40:00'), (9.08, 9.6, '2022-06-21 13:30:00'), (9.58, 10.13, '2022-06-13 19:55:00'), (9.935, 10.44, '2022-05-26 15:20:00'), (9.07, 9.58, '2022-05-24 19:55:00'), (9.21, 9.74, '2022-05-19 19:55:00'), (10.89, 11.43, '2022-05-06 15:20:00'), (10.39, 11.17, '2022-05-06 14:05:00'), (14.12, 14.63, '2022-05-05 19:50:00'), (17.13, 17.66, '2022-03-01 20:55:00'), (15.21, 15.9, '2022-02-24 14:35:00'), (17.74, 18.435, '2022-02-11 15:00:00'), (19.785, 20.42, '2022-02-10 20:55:00'), (18.645, 19.23, '2022-01-14 20:10:00')],
        #         'Supply Zones': [[6.81, 6.25, '2022-09-30 13:30:00'], [7.975, 7.475, '2022-09-28 19:55:00'], [9.39, 8.89, '2022-08-03 19:55:00'], [8.835, 8.345, '2022-06-29 14:45:00'], [9.33, 8.83, '2022-06-17 19:45:00'], [10.05, 9.55, '2022-05-26 15:00:00'], [10.07, 9.46, '2022-05-19 13:30:00'], [10.94, 10.435, '2022-05-06 19:55:00'], [11.34, 10.78, '2022-05-06 13:35:00'], [18.12, 17.5899, '2022-02-11 15:10:00'], [19.1, 18.57, '2022-02-11 14:30:00']]
        #     },
        #     {
        #         'Symbol': 'MSFT',
        #         'Score': 6,
        #         'Limit': 6,
        #         'Budget': 203.0,
        #         'Client Id': 'GYNQGLGLPINXJNWLKFZM48NQ1C86KS8D',
        #         'Account Id': '277420951',
        #         'Sentiment': 'Negative',
        #         'Key Levels': [294.18, 236.59, 235.5, 234.77, 232.73, 349.67, 236.68, 234.6, 233.44, 232.45, 231.58, 230.24, 229.41, 199.62],
        #         'Support Resistance': {
        #             'resistance': 234.77,
        #             'support': 232.73
        #         },
        #         'Demand Zones': [(237.16, 240.48, '2022-09-27 13:30:00'), (236.93, 237.54, '2022-09-26 13:30:00'), (237.57, 238.68, '2022-09-22 13:30:00'), (282.46, 284.46, '2022-08-08 13:35:00'), (278.68, 279.7, '2022-08-05 13:30:00'), (274.64, 277.44, '2022-08-03 13:30:00'), (264.42, 265.1285, '2022-07-27 18:35:00'), (250.65, 251.23, '2022-07-26 19:50:00'), (253.66, 256.22, '2022-07-15 13:30:00'), (252.08, 253.14, '2022-07-12 19:50:00'), (265.02, 265.67, '2022-07-07 13:30:00'), (263.28, 264.64, '2022-06-28 13:30:00'), (265.34, 265.91, '2022-06-24 19:50:00'), (264.05, 264.89, '2022-06-24 14:00:00'), (258.5, 262.31, '2022-06-24 13:30:00')],
        #         'Supply Zones': [[242.97, 242.73, '2022-09-22 19:50:00'], [258.85, 259.48, '2022-07-26 13:30:00'], [260.52, 260.015, '2022-07-12 13:55:00'], [264.65, 265.5, '2022-07-12 13:30:00'], [266.95, 267.64, '2022-06-27 13:30:00'], [260.58, 259.92, '2022-06-10 13:30:00'], [255.54, 254.9624, '2022-05-24 14:10:00'], [258.51, 257.13, '2022-05-12 13:30:00'], [287.2, 287.4793, '2022-04-27 15:05:00'], [285.19, 284.59, '2022-04-27 14:00:00'], [289.73, 289.09, '2022-04-11 13:55:00'], [303.93, 304.73, '2022-03-25 13:30:00'], [300.79, 300.18, '2022-03-23 13:30:00'], [287.11, 286.06, '2022-03-07 14:50:00'], [295.01, 294.5, '2022-02-25 14:30:00']]
        #     },
        #     {
        #         'Symbol': 'DIS',
        #         'Score': 6,
        #         'Limit': 6,
        #         'Budget': 203.0,
        #         'Client Id': 'GYNQGLGLPINXJNWLKFZM48NQ1C86KS8D',
        #         'Account Id': '277420951',
        #         'Sentiment': 'Negative',
        #         'Key Levels': [126.48, 98.54, 97.43, 96.51, 95.68, 94.28, 203.02, 98.35, 97.41, 96.35, 95.55, 94.41, 93.48, 92.43, 90.23],
        #         'Support Resistance': {
        #             'resistance': 95.68,
        #             'support': 94.28
        #         },
        #         'Demand Zones': [(120.99, 121.5, '2022-08-15 13:30:00'), (117.49, 119.5, '2022-08-12 13:30:00'), (117.9, 118.54, '2022-08-11 15:20:00'), (120.78, 121.7, '2022-08-11 14:00:00'), (99.36, 100.75, '2022-07-20 13:30:00'), (94.05, 94.62, '2022-07-15 15:55:00'), (91.735, 93.41, '2022-07-15 13:30:00'), (103.65, 105.86, '2022-05-13 13:30:00'), (103.04, 103.99, '2022-05-12 16:30:00'), (101.05, 102.0, '2022-05-12 14:05:00'), (99.47, 100.0573, '2022-05-12 13:40:00'), (106.48, 110.335, '2022-05-10 13:30:00'), (108.04, 108.61, '2022-05-09 13:30:00'), (117.0, 118.16, '2022-04-25 13:30:00'), (138.2, 141.85, '2022-03-29 13:30:00')],
        #         'Supply Zones': [[124.36, 124.23, '2022-08-16 13:30:00'], [120.0, 119.4801, '2022-08-11 14:55:00'], [121.71, 121.63, '2022-08-11 14:20:00'], [122.26, 121.66, '2022-08-11 13:50:00'], [109.245, 109.5, '2022-05-31 13:30:00'], [103.0, 102.4, '2022-05-12 13:30:00'], [126.57, 125.99, '2022-04-20 13:40:00'], [157.25, 156.68, '2022-02-10 15:10:00'], [138.88, 138.36, '2022-01-21 14:50:00']]
        #     },
        #     {
        #         'Symbol': 'AMD',
        #         'Score': 5,
        #         'Limit': 6,
        #         'Budget': 203.0,
        #         'Client Id': 'GYNQGLGLPINXJNWLKFZM48NQ1C86KS8D',
        #         'Account Id': '277420951',
        #         'Sentiment': 'Positive',
        #         'Key Levels': [104.59, 68.31, 67.51, 66.62, 65.37, 64.47, 63.61, 62.8301, 164.4599, 67.48],
        #         'Support Resistance': {
        #             'resistance': 63.61,
        #             'support': 62.8301
        #         },
        #         'Demand Zones': [(63.89, 64.74, '2022-09-30 14:05:00'), (63.54, 64.13, '2022-09-30 13:30:00'), (63.38, 64.0, '2022-09-29 19:30:00'), (67.1806, 67.95, '2022-09-28 14:15:00'), (66.56, 67.2201, '2022-09-28 13:35:00'), (66.7, 67.33, '2022-09-27 18:35:00'), (66.28, 67.06, '2022-09-27 18:00:00'), (66.25, 68.39, '2022-09-27 13:30:00'), (66.48, 67.21, '2022-09-26 18:45:00'), (66.375, 66.92, '2022-09-26 18:00:00'), (67.872, 68.52, '2022-09-26 14:10:00'), (67.75, 68.52, '2022-09-26 13:35:00'), (66.8216, 67.41, '2022-09-23 19:00:00'), (67.07, 67.7626, '2022-09-23 14:25:00'), (70.01, 70.675, '2022-09-22 19:45:00')],
        #         'Supply Zones': [[63.445, 62.925, '2022-09-29 18:15:00'], [64.74, 64.28, '2022-09-29 15:10:00'], [65.55, 64.91, '2022-09-29 14:00:00'], [67.22, 66.72, '2022-09-29 13:30:00'], [68.64, 68.225, '2022-09-28 16:20:00'], [66.8784, 66.3407, '2022-09-27 16:10:00'], [68.37, 67.7949, '2022-09-27 14:00:00'], [68.41, 67.8986, '2022-09-26 14:45:00'], [68.0297, 67.52, '2022-09-23 15:40:00'], [68.17, 67.61, '2022-09-23 14:55:00'], [67.77, 67.2501, '2022-09-23 14:05:00'], [70.23, 70.0, '2022-09-22 19:50:00'], [70.215, 69.715, '2022-09-22 18:10:00'], [71.9156, 71.3201, '2022-09-22 14:45:00'], [72.48, 72.0375, '2022-09-22 14:15:00']]
        #     },
        #     {
        #         'Symbol': 'AAL',
        #         'Score': 5,
        #         'Limit': 6,
        #         'Budget': 203.0,
        #         'Client Id': 'GYNQGLGLPINXJNWLKFZM48NQ1C86KS8D',
        #         'Account Id': '277420951',
        #         'Sentiment': 'Positive',
        #         'Key Levels': [15.71, 15.25, 14.43, 13.62, 12.47, 11.94, 11.85, 26.09, 16.52, 15.51, 14.49, 13.51, 12.58, 11.5, 10.9, 10.63],
        #         'Support Resistance': {
        #             'resistance': 12.47,
        #             'support': 11.94
        #         },
        #         'Demand Zones': [(12.15, 12.78, '2022-09-30 13:55:00'), (12.16, 12.69, '2022-09-29 13:45:00'), (12.08, 12.74, '2022-09-28 13:30:00'), (11.94, 12.5201, '2022-09-27 18:40:00'), (12.2, 12.785, '2022-09-27 14:20:00'), (12.14, 12.8, '2022-09-27 13:55:00'), (11.85, 12.69, '2022-09-27 13:30:00'), (12.21, 12.78, '2022-09-26 14:10:00'), (12.03, 12.57, '2022-09-26 13:45:00'), (12.12, 12.64, '2022-09-23 19:50:00'), (11.87, 12.4842, '2022-09-23 19:05:00'), (11.93, 12.455, '2022-09-23 15:00:00'), (12.2, 12.75, '2022-09-23 14:05:00'), (12.61, 13.125, '2022-09-22 15:05:00'), (12.705, 13.22, '2022-09-22 14:10:00')],
        #         'Supply Zones': [[12.285, 11.81, '2022-09-29 14:10:00'], [12.79, 12.285, '2022-09-28 19:55:00'], [12.58, 12.1, '2022-09-28 14:05:00'], [12.3, 11.8701, '2022-09-27 14:00:00'], [12.285, 11.875, '2022-09-26 14:00:00'], [12.225, 11.785, '2022-09-23 14:30:00'], [12.79, 12.27, '2022-09-22 19:55:00'], [12.78, 12.395, '2022-09-22 15:50:00'], [13.305, 12.805, '2022-09-21 19:55:00'], [13.85, 13.35, '2022-09-21 13:30:00'], [13.895, 13.38, '2022-09-20 17:25:00'], [14.4, 13.96, '2022-09-15 14:15:00'], [14.05, 13.55, '2022-09-13 14:20:00'], [14.625, 14.155, '2022-09-12 14:00:00'], [13.53, 13.025, '2022-09-07 13:55:00']]
        #     },
        #     {
        #         'Symbol': 'TEVA',
        #         'Score': 5,
        #         'Limit': 6,
        #         'Budget': 203.0,
        #         'Client Id': 'GYNQGLGLPINXJNWLKFZM48NQ1C86KS8D',
        #         'Account Id': '277420951',
        #         'Sentiment': 'Negative',
        #         'Key Levels': [11.34, 11.14, 10.31, 9.46, 8.65, 7.63, 7.04, 13.3, 12.34, 11.36, 10.42, 9.47, 8.55, 7.62, 6.88, 6.7791],
        #         'Support Resistance': {
        #             'resistance': 8.65,
        #             'support': 7.63
        #         },
        #         'Demand Zones': [(8.96, 9.465, '2022-09-08 19:45:00'), (8.52, 9.02, '2022-09-06 13:40:00'), (8.84, 9.385, '2022-09-01 19:55:00'), (9.03, 9.635, '2022-08-31 13:50:00'), (10.16, 10.735, '2022-08-18 14:50:00'), (9.92, 10.53, '2022-08-17 18:10:00'), (9.4, 10.185, '2022-08-17 16:55:00'), (11.06, 11.56, '2022-08-11 19:50:00'), (10.955, 11.475, '2022-08-09 19:55:00'), (10.82, 11.33, '2022-08-09 13:30:00'), (10.55, 11.055, '2022-08-05 18:25:00'), (10.08, 10.75, '2022-08-05 13:30:00'), (9.95, 10.5, '2022-08-04 13:35:00'), (9.7401, 10.28, '2022-08-02 19:55:00'), (9.18, 9.73, '2022-08-01 13:30:00')],
        #         'Supply Zones': [[8.66, 8.13, '2022-09-06 13:30:00'], [10.05, 9.54, '2022-08-18 13:45:00'], [9.7583, 9.2583, '2022-08-17 16:45:00'], [10.41, 9.9, '2022-08-17 14:05:00'], [10.82, 10.3, '2022-08-17 13:30:00'], [10.675, 10.175, '2022-08-05 18:10:00'], [9.45, 8.945, '2022-07-28 18:00:00'], [9.23, 8.76, '2022-07-28 13:50:00'], [8.88, 8.395, '2022-07-27 15:00:00'], [8.8, 8.35, '2022-07-27 14:30:00'], [8.66, 8.1705, '2022-07-27 14:00:00'], [7.16, 8.1, '2022-07-27 13:30:00'], [7.2101, 6.71, '2022-07-21 14:00:00'], [7.015, 6.51, '2022-07-11 18:10:00'], [8.54, 7.91, '2022-05-04 13:30:00']]
        #     },
        #     {
        #         'Symbol': 'SOFI',
        #         'Score': 5,
        #         'Limit': 6,
        #         'Budget': 203.0,
        #         'Client Id': 'GYNQGLGLPINXJNWLKFZM48NQ1C86KS8D',
        #         'Account Id': '277420951',
        #         'Sentiment': '',
        #         'Key Levels': [8.52, 8.16, 7.6, 6.32, 5.63, 4.91, 4.79, 28.26, 9.53, 8.41, 7.42, 6.41, 5.64],
        #         'Support Resistance': {
        #             'resistance': 4.91,
        #             'support': 4.79
        #         },
        #         'Demand Zones': [(4.86, 5.4199, '2022-09-30 14:10:00'), (4.81, 5.3517, '2022-09-30 13:45:00'), (4.8201, 5.335, '2022-09-29 19:40:00'), (4.885, 5.4, '2022-09-29 14:15:00'), (5.095, 5.625, '2022-09-28 14:20:00'), (5.04, 5.585, '2022-09-28 13:40:00'), (5.0, 5.515, '2022-09-27 18:25:00'), (5.01, 5.515, '2022-09-27 16:20:00'), (5.2, 5.705, '2022-09-27 14:10:00'), (5.08, 5.7352, '2022-09-27 13:35:00'), (5.39, 5.935, '2022-09-26 14:10:00'), (5.285, 5.8, '2022-09-26 13:30:00'), (5.24, 5.765, '2022-09-23 19:30:00'), (5.16, 5.675, '2022-09-23 15:05:00'), (5.16, 5.6799, '2022-09-23 14:25:00')],
        #         'Supply Zones': [[4.99, 4.485, '2022-09-29 14:05:00'], [5.11, 4.6006, '2022-09-29 13:35:00'], [5.0786, 4.5773, '2022-09-27 16:10:00'], [5.21, 4.705, '2022-09-27 15:25:00'], [5.315, 4.81, '2022-09-26 15:35:00'], [5.46, 4.9546, '2022-09-26 14:40:00'], [5.475, 4.96, '2022-09-26 13:40:00'], [5.28, 4.7728, '2022-09-23 15:40:00'], [5.22, 4.71, '2022-09-23 14:55:00'], [5.4299, 4.935, '2022-09-22 19:50:00'], [5.44, 4.935, '2022-09-22 18:05:00'], [5.73, 5.21, '2022-09-22 13:35:00'], [5.79, 5.29, '2022-09-21 19:35:00'], [5.955, 5.44, '2022-09-21 19:00:00'], [6.12, 5.62, '2022-09-16 15:20:00']]
        #     },
        #     {
        #         'Symbol': 'SPY',
        #         'Score': 5,
        #         'Limit': 6,
        #         'Budget': 203.0,
        #         'Client Id': 'GYNQGLGLPINXJNWLKFZM48NQ1C86KS8D',
        #         'Account Id': '277420951',
        #         'Sentiment': 'Negative',
        #         'Key Levels': [431.73, 361.59, 360.61, 359.66, 357.11, 479.98, 358.36, 357.61, 355.32, 354.34, 322.6],
        #         'Support Resistance': {
        #             'resistance': 359.66,
        #             'support': 357.11
        #         },
        #         'Demand Zones': [(363.48, 364.55, '2022-09-26 18:45:00'), (363.1619, 363.79, '2022-09-26 18:00:00'), (395.47, 396.245, '2022-07-27 18:35:00'), (380.86, 382.015, '2022-07-06 18:25:00'), (375.75, 377.85, '2022-07-01 13:45:00'), (387.44, 390.72, '2022-06-28 13:30:00'), (383.51, 384.33, '2022-06-24 14:00:00'), (374.8, 375.68, '2022-06-23 13:55:00'), (377.85, 379.59, '2022-06-15 19:20:00'), (372.12, 373.97, '2022-06-15 18:35:00'), (396.4, 399.17, '2022-05-26 13:30:00'), (382.74, 384.185, '2022-05-20 19:25:00'), (388.48, 389.1522, '2022-05-19 17:50:00'), (388.47, 389.06, '2022-05-19 13:30:00'), (398.35, 399.235, '2022-05-13 18:50:00')],
        #         'Supply Zones': [[365.26, 364.65, '2022-09-29 13:40:00'], [381.93, 381.36, '2022-09-21 19:35:00'], [399.69, 399.01, '2022-09-08 16:00:00'], [391.65, 391.15, '2022-09-06 14:00:00'], [418.555, 417.98, '2022-08-26 14:20:00'], [381.35, 380.73, '2022-06-29 13:30:00'], [382.35, 383.085, '2022-06-15 19:35:00'], [377.47, 377.64, '2022-06-15 18:00:00'], [393.39, 392.89, '2022-06-10 14:00:00'], [413.93, 413.05, '2022-05-31 13:30:00'], [406.59, 406.78, '2022-05-17 18:10:00'], [400.84, 400.94, '2022-05-11 16:25:00'], [410.05, 409.52, '2022-05-06 13:35:00'], [414.77, 414.15, '2022-05-05 15:50:00'], [419.78, 418.5686, '2022-04-27 14:10:00']]
        #     },
        #     {
        #         'Symbol': 'PFE',
        #         'Score': 5,
        #         'Limit': 6,
        #         'Budget': 203.0,
        #         'Client Id': 'GYNQGLGLPINXJNWLKFZM48NQ1C86KS8D',
        #         'Account Id': '277420951',
        #         'Sentiment': 'Positive',
        #         'Key Levels': [52.85, 47.63, 46.4, 45.68, 44.45, 43.85, 43.52, 61.71, 48.5, 47.52, 46.44, 45.52, 43.49, 42.49, 41.53, 40.32, 39.46, 32.7990008],
        #         'Support Resistance': {
        #             'resistance': 43.85,
        #             'support': 43.52
        #         },
        #         'Demand Zones': [(45.14, 45.64, '2022-09-01 13:30:00'), (48.2, 49.05, '2022-08-12 13:30:00'), (50.15, 50.82, '2022-08-01 13:30:00'), (49.48, 50.02, '2022-07-28 13:35:00'), (52.685, 53.45, '2022-07-07 13:30:00'), (51.5, 52.14, '2022-07-06 13:30:00'), (47.31, 47.88, '2022-06-17 13:30:00'), (47.91, 48.49, '2022-06-15 18:35:00'), (53.15, 53.77, '2022-06-07 13:30:00'), (52.41, 52.96, '2022-06-03 13:30:00'), (53.655, 54.56, '2022-05-26 13:30:00'), (53.19, 53.7, '2022-05-25 13:30:00'), (52.64, 53.21, '2022-05-24 13:30:00'), (52.36, 53.08, '2022-05-23 13:30:00'), (50.47, 51.765, '2022-05-20 13:30:00')],
        #         'Supply Zones': [[49.69, 49.16, '2022-08-12 13:45:00'], [49.1, 48.47, '2022-08-11 13:35:00'], [49.3, 48.89, '2022-08-08 13:30:00'], [50.62, 50.06, '2022-07-29 13:30:00'], [51.89, 51.25, '2022-07-05 13:30:00'], [51.89, 51.38, '2022-06-28 13:35:00'], [47.99, 47.38, '2022-06-15 13:30:00'], [51.37, 50.81, '2022-06-10 13:30:00'], [52.37, 51.87, '2022-06-02 13:30:00'], [53.09, 52.87, '2022-05-31 19:55:00'], [53.41, 52.82, '2022-05-31 13:30:00'], [50.725, 50.21, '2022-05-17 13:35:00'], [50.52, 50.2478, '2022-05-13 13:40:00'], [48.56, 47.88, '2022-05-09 13:30:00'], [49.12, 48.39, '2022-05-04 13:30:00']]
        #     },
        #     {
        #         'Symbol': 'BAC',
        #         'Score': 5,
        #         'Limit': 6,
        #         'Budget': 203.0,
        #         'Client Id': 'GYNQGLGLPINXJNWLKFZM48NQ1C86KS8D',
        #         'Account Id': '277420951',
        #         'Sentiment': 'Positive',
        #         'Key Levels': [36.94, 35.41, 34.42, 33.58, 32.73, 31.26, 30.64, 30.165, 50.11, 34.48, 33.49, 32.54, 31.48, 30.47, 29.47, 28.66, 27.45, 26.76, 23.12],
        #         'Support Resistance': {
        #             'resistance': 30.64,
        #             'support': 30.165
        #         },
        #         'Demand Zones': [(30.895, 31.51, '2022-09-30 15:05:00'), (30.58, 31.215, '2022-09-30 13:50:00'), (30.71, 31.285, '2022-09-28 14:20:00'), (30.6412, 31.315, '2022-09-28 13:50:00'), (31.32, 31.82, '2022-09-26 13:30:00'), (
        #     31.23, 31.75, '2022-09-23 19:00:00'), (33.63, 34.22, '2022-09-19 13:30:00'), (33.7, 34.42, '2022-09-15 13:40:00'), (34.225, 34.84, '2022-09-08 14:30:00'), (33.27, 33.93, '2022-09-08 13:40:00'), (33.75, 34.45, '2022-08-31 13:40:00'), (36.11, 36.7, '2022-08-17 13:30:00'), (33.82, 35.07, '2022-08-10 13:35:00'), (33.41, 34.05, '2022-08-09 13:35:00'), (33.61, 34.355, '2022-08-05 14:00:00')],
        #         'Supply Zones': [[30.715, 30.215, '2022-09-29 13:40:00'], [31.8, 31.29, '2022-09-23 13:55:00'], [32.66, 32.145, '2022-09-22 19:55:00'], [32.95, 32.49, '2022-09-22 14:15:00'], [33.29, 32.81, '2022-09-22 13:30:00'], [33.6299, 33.1299, '2022-09-21 19:35:00'], [34.255, 33.81, '2022-09-21 13:30:00'], [34.05, 33.505, '2022-09-16 13:50:00'], [33.055, 32.55, '2022-09-06 14:00:00'], [33.44, 33.2201, '2022-09-06 13:35:00'], [34.165, 33.635, '2022-09-02 14:00:00'], [32.99, 32.47, '2022-09-01 14:00:00'], [33.82, 33.3, '2022-08-31 19:55:00'], [33.62, 33.1, '2022-08-02 13:30:00'], [33.87, 33.335, '2022-08-01 14:15:00']]
        # },
        #     {
        #         'Symbol': 'CRM',
        #         'Score': 5,
        #         'Limit': 6,
        #         'Budget': 203.0,
        #         'Client Id': 'GYNQGLGLPINXJNWLKFZM48NQ1C86KS8D',
        #         'Account Id': '277420951',
        #         'Sentiment': '',
        #         'Key Levels': [194.37, 148.59, 147.42, 146.47, 145.54, 143.75, 311.75],
        #         'Support Resistance': {
        #             'resistance': 145.54,
        #             'support': 143.75
        #         },
        #         'Demand Zones': [(147.57, 150.01, '2022-09-22 13:30:00'), (172.4532, 173.69, '2022-08-25 19:55:00'), (165.77, 167.3, '2022-08-25 13:55:00'), (158.17, 159.02, '2022-06-17 13:30:00'), (187.3303, 188.01, '2022-06-02 19:55:00'), (160.19, 178.51, '2022-06-01 13:30:00'), (214.1, 218.57, '2022-04-04 13:40:00'), (206.26, 207.01, '2022-03-02 14:40:00'), (184.44, 185.24, '2022-02-24 14:30:00')],
        #         'Supply Zones': [[182.89, 182.38, '2022-06-01 14:05:00'], [209.21, 215.6, '2022-03-02 14:30:00']]
        #     },
        #     {
        #         'Symbol': 'BABA',
        #         'Score': 5,
        #         'Limit': 6,
        #         'Budget': 203.0,
        #         'Client Id': 'GYNQGLGLPINXJNWLKFZM48NQ1C86KS8D',
        #         'Account Id': '277420951',
        #         'Sentiment': 'Positive',
        #         'Key Levels': [105.25, 83.44, 82.48, 81.35, 80.59, 79.47, 78.51, 77.64, 76.28, 319.32, 84.65, 82.34, 81.48, 80.64, 79.41, 73.28],
        #         'Support Resistance': {
        #             'resistance': 80.59,
        #             'support': 79.47
        #         },
        #         'Demand Zones': [(78.65, 79.24, '2022-09-30 13:30:00'), (76.28, 76.985, '2022-09-28 13:30:00'), (95.7, 96.94, '2022-08-31 13:40:00'), (97.71, 99.43, '2022-08-29 13:30:00'), (99.29, 100.095, '2022-08-26 14:40:00'), (98.9, 99.84, '2022-08-25 14:35:00'), (94.95, 96.31, '2022-08-25 13:55:00'), (92.43, 96.56, '2022-08-25 13:30:00'), (91.66, 92.7, '2022-08-24 14:45:00'), (90.74, 91.9101, '2022-08-24 14:10:00'), (87.55, 89.01, '2022-08-24 13:35:00'), (87.96, 89.1, '2022-08-23 14:10:00'), (86.71, 87.25, '2022-08-23 13:45:00'), (92.67, 93.7, '2022-08-15 13:30:00'), (91.43, 92.01, '2022-08-12 13:30:00')],
        #         'Supply Zones': [[81.94, 82.335, '2022-09-22 13:35:00'], [85.16, 84.62, '2022-09-21 13:30:00'], [102.65, 102.125, '2022-08-26 14:20:00'], [102.81, 102.26, '2022-08-26 13:40:00'], [100.2138, 99.7138, '2022-08-25 15:00:00'], [89.75, 88.67, '2022-08-23 13:30:00'], [97.4, 97.13, '2022-08-04 14:35:00'], [100.03, 99.21, '2022-08-04 13:50:00'], [94.09, 93.7, '2022-08-02 15:00:00'], [90.0098, 89.5, '2022-07-29 19:45:00'], [91.15, 90.65, '2022-07-29 17:00:00'], [91.8, 91.298, '2022-07-29 16:25:00'], [94.26, 93.72, '2022-07-29 13:30:00'], [98.13, 97.63, '2022-07-28 14:05:00'], [100.606, 100.06, '2022-07-28 13:40:00']]
        #     },
        #     {
        #         'Symbol': 'PYPL',
        #         'Score': 5,
        #         'Limit': 6,
        #         'Budget': 203.0,
        #         'Client Id': 'GYNQGLGLPINXJNWLKFZM48NQ1C86KS8D',
        #         'Account Id': '277420951',
        #         'Sentiment': 'Negative',
        #         'Key Levels': [103.03, 90.54, 89.43, 88.44, 87.53, 86.48, 85.49, 84.71, 83.44, 82.29, 81.59, 76.71, 310.16, 90.61, 89.47, 88.54, 87.54, 86.66, 85.47, 84.35, 83.49, 82.31, 81.42, 67.58],
        #         'Support Resistance': {
        #             'resistance': 86.48,
        #             'support': 85.49
        #         },
        #         'Demand Zones': [(90.395, 90.97, '2022-09-20 13:50:00'), (95.02, 95.65, '2022-09-15 13:30:00'), (93.8, 94.76, '2022-09-13 13:30:00'), (96.36, 97.71, '2022-08-10 13:45:00'), (95.27, 96.85, '2022-08-08 13:30:00'), (99.86, 100.77, '2022-08-03 13:45:00'), (83.37, 84.9, '2022-07-27 14:55:00'), (79.72, 80.29, '2022-07-21 13:30:00'), (76.85, 77.38, '2022-07-20 13:30:00'), (73.38, 75.618, '2022-07-18 13:30:00'), (69.45, 71.03, '2022-07-15 13:30:00'), (75.52, 76.13, '2022-06-27 19:55:00'), (73.72, 75.29, '2022-06-24 13:30:00'), (70.87, 71.83, '2022-06-17 13:30:00'), (85.2, 86.125, '2022-06-02 16:35:00')],
        #         'Supply Zones': [[101.63, 102.09, '2022-08-16 13:30:00'], [94.7344, 97.49, '2022-08-10 13:30:00'], [101.66, 101.12, '2022-08-03 14:05:00'], [80.22, 79.5304, '2022-07-26 13:30:00'], [85.37, 84.82, '2022-05-06 13:30:00'], [90.73, 90.23, '2022-05-05 13:30:00'], [88.97, 88.47, '2022-04-29 19:50:00'], [92.32, 92.5, '2022-04-28 18:00:00'], [89.54, 90.28, '2022-04-28 17:00:00'], [82.91, 86.59, '2022-04-28 13:35:00'], [102.0, 101.47, '2022-04-20 13:30:00'], [121.9, 122.2, '2022-04-05 13:30:00'], [99.05, 98.55, '2022-03-10 14:30:00'], [95.33, 97.96, '2022-03-09 14:35:00'], [106.28, 105.15, '2022-03-02 14:30:00']]
        #     },
        #     {
        #         'Symbol': 'DKNG',
        #         'Score': 5,
        #         'Limit': 6,
        #         'Budget': 203.0,
        #         'Client Id': 'GYNQGLGLPINXJNWLKFZM48NQ1C86KS8D',
        #         'Account Id': '277420951',
        #         'Sentiment': '',
        #         'Key Levels': [21.45, 20.58, 19.41, 18.47, 17.5, 16.47, 15.5, 14.66, 13.46, 12.72, 12.56, 74.38, 19.37, 18.51, 17.49, 16.51, 15.53, 14.45, 13.5, 12.59, 11.54, 10.58, 9.77],
        #         'Support Resistance': {
        #             'resistance': 15.5,
        #             'support': 14.66
        #         },
        #         'Demand Zones': [(15.44, 16.0, '2022-09-28 18:00:00'), (15.21, 15.765, '2022-09-27 19:55:00'), (14.57, 15.56, '2022-09-27 13:30:00'), (14.89, 15.64, '2022-09-26 13:30:00'), (15.19, 15.86, '2022-09-23 13:30:00'), (16.065, 16.76, '2022-09-22 14:00:00'), (17.8408, 18.4501, '2022-09-20 14:05:00'), (18.4531, 19.02, '2022-09-16 19:50:00'), (19.03, 19.66, '2022-09-15 14:35:00'), (19.14, 19.9501, '2022-09-15 14:05:00'), (18.17, 19.05, '2022-09-15 13:40:00'), (17.925, 18.47, '2022-09-14 19:40:00'), (17.06, 18.06, '2022-09-13 13:55:00'), (17.2, 17.88, '2022-09-13 13:30:00'), (17.585, 18.35, '2022-09-12 13:30:00')],
        #         'Supply Zones': [[15.685, 15.1799, '2022-09-22 19:55:00'], [17.0287, 16.5, '2022-09-21 13:55:00'], [18.69, 17.97, '2022-09-20 13:30:00'], [19.29, 18.785, '2022-09-15 14:50:00'], [19.75, 19.22, '2022-09-15 14:20:00'], [17.97, 17.565, '2022-09-14 15:50:00'], [17.86, 17.4511, '2022-09-13 14:15:00'], [17.77, 17.2391, '2022-09-12 14:55:00'], [15.49, 15.33, '2022-09-06 13:30:00'], [15.87, 15.780000000000001, '2022-09-02 13:30:00'], [17.07, 16.51, '2022-08-26 14:25:00'], [17.38, 16.86, '2022-08-22 16:50:00'], [19.68, 19.1773, '2022-08-18 19:55:00'], [20.235, 19.73, '2022-08-18 13:45:00'], [21.03, 20.52, '2022-08-16 19:50:00']]
        #     },
        #     {
        #         'Symbol': 'WMT',
        #         'Score': 5,
        #         'Limit': 6,
        #         'Budget': 203.0,
        #         'Client Id': 'GYNQGLGLPINXJNWLKFZM48NQ1C86KS8D',
        #         'Account Id': '277420951',
        #         'Sentiment': 'Neutral',
        #         'Key Levels': [142.72, 134.5, 133.46, 132.55, 131.52, 130.54, 129.57, 128.61, 127.57, 126.34, 125.54, 120.06, 160.77, 134.51, 133.42, 132.5, 131.49, 130.43, 129.71, 128.54, 127.58, 126.48, 125.36, 117.27],
        #         'Support Resistance': {
        #             'resistance': 130.54,
        #             'support': 129.57
        #         },
        #         'Demand Zones': [(121.0267, 121.74, '2022-07-27 13:30:00'), (119.92, 120.52, '2022-06-17 13:30:00'), (134.36, 135.3, '2022-05-17 14:00:00'), (153.63, 154.175, '2022-04-11 19:55:00'), (133.38, 134.5, '2022-02-17 14:30:00')],
        #         'Supply Zones': [[128.27, 127.53, '2022-05-18 13:30:00']]
        #     },
        #     {
        #         'Symbol': 'JPM',
        #         'Score': 5,
        #         'Limit': 6,
        #         'Budget': 203.0,
        #         'Client Id': 'GYNQGLGLPINXJNWLKFZM48NQ1C86KS8D',
        #         'Account Id': '277420951',
        #         'Sentiment': 'Neutral',
        #         'Key Levels': [124.2389, 108.3, 107.51, 106.45, 105.55, 104.81, 104.398, 172.96, 108.21, 107.64, 103.24, 102.53, 101.55, 100.5, 94.332],
        #         'Support Resistance': {
        #             'resistance': 104.81,
        #             'support': 104.398
        #         },
        #         'Demand Zones': [(112.05, 113.83, '2022-08-05 13:40:00'), (106.06, 106.63, '2022-07-14 13:45:00'), (112.0221, 112.65, '2022-06-30 19:55:00'), (112.83, 113.73, '2022-06-24 13:30:00'), (124.15, 124.86, '2022-05-24 13:30:00'), (117.02, 117.85, '2022-05-12 19:55:00'), (122.76, 123.3, '2022-05-04 13:30:00'), (126.01, 127.44, '2022-04-13 13:30:00'), (128.22, 133.4, '2022-03-09 14:30:00'), (150.94, 152.63, '2022-02-18 14:30:00'), (152.7, 156.0, '2022-02-08 14:35:00')],
        #         'Supply Zones': [[111.845, 111.31, '2022-09-22 19:55:00'], [107.96, 108.66, '2022-07-15 13:30:00'], [111.215, 110.27, '2022-07-05 13:30:00'], [125.86, 126.68, '2022-05-24 13:45:00'], [117.49, 116.5, '2022-05-12 13:30:00'], [140.76, 139.5, '2022-03-28 13:30:00'], [140.07, 139.69, '2022-03-18 13:30:00'], [132.08, 131.27, '2022-03-07 14:35:00'], [139.21, 138.7, '2022-03-01 14:50:00'], [153.31, 152.82, '2022-02-04 20:55:00'], [145.0, 143.69, '2022-01-28 14:30:00'], [143.65, 143.15, '2022-01-25 14:30:00'], [151.05, 151.07, '2022-01-19 14:30:00']]
        #     },
        #     {
        #         'Symbol': 'PBR',
        #         'Score': 5,
        #         'Limit': 6,
        #         'Budget': 203.0,
        #         'Client Id': 'GYNQGLGLPINXJNWLKFZM48NQ1C86KS8D',
        #         'Account Id': '277420951',
        #         'Sentiment': '',
        #         'Key Levels': [15.83, 15.38, 14.38, 13.61, 12.41, 11.94, 11.845, 16.295, 15.39, 14.44, 13.56, 12.49, 11.38, 10.57, 9.61, 8.4, 7.4, 6.15],
        #         'Support Resistance': {
        #             'resistance': 12.41,
        #             'support': 11.94
        #         },
        #         'Demand Zones': [(12.245, 12.775, '2022-09-30 15:30:00'), (12.0, 12.54, '2022-09-30 13:30:00'), (12.055, 12.585, '2022-09-29 19:30:00'), (11.9177, 12.475, '2022-09-29 14:50:00'), (11.945, 12.51, '2022-09-28 14:30:00'), (12.24, 12.76, '2022-09-27 13:50:00'), (12.32, 12.83, '2022-09-26 14:35:00'), (12.51, 13.025, '2022-09-23 19:40:00'), (12.47, 12.975, '2022-09-23 19:00:00'), (12.6, 13.125, '2022-09-23 15:00:00'), (13.34, 13.855, '2022-09-21 18:15:00'), (13.385, 13.93, '2022-09-20 13:35:00'), (13.155, 13.725, '2022-09-19 14:40:00'), (13.1431, 13.675, '2022-09-15 19:35:00'), (13.18, 13.69, '2022-09-15 15:00:00')],
        #         'Supply Zones': [[12.24, 11.72, '2022-09-28 13:55:00'], [12.31, 11.84, '2022-09-28 13:30:00'], [12.715, 12.205, '2022-09-23 17:35:00'], [12.93, 12.425, '2022-09-23 13:55:00'], [13.28, 12.75, '2022-09-23 13:30:00'], [13.64, 13.11, '2022-09-22 13:45:00'], [13.745, 13.24, '2022-09-06 13:50:00'], [14.345, 13.845, '2022-08-31 19:20:00'], [14.76, 14.25, '2022-08-30 13:40:00'], [14.62, 14.11, '2022-08-25 14:00:00'], [14.71, 14.32, '2022-08-25 13:30:00'], [13.45, 13.02, '2022-08-22 14:20:00'], [13.48, 12.95, '2022-08-22 13:30:00'], [13.675, 13.195, '2022-08-12 19:45:00'], [15.2, 14.69, '2022-08-11 19:10:00']]
        #     },
        #     {
        #         'Symbol': 'UAL',
        #         'Score': 5,
        #         'Limit': 6,
        #         'Budget': 203.0,
        #         'Client Id': 'GYNQGLGLPINXJNWLKFZM48NQ1C86KS8D',
        #         'Account Id': '277420951',
        #         'Sentiment': 'Positive',
        #         'Key Levels': [40.77, 37.48, 36.43, 35.63, 34.5, 33.38, 32.56, 31.91, 31.8, 63.6999, 37.44, 36.49, 35.49, 34.46, 33.55, 30.54],
        #         'Support Resistance': {
        #             'resistance': 32.56,
        #             'support': 31.91
        #         },
        #         'Demand Zones': [(35.788, 36.36, '2022-07-28 13:30:00'), (37.47, 38.54, '2022-07-21 13:45:00'), (39.16, 40.72, '2022-07-19 13:30:00'), (36.45, 37.45, '2022-07-13 13:40:00'), (35.09, 35.71, '2022-06-30 19:55:00'), (37.12, 38.64, '2022-06-28 13:30:00'), (35.87, 36.91, '2022-06-24 13:45:00'), (36.15, 36.66, '2022-06-17 19:50:00'), (34.54, 35.11, '2022-06-16 19:55:00'), (37.1, 37.77, '2022-06-15 18:35:00'), (36.92, 38.3356, '2022-06-15 13:35:00'), (48.02, 48.59, '2022-05-27 19:50:00'), (42.8, 44.11, '2022-05-26 13:30:00'), (43.87, 44.44, '2022-05-19 13:30:00'), (41.01, 41.705, '2022-05-12 19:55:00')],
        #         'Supply Zones': [[36.28, 35.78, '2022-09-21 19:55:00'], [38.97, 38.31, '2022-07-21 13:30:00'], [35.16, 34.65, '2022-07-05 13:30:00'], [36.37, 36.85, '2022-06-21 13:30:00'], [36.585, 35.93, '2022-06-16 13:30:00'], [46.97, 46.32, '2022-05-31 14:00:00'], [51.4799, 50.99, '2022-04-25 13:40:00'], [51.75, 51.23, '2022-04-21 16:25:00'], [43.335, 42.76, '2022-04-06 13:30:00'], [35.09, 38.07, '2022-03-15 13:35:00'], [35.22, 35.44, '2022-03-11 14:30:00'], [33.12, 32.59, '2022-03-08 18:30:00'], [31.49, 31.02, '2022-03-07 20:30:00'], [31.745, 31.245, '2022-03-07 19:40:00'], [33.76, 33.225, '2022-03-07 15:35:00']]
        #     },
        #     {
        #         'Symbol': 'RIVN',
        #         'Score': 5,
        #         'Limit': 6,
        #         'Budget': 203.0,
        #         'Client Id': 'GYNQGLGLPINXJNWLKFZM48NQ1C86KS8D',
        #         'Account Id': '277420951',
        #         'Sentiment': '',
        #         'Key Levels': [40.86, 37.5, 36.5, 35.46, 34.46, 33.45, 32.47, 31.56, 30.87, 30.71, 179.4699, 37.54, 36.49, 35.4, 34.52, 33.5, 32.49, 31.62, 30.53, 29.58, 28.58, 27.49, 19.25],
        #         'Support Resistance': {
        #             'resistance': 33.45,
        #             'support': 32.47
        #         },
        #         'Demand Zones': [(39.91, 40.93, '2022-09-15 14:15:00'), (39.45, 40.21, '2022-09-15 13:30:00'), (38.4, 39.465, '2022-09-13 14:10:00'), (36.85, 38.13, '2022-09-13 13:40:00'), (37.53, 38.15, '2022-09-12 13:30:00'), (36.78, 37.475, '2022-09-09 17:10:00'), (35.21, 35.91, '2022-09-09 13:45:00'), (34.6, 35.18, '2022-09-08 14:55:00'), (35.31, 35.98, '2022-09-08 14:15:00'), (33.21, 35.74, '2022-09-08 13:30:00'), (33.525, 34.5299, '2022-09-06 14:50:00'), (32.02, 32.89, '2022-09-06 13:40:00'), (32.685, 33.34, '2022-08-31 19:50:00'), (32.285, 33.24, '2022-08-30 13:30:00'), (37.23, 37.74, '2022-08-15 13:30:00')],
        #         'Supply Zones': [[35.22, 34.84, '2022-09-29 13:30:00'], [35.93, 35.43, '2022-09-20 19:55:00'], [39.66, 39.16, '2022-09-16 19:50:00'], [37.77, 38.37, '2022-09-12 13:35:00'], [36.45, 35.79, '2022-09-09 13:30:00'], [35.75, 35.25, '2022-09-08 14:20:00'], [36.4999, 35.96, '2022-09-08 13:55:00'], [32.87, 32.52, '2022-08-31 19:55:00'], [37.04, 36.46, '2022-08-16 13:30:00'], [38.91, 38.44, '2022-08-15 13:35:00'], [40.21, 39.56, '2022-08-12 14:40:00'], [40.15, 39.6, '2022-08-12 14:00:00'], [37.585, 38.1, '2022-08-11 13:30:00'], [36.21, 35.59, '2022-08-05 13:30:00'], [36.71, 36.19, '2022-08-04 14:30:00']]
        #     },
        #     {
        #         'Symbol': 'FCX',
        #         'Score': 5,
        #         'Limit': 6,
        #         'Budget': 203.0,
        #         'Client Id': 'GYNQGLGLPINXJNWLKFZM48NQ1C86KS8D',
        #         'Account Id': '277420951',
        #         'Sentiment': 'Negative',
        #         'Key Levels': [33.89, 31.46, 30.43, 29.61, 28.43, 27.68, 26.56, 26.033, 51.99, 31.48, 30.56, 29.55, 28.47, 27.52, 26.48, 25.35, 24.57, 23.55, 15.22],
        #         'Support Resistance': {
        #             'resistance': 27.68,
        #             'support': 26.56
        #         },
        #         'Demand Zones': [(26.915, 27.72, '2022-09-28 13:45:00'), (26.12, 26.68, '2022-09-26 13:30:00'), (26.335, 26.91, '2022-09-23 19:30:00'), (28.755, 29.575, '2022-09-19 13:45:00'), (29.7418, 30.36, '2022-09-14 19:50:00'), (30.47, 31.8, '2022-09-09 13:30:00'), (31.31, 32.45, '2022-08-25 13:35:00'), (31.34, 32.59, '2022-08-11 13:30:00'), (30.16, 31.44, '2022-08-08 13:30:00'), (29.775, 30.475, '2022-08-05 14:20:00'), (28.5, 29.12, '2022-08-05 13:30:00'), (28.7, 29.27, '2022-07-21 19:55:00'), (28.7, 29.2999, '2022-07-20 14:45:00'), (27.26, 27.795, '2022-07-18 19:50:00'), (27.28, 27.985, '2022-07-18 14:35:00')],
        #         'Supply Zones': [[27.97, 27.48, '2022-09-29 13:30:00'], [28.48, 27.97, '2022-09-22 19:55:00'], [30.16, 29.62, '2022-09-14 14:10:00'], [31.61, 31.105, '2022-09-09 14:30:00'], [28.52, 27.99, '2022-09-01 13:30:00'], [33.19, 33.32, '2022-08-26 13:30:00'], [30.3, 31.0, '2022-08-08 13:35:00'], [29.33, 28.75, '2022-08-02 13:40:00'], [28.79, 29.125, '2022-07-22 13:30:00'], [29.09, 28.535, '2022-07-20 13:40:00'], [25.3, 24.8, '2022-07-14 13:30:00'], [25.67, 25.15, '2022-07-13 14:10:00'], [29.22, 28.71, '2022-07-08 13:30:00'], [29.34, 28.835, '2022-07-07 13:55:00'], [26.94, 26.43, '2022-07-06 13:45:00']]
        #     },
        #     {
        #         'Symbol': 'MRO',
        #         'Score': 5,
        #         'Limit': 6,
        #         'Budget': 203.0,
        #         'Client Id': 'GYNQGLGLPINXJNWLKFZM48NQ1C86KS8D',
        #         'Account Id': '277420951',
        #         'Sentiment': 'Positive',
        #         'Key Levels': [27.915, 27.26, 26.38, 25.59, 24.53, 23.51, 22.5, 21.68, 21.0, 33.235, 27.44, 26.47, 25.47, 24.49, 23.48, 22.51, 21.51, 20.59, 19.56, 18.46, 3.73],
        #         'Support Resistance': {
        #             'resistance': 23.51,
        #             'support': 22.5
        #         },
        #         'Demand Zones': [(26.54, 27.14, '2022-09-15 13:30:00'), (25.605, 26.41, '2022-09-06 13:30:00'), (25.67, 26.33, '2022-08-24 13:30:00'), (24.58, 25.75, '2022-08-23 13:30:00'), (21.78, 22.74, '2022-08-09 13:30:00'), (21.0, 21.5, '2022-08-05 13:30:00'), (23.925, 24.95, '2022-07-29 13:30:00'), (20.72, 21.89, '2022-07-18 13:30:00'), (19.92, 20.46, '2022-07-14 17:55:00'), (20.42, 20.98, '2022-07-13 13:30:00'), (20.41, 21.01, '2022-07-12 13:30:00'), (20.845, 21.505, '2022-07-05 19:45:00'), (22.275, 22.83, '2022-06-30 19:55:00'), (22.08, 22.63, '2022-06-30 13:30:00'), (23.58, 24.52, '2022-06-28 13:50:00')],
        #         'Supply Zones': [[22.26, 21.79, '2022-09-23 15:05:00'], [23.35, 22.85, '2022-09-23 13:30:00'], [26.77, 26.18, '2022-09-16 13:30:00'], [23.53, 23.0, '2022-08-17 14:45:00'], [23.09, 22.48, '2022-08-15 13:30:00'], [21.99, 21.43, '2022-07-05 13:30:00'], [24.13, 23.96, '2022-06-29 13:30:00'], [23.19, 23.52, '2022-06-28 13:35:00'], [22.02, 21.84, '2022-06-24 13:30:00'], [24.07, 23.57, '2022-06-17 14:40:00'], [24.8, 24.3, '2022-06-17 14:15:00'], [25.53, 24.89, '2022-06-17 13:30:00'], [29.25, 28.695, '2022-06-13 13:30:00'], [31.53, 31.03, '2022-05-31 19:35:00'], [32.83, 32.33, '2022-05-31 13:55:00']]
        #     },
        #     {
        #         'Symbol': 'MU',
        #         'Score': 5,
        #         'Limit': 6,
        #         'Budget': 203.0,
        #         'Client Id': 'GYNQGLGLPINXJNWLKFZM48NQ1C86KS8D',
        #         'Account Id': '277420951',
        #         'Sentiment': 'Positive',
        #         'Key Levels': [65.415, 54.63, 53.45, 52.46, 51.38, 50.48, 49.61, 48.87, 48.45, 98.45, 54.47, 53.48, 52.51, 51.67, 50.44, 49.62, 48.67, 47.41, 46.81, 46.5],
        #         'Support Resistance': {
        #             'resistance': 50.48,
        #             'support': 49.61
        #         },
        #         'Demand Zones': [(51.2621, 52.19, '2022-09-30 14:50:00'), (50.05, 51.1031, '2022-09-30 14:05:00'), (49.96, 50.67, '2022-09-30 13:40:00'), (50.19, 50.75, '2022-09-27 19:50:00'), (48.84, 50.32, '2022-09-27 13:30:00'), (50.615, 51.42, '2022-09-21 18:30:00'), (50.745, 51.68, '2022-09-21 14:00:00'), (49.71, 50.24, '2022-09-21 13:30:00'), (52.25, 52.89, '2022-09-16 19:50:00'), (52.58, 53.17, '2022-09-14 19:50:00'), (55.28, 56.2, '2022-09-13 13:30:00'), (55.87, 56.5, '2022-08-30 19:55:00'), (61.9, 62.71, '2022-08-05 13:35:00'), (61.28, 61.78, '2022-07-22 13:30:00'), (62.6, 63.1, '2022-07-21 13:30:00')],
        #         'Supply Zones': [[50.13, 49.54, '2022-09-29 13:30:00'], [49.37, 48.87, '2022-09-23 13:30:00'], [50.37, 49.83, '2022-09-21 19:55:00'], [54.51, 54.01, '2022-09-08 13:30:00'], [57.11, 56.98, '2022-08-30 13:30:00'], [60.41, 59.7899, '2022-08-09 13:35:00'], [52.83, 52.2301, '2022-07-01 14:10:00'], [53.79, 53.0, '2022-07-01 13:40:00'], [57.52, 56.74, '2022-06-29 13:30:00'], [59.04, 58.5169, '2022-06-15 18:00:00'], [64.45, 63.94, '2022-06-10 14:00:00'], [71.69, 71.18, '2022-06-03 13:30:00'], [69.54, 70.1, '2022-05-20 13:30:00'], [69.25, 68.77, '2022-04-25 13:35:00'], [72.75, 75.455, '2022-04-20 13:35:00']]
        #     }]

        # sort temporary opportunities list by score
        sorted_temp = sorted(temp_opportunity_list,
                             key=lambda i: i['Score'], reverse=True)

        print('opportunities', sorted_temp)

        # # establish a limit_count
        # limit_count = 0

        # # loop through the sorted_temp
        # while limit_count < self.limit:
        #     # create and send a find position event for the current company
        #     self.send_find_position_event(sorted_temp[limit_count])

        #     # increase limit_count by 1
        #     limit_count += 1

        return sorted_temp

    def premarket_data_to_dict(self, premarket_data, ctx):
        """
            Returns a dict representation of a premarket_data item for serialization.
            Args:
                premarket_data (start_pre_market_analysis): an item from the pre-market data return from start_pre_market_analysis.
                ctx (SerializationContext): Metadata pertaining to the serialization
                    operation.
            Returns:
                dict: Dict populated with premarket_data attributes to be serialized.
        """
        return dict(
            limit=premarket_data['Limit'],
            budget=premarket_data['Budget'],
            client_id=premarket_data['Client Id'],
            account_id=premarket_data['Account Id'],
            symbol=premarket_data['Symbol'],
            score=premarket_data['Score'],
            sentiment=premarket_data['Sentiment'],
            keyLevels=premarket_data['Key Levels'],
            supportResistance=premarket_data['Support Resistance'],
            demandZones=premarket_data['Demand Zones'],
            supplyZones=premarket_data['Supply Zones']
        )

    def delivery_report(self, err, msg):
        """
            Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush().
        """
        if err is not None:
            print('Message delivery failed: {}'.format(msg.key(), err))
        else:
            print('Message delivered to {} [{}]'.format(
                msg.key(), msg.topic(), msg.partition()))

    def send_find_position_event(self, premarket_data):
        topic = 'find-position'

        # establish premarket schema and value schema
        premarket_schema_str = """
        {
            "namspace": "tickr",
            "name": "premarket",
            "type": "record",
            "fields": [
                {
                    "name": "limit",
                    "type": "int"
                },
                {
                    "name": "budget",
                    "type": "int"
                },
                {
                    "name": "client_id",
                    "type": "string"
                },
                {
                    "name": "account_id",
                    "type": "string"
                },
                {
                    "name": "symbol",
                    "type": "string"
                },
                {
                    "name": "score",
                    "type": "int"
                },
                {
                    "name": "sentiment",
                    "type": "string"
                },
                {
                    "name": "keyLevels",
                    "type": {
                        "type": "array",
                        "items": {
                            "type": "double"
                        }
                    },
                    "default": []
                },
                {
                    "name": "supportResistance",
                    "type": {
                        "type": "map",
                        "values": {
                            "type": "double"
                        }
                    },
                    "default": {}
                },
                {
                    "name": "demandZones",
                    "type": {
                        "type": "array",
                        "items": {
                            "type": "array",
                            "items": [
                                "double",
                                "string"
                            ]
                        }
                    },
                    "default": []
                },
                {
                    "name": "supplyZones",
                    "type": {
                        "type": "array",
                        "items": {
                            "type": "array",
                            "items": [
                                "double",
                                "string"
                            ]
                        }
                    },
                    "default": []
                }
            ]
        }
        """

        schema_registry_config = {'url': 'http://167.172.137.136:8081'}
        schema_registry_client = SchemaRegistryClient(schema_registry_config)
        avro_serializer = AvroSerializer(schema_registry_client=schema_registry_client,
                                         schema_str=premarket_schema_str, to_dict=self.premarket_data_to_dict)

        # establish the producer config
        producer_config = {
            'bootstrap.servers': self.BOOTSTRAP_SERVER,
            'key.serializer': StringSerializer(codec='utf_8'),
            'value.serializer': avro_serializer
        }

        producer = SerializingProducer(conf=producer_config)
        producer.poll(0.0)
        producer.produce(topic=topic, key=str(uuid4()),
                         value=premarket_data, on_delivery=self.delivery_report)

        return producer.flush()

    def send_account_low_event(self, message='Not Enough Capital'):
        topic = 'available-account-low'

        # establish account_low schema and value schema
        account_low_schema_str = """
        {
            "namspace": "tickr",
            "name": "account-low",
            "type": "string"
        }
        """

        schema_registry_config = {'url': self.SCHEMA_REGISTRY_URL}
        schema_registry_client = SchemaRegistryClient(schema_registry_config)
        avro_serializer = AvroSerializer(
            schema_registry_client=schema_registry_client, schema_str=account_low_schema_str)

        # establish the producer config
        producer_config = {
            'bootstrap.servers': self.BOOTSTRAP_SERVER,
            'key.serializer': StringSerializer(codec='utf_8'),
            'value.serializer': avro_serializer
        }

        producer = SerializingProducer(conf=producer_config)
        producer.poll(0.0)
        producer.produce(topic=topic, key=str(uuid4()),
                         value=message, on_delivery=self.delivery_report)

        return producer.flush()


if __name__ == '__main__':
    pre = Pre_Market()
    pre.start_pre_market_analysis()
