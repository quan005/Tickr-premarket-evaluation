import os
import json
from uuid import uuid4
from typing import OrderedDict
from datetime import datetime, time, timezone, timedelta, date
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
        self.small_watchlist = ['AMD', 'NKE', 'AAL',
                                'SPY', 'PFE', 'BAC', 'CGC', 'AAPL']
        self.medium_watchlist = ['AMD', 'NKE', 'AAL', 'SPY', 'PFE', 'BAC', 'CGC', 'AAPL',
                                 'MSFT', 'FB', 'CRM', 'BABA', 'PYPL', 'PTON', 'DKNG', 'WMT', 'JPM', 'DIS']
        self.large_watchlist = ['AMD', 'NKE', 'AAL', 'SPY', 'PFE', 'BAC', 'CGC', 'AAPL', 'MSFT', 'FB', 'CRM', 'BABA',
                                'PYPL', 'PTON', 'DKNG', 'WMT', 'JPM', 'DIS', 'ROKU', 'SQ', 'NFLX', 'TSLA', 'AMZN', 'SHOP', 'ZM', 'NVDA', 'LCID']
        self.xtra_large_watchlist = ['AMD', 'NKE', 'AAL', 'SPY', 'PFE', 'BAC', 'CGC', 'AAPL', 'MSFT', 'FB', 'CRM', 'BABA', 'PYPL', 'PTON', 'DKNG',
                                     'WMT', 'JPM', 'DIS', 'ROKU', 'SQ', 'NFLX', 'TSLA', 'AMZN', 'SHOP', 'ZM', 'NVDA', 'LCID', 'GOOG', 'FL', 'EBAY', 'CMG', 'BA', 'MU', 'BYND', 'DOCU']
        self.limit = 0
        self.opportunities = []

    def start_pre_market_analysis(self):

        # initiate and get token
        getToken = TokenInitiator()
        userPrinciples = getToken.get_access_token()

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
            new_opportunity['Token'] = self.TOKEN
            new_opportunity['User Principles'] = userPrinciples
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

            # five minute end date
            five_minute_end_date = start_date - timedelta(weeks=75)

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

        # sort temporary opportunities list by score
        sorted_temp = sorted(temp_opportunity_list,
                             key=lambda i: i['Score'], reverse=True)

        # establish a limit_count
        limit_count = 0

        # loop through the sorted_temp
        while limit_count < self.limit:
            # create and send a find position event for the current company
            self.send_find_position_event(sorted_temp[limit_count])

            # increase limit_count by 1
            limit_count += 1

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
            token=premarket_data['Token'],
            symbol=premarket_data['Symbol'],
            score=premarket_data['Score'],
            sentiment=premarket_data['Sentiment'],
            keyLevels=premarket_data['Key Levels'],
            supportResistance=premarket_data['Support Resistance'],
            demandZones=premarket_data['Demand Zones'],
            supplyZones=premarket_data['Supply Zones'],
            userPrinciples=premarket_data['User Principles']
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
                    "name": "token",
                    "type": {
                        "type": "record",
                        "name": "tokenRecord",
                        "fields": [
                            {
                                "name": "access_token",
                                "type": "string"
                            },
                            {
                                "name": "refresh_token",
                                "type": "string"
                            },
                            {
                                "name": "scope",
                                "type": "string"
                            },
                            {
                                "name": "expires_in",
                                "type": "int"
                            },
                            {
                                "name": "refresh_token_expires_in",
                                "type": "int"
                            },
                            {
                                "name": "token_type",
                                "type": "string"
                            },
                            {
                                "name": "access_token_expires_at",
                                "type": "double"
                            },
                            {
                                "name": "refresh_token_expires_at",
                                "type": "double"
                            },
                            {
                                "name": "logged_in",
                                "type": "boolean"
                            },
                            {
                                "name": "access_token_expires_at_date",
                                "type": "string"
                            },
                            {
                                "name": "refresh_token_expires_at_date",
                                "type": "string"
                            }
                        ]
                    }
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
                },
                {
                    "name": "userPrinciples",
                    "type": {
                        "name": "userPrinciplesRecord",
                        "type": "record",
                        "fields": [
                            {
                                "name": "userId",
                                "type": "string"
                            },
                            {
                                "name": "userCdDomainId",
                                "type": "string"
                            },
                            {
                                "name": "primaryAccountId",
                                "type": "string"
                            },
                            {
                                "name": "lastLoginTime",
                                "type": "string"
                            },
                            {
                                "name": "tokenExpirationTime",
                                "type": "string"
                            },
                            {
                                "name": "loginTime",
                                "type": "string"
                            },
                            {
                                "name": "accessLevel",
                                "type": "string"
                            },
                            {
                                "name": "stalePassword",
                                "type": "boolean"
                            },
                            {
                                "name": "streamerInfo",
                                "type": {
                                    "type": "map",
                                    "values": {
                                        "type": "string"
                                    }
                                }
                            },
                            {
                                "name": "professionalStatus",
                                "type": "string"
                            },
                            {
                                "name": "quotes",
                                "type": {
                                    "type": "map",
                                    "values": {
                                        "type": "boolean"
                                    }
                                }
                            },
                            {
                                "name": "streamerSubscriptionKeys",
                                "type": {
                                    "type": "map",
                                    "values": {
                                        "type": "array",
                                        "items": {
                                            "type": "map",
                                            "values": {
                                                "type": "string"
                                            }
                                        }
                                    }
                                }
                            },
                            {
                                "name": "exchangeAgreements",
                                "type": {
                                    "type": "map",
                                    "values": {
                                        "type": "string"
                                    }
                                }
                            },
                            {
                                "name": "accounts",
                                "type": {
                                    "type": "array",
                                    "items": {
                                        "name": "accountItems",
                                        "type": "record",
                                        "fields": [
                                            {
                                                "name": "accountId",
                                                "type": "string"
                                            },
                                            {
                                                "name": "displayName",
                                                "type": "string"
                                            },
                                            {
                                                "name": "accountCdDomainId",
                                                "type": "string"
                                            },
                                            {
                                                "name": "company",
                                                "type": "string"
                                            },
                                            {
                                                "name": "segment",
                                                "type": "string"
                                            },
                                            {
                                                "name": "acl",
                                                "type": "string"
                                            },
                                            {
                                                "name": "authorizations",
                                                "type": {
                                                    "type": "map",
                                                    "values": [
                                                        "boolean",
                                                        "string"
                                                    ]
                                                }
                                            }
                                        ]
                                    }
                                }
                            }
                        ]
                    }
                }
            ]
        }
        """

        schema_registry_config = {'url': 'http://137.184.135.78:8081'}
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
