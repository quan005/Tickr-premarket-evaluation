import pandas as pd
import os

from configparser import ConfigParser

from write_config import Write_Config
from py_trader import PyTrader
from datetime import datetime, time, timezone, timedelta, date


class HistoricalAnalysis:
    def __init__(self, ticker=None):

        config = ConfigParser()
        config.read('configs.ini')
        self.ticker = ticker.upper()
        self.date = date.today().weekday()
        self.CLIENT_ID = config.get('main', 'CLIENT_ID')
        self.REDIRECT_URI = config.get('main', 'REDIRECT_URI')
        self.ACCOUNT_ID = config.get('main', 'ACCOUNT_ID')
        self.start_date = datetime.today() - timedelta(days=2920)
        self.end_date = datetime.today()

    def get_daily_historical_data(self):

        # write config file with credential path
        Write_Config()

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

        # create a portfolio
        bot_portfolio = bot.create_portfolio()

        # add the position the the current company
        bot_portfolio.add_position(
            symbol=self.ticker,
            asset_type='equity'
        )

        # grab historical prices using the daily interval
        historical_prices = bot.grab_single_historical_prices(
            symbol=self.ticker,
            start=self.start_date,
            end=self.end_date,
            period_type='month',
            bar_size=1,
            bar_type='daily'
        )

        # convert historical prices to a pandas dataframe using the stock frame
        stock_frame = bot.create_stock_frame(
            data=historical_prices['aggregated']
        )
        # print('stock frame', stock_frame.frame)

        # remove the current ticker from the position
        bot_portfolio.remove_position(
            symbol=self.ticker
        )

        # return data as a pandas Dataframe
        return stock_frame.frame

    def start_historical_analysis(self):
        dataframe = self.get_daily_historical_data()
        row_index = dataframe.index
        date_list = list(row_index)
        analysis = 0
        green = 0
        red = 0

        for i in range(len(dataframe)):
            high = dataframe.iloc[i, 0] + 2
            day = date.strftime(date_list[i][1], '%Y-%m-%d')
            dayday = date.fromisoformat(day).weekday()
            if date.fromisoformat(day).weekday() == self.date:
                if dataframe.iloc[i, 1] > dataframe.iloc[i, 0]:
                    green += 1
                elif dataframe.iloc[i, 1] < dataframe.iloc[i, 0] and dataframe.iloc[i, 2] >= high:
                    green += 1
                elif dataframe.iloc[i, 1] < dataframe.iloc[i, 0]:
                    red += 1

                analysis += 1
            else:
                pass

        # print('dataframe length', len(dataframe))
        # print('analysis', analysis)
        # print('green', green)
        # print('red', red)

        good = .55 * analysis
        great = .65 * analysis
        perfect = .75 * analysis

        if green >= good and green < great:
            return 1
        elif green >= great and green < perfect:
            return 2
        elif green >= perfect:
            return 3
        else:
            return 0
