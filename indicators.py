import operator
import talib
import math
import bisect
import pandas as pd
import numpy as np
from statistics import mean
from datetime import time, datetime, timezone, timedelta
from typing import Any, List, Dict, Union
from collections import defaultdict
from statistics import mean
from stock_frame import StockFrame


class Indicators():

    """
    Represents an Indicator Object which can be used
    to easily add technical indicators to a StockFrame.
    """

    def __init__(self, price_data_frame: StockFrame) -> None:
        """Initalizes the Indicator Client.
        Arguments:
        ----
        price_data_frame {pyrobot.StockFrame} -- The price data frame which is used to add indicators to.
            At a minimum this data frame must have the following columns: `['timestamp','close','open','high','low']`.

        Usage:
        ----
            >>> historical_prices_df = trading_robot.grab_historical_prices(
                start=start_date,
                end=end_date,
                bar_size=1,
                bar_type='minute'
            )
            >>> price_data_frame = pd.DataFrame(data=historical_prices)
            >>> indicator_client = Indicators(price_data_frame=price_data_frame)
            >>> indicator_client.price_data_frame
        """

        self._stock_frame: StockFrame = price_data_frame
        self._price_groups = price_data_frame.symbol_groups
        self._current_indicators = {}
        self.support = []
        self.resistance = []
        self.levels = []
        self.support_resistance = {
            'support': [],
            'resistance': []
        }
        self._indicator_signals = {}
        self._frame = self._stock_frame.frame

        self._indicators_comp_key = []
        self._indicators_key = []

        if self.is_multi_index:
            True

    @property
    def price_data_frame(self) -> pd.DataFrame:
        """Return the raw Pandas Dataframe Object.
        Returns:
        ----
        {pd.DataFrame} -- A multi-index data frame.
        """

        return self._frame

    @price_data_frame.setter
    def price_data_frame(self, price_data_frame: pd.DataFrame) -> None:
        """Sets the price data frame.
        Arguments:
        ----
        price_data_frame {pd.DataFrame} -- A multi-index data frame.
        """

        self._frame = price_data_frame

    @property
    def is_multi_index(self) -> bool:
        """Specifies whether the data frame is a multi-index dataframe.
        Returns:
        ----
        {bool} -- `True` if the data frame is a `pd.MultiIndex` object. `False` otherwise.
        """

        if isinstance(self._frame.index, pd.MultiIndex):
            return True
        else:
            return False

    def find_surrounding_levels(self, list: list, value: int):

        left = 0
        right = len(list) - 1

        while left <= right:
            middle = (left + right) // 2

            if list[middle - 1] >= value and list[middle] <= value:
                return (middle - 1, middle)

            if list[left] == list[right]:
                return (middle, middle)

            if list[middle] < value:
                right = middle - 1
            elif list[middle] > value:
                left = middle + 1

        return (None, None)

    def s_r_levels(self, dataframe: pd.DataFrame, price_dic: dict):

        highPrice = dataframe['high']
        lowPrice = dataframe['low']
        openPrice = dataframe['open']
        closePrice = dataframe['close']

        high = highPrice.max()
        low = lowPrice.min(skipna=True)

        all_prices = np.concatenate([highPrice.values, lowPrice.values, openPrice.values, closePrice.values])
        unique_prices, price_counts = np.unique(all_prices, return_counts=True)

        for price, count in zip(unique_prices, price_counts):
            rounded_price = round(price, 0)
            key = f'{rounded_price}'

            if key not in price_dic:
                price_dic[key] = {
                    'ranges': defaultdict(lambda: {'count': 0, 'mean': []}),
                    'occurrences': count
                }
            
            for i in range(1, 8):
                diff_below = round(price - (price * 0.0055 * i), 2)
                diff_above = round((price * 0.0055 * i) + price, 2)
                range_key = f'{diff_below}_{diff_above}'
                price_dic[key]['ranges'][range_key]['count'] += count
                price_dic[key]['ranges'][range_key]['mean'].extend([price] * count)

        ranges = []
        common_prices = []
        for key, value in price_dic.items():
            if value['occurrences'] == len(dataframe):
                common_prices.append(float(key))
                for mean_key, mean_value in value['ranges'].items():
                    if mean_value['count'] >= 5:
                        ranges.append(mean_value['mean'])

        key_levels = [high, low] + common_prices + [np.mean(price_range) for price_range in ranges]
        key_levels = list(filter(lambda x: x != 0.0, key_levels))
        key_levels.sort(reverse=True)

        new_dict = {
            'price_dic': price_dic,
            'key_levels': key_levels
        }

        return new_dict

    def scrub_key_levels(self, key_levels: list):
        clean_key_levels = []
        if not key_levels:
            return clean_key_levels

        range_threshold = 0.0055

        for i, first_pointer in enumerate(key_levels):
            first_pointer_above = first_pointer * (1 + range_threshold)
            first_pointer_below = first_pointer * (1 - range_threshold)

            temp_list = [first_pointer] + [val for val in key_levels[i + 1:] if first_pointer_below <= val <= first_pointer_above]

            if temp_list:
                sequence_mean = round(mean(temp_list), 2)
                clean_key_levels.append(sequence_mean)

        return clean_key_levels

    def get_supply_demand_zones(self, dataframe: pd.DataFrame, key_levels: list, price_threshold: float):
        demand_zones = []
        supply_zones = []
        broke_key_level = []

        high_prices = dataframe['high'].values
        low_prices = dataframe['low'].values
        close_prices = dataframe['close'].values
        date_and_time = dataframe.index

        min_key_level = np.min(key_levels)
        max_key_level = np.max(key_levels)

        # Iterate over the price data
        for i in range(1, len(close_prices)):
            current_price = close_prices[i]
            previous_price = close_prices[i - 1]
            is_increase = current_price > previous_price

            # Calculate the price change and check if it exceeds the threshold
            price_change = current_price - previous_price
            abs_price_change = abs(price_change)

            if abs_price_change >= price_threshold:
                # Check if the price crossed a key level
                if is_increase and previous_price <= min_key_level <= current_price <= max_key_level:
                    demand_zones.append({
                        "bottom": previous_price,
                        "top": current_price,
                        "dateTime": str(date_and_time[i - 1])
                    })
                    broke_key_level.append((previous_price, current_price, 'demand_zone'))
                elif not is_increase and max_key_level >= previous_price >= min_key_level >= current_price:
                    supply_zones.append({
                        "top": previous_price,
                        "bottom": current_price,
                        "dateTime": str(date_and_time[i - 1])
                    })
                    broke_key_level.append((previous_price, current_price, 'supply_zone'))

        final_demand_supply_zones = {
            'demand_zones': demand_zones,
            'supply_zones': supply_zones,
            'broke_key_level': broke_key_level
        }

        print('final_demand_supply_zones', final_demand_supply_zones)

        return final_demand_supply_zones

    def candle_pattern_check(self, dataframe):

        patterns = [
            'CDLHAMMER',
            'CDLINVERTEDHAMMER',
            'CDLENGULFING',
            'CDLMORNINGSTAR',
            'CDL3WHITESOLDIERS',
            'CDLPIERCING',
            'CDLCOUNTERATTACK',
            'CDLHANGINGMAN',
            'CDLHARAMI',
            'CDLHARAMICROSS',
            'CDLSHOOTINGSTAR',
            'CDLEVENINGSTAR',
            'CDL3BLACKCROWS',
            'CDLMORNINGDOJISTAR',
            'CDLDRAGONFLYDOJI',
            'CDLABANDONEDBABY',
            'CDLBREAKAWAY',
            'CDLDARKCLOUDCOVER',
            'CDLSPINNINGTOP',
            'CDLKICKING',
            'CDLMATHOLD',
            'CDLRISEFALL3METHODS',
            'CDLGAPSIDESIDEWHITE',
            'CDL3OUTSIDE',
            'CDL3INSIDE',
            'CDL3STARSINSOUTH',
            'CDL3LINESTRIKE',
        ]

        for pattern in patterns:
            pattern_function = getattr(talib, pattern)
            check_for_pattern = pattern_function(
                dataframe['open'], dataframe['high'], dataframe['low'], dataframe['close'])
            last = sum(check_for_pattern.values)

            if last > 0:
                return 'BULLISH'
            elif last < 0:
                return 'BEARISH'

        return 'NO OPPURTUNITY'

    def get_support_resistance(self, sr_list: list, close_price_series):
        # Find which support/resistance lines
        # are surrounding the current price.
        if len(sr_list) > 1:
            for i in range(len(sr_list)):
                if i >= len(sr_list) - 1:
                    if close_price_series[-2] <= sr_list[i]:
                        return {
                            'resistance': sr_list[i],
                            'support': 0,
                        }
                    elif close_price_series[-2] >= sr_list[i]:
                        return {
                            'resistance': sr_list[i - 1],
                            'support': sr_list[i],
                        }
                    else:
                        return 'Theres is no Support / Resistance levels'

                elif close_price_series[-2] <= sr_list[i] and close_price_series[-2] >= sr_list[i + 1]:
                    return {
                        'resistance': sr_list[i],
                        'support': sr_list[i + 1],
                    }
                else:
                    pass

        return 'Theres is no Support / Resistance levels'

    def isPriceSideways(self, close_price_series: pd.Series, start: int, limit: int):

        within_range = 0

        for i in range(start, limit):
            j = i - 3
            diff_above = round(
                (close_price_series[i] * 0.001) + close_price_series[i], 2)
            diff_below = round(
                close_price_series[i] - (close_price_series[i] * 0.001), 2)

            if close_price_series[j] < diff_above and close_price_series[j] > diff_below:
                within_range += 1
            else:
                pass

        diff_percent = within_range / limit

        if diff_percent >= .4:
            return True
        else:
            return False

    def ema_crossover(self, price_data: dict):

        if price_data['current price']['ema2'] == price_data['current price']['ema4'] and price_data['previous price']['ema2'] < price_data['previous price']['ema4']:
            print('CALL')
            return 'CALL'
        elif price_data['current price']['ema2'] > price_data['current price']['ema4'] and price_data['previous price']['ema2'] <= price_data['previous price']['ema4']:
            print('CALL')
            return 'Call'
        elif price_data['current price']['ema2'] == price_data['current price']['ema4'] and price_data['previous price']['ema2'] > price_data['previous price']['ema4']:
            print('PUT')
            return 'PUT'
        elif price_data['current price']['ema2'] < price_data['current price']['ema4'] and price_data['previous price']['ema2'] <= price_data['previous price']['ema4']:
            print('PUT')
            return 'PUT'
        else:
            print('no good buys')
            return 'NON'

    def ema200(self, price_data: dict):

        if price_data['current price']['close'] > price_data['current price']['ema200']:
            return 'ABOVE'
        else:
            return 'BELOW'

    def ema(self, period: int, alpha: float = 0.0) -> pd.DataFrame:
        """Calculates the Exponential Moving Average (EMA).
        Arguments:
        ----
        period {int} -- The number of periods to use when calculating the EMA.
        alpha {float} -- The alpha weight used in the calculation. (default: {0.0})
        Returns:
        ----
        {pd.DataFrame} -- A Pandas data frame with the EMA indicator included.
        Usage:
        ----
            >>> historical_prices_df = trading_robot.grab_historical_prices(
                start=start_date,
                end=end_date,
                bar_size=1,
                bar_type='minute'
            )
            >>> price_data_frame = pd.DataFrame(data=historical_prices)
            >>> indicator_client = Indicators(price_data_frame=price_data_frame)
            >>> indicator_client.ema(period=50, alpha=1/50)
        """

        locals_data = locals()
        del locals_data['self']

        column_name = 'ema'
        self._current_indicators[column_name] = {}
        self._current_indicators[column_name]['args'] = locals_data
        self._current_indicators[column_name]['func'] = self.ema

        # Add the EMA
        self._frame[column_name] = self._price_groups['close'].transform(
            lambda x: x.ewm(span=period).mean()
        )

        return self._frame

    def rate_of_change(self, period: int = 1) -> pd.DataFrame:
        """Calculates the Rate of Change (ROC).
        Arguments:
        ----
        period {int} -- The number of periods to use when calculating 
            the ROC. (default: {1})
        Returns:
        ----
        {pd.DataFrame} -- A Pandas data frame with the ROC indicator included.
        Usage:
        ----
            >>> historical_prices_df = trading_robot.grab_historical_prices(
                start=start_date,
                end=end_date,
                bar_size=1,
                bar_type='minute'
            )
            >>> price_data_frame = pd.DataFrame(data=historical_prices)
            >>> indicator_client = Indicators(price_data_frame=price_data_frame)
            >>> indicator_client.rate_of_change()
        """
        locals_data = locals()
        del locals_data['self']

        column_name = 'rate_of_change'
        self._current_indicators[column_name] = {}
        self._current_indicators[column_name]['args'] = locals_data
        self._current_indicators[column_name]['func'] = self.rate_of_change

        # Add the Momentum indicator.
        self._frame[column_name] = self._price_groups['close'].transform(
            lambda x: x.pct_change(periods=period)
        )

        return self._frame

    def macd(self, fast_period: int = 4, slow_period: int = 9) -> pd.DataFrame:
        """Calculates the Moving Average Convergence Divergence (MACD).
        Arguments:
        ----
        fast_period {int} -- The number of periods to use when calculating 
            the fast moving MACD. (default: {12})
        slow_period {int} -- The number of periods to use when calculating 
            the slow moving MACD. (default: {26})
        Returns:
        ----
        {pd.DataFrame} -- A Pandas data frame with the MACD included.
        Usage:
        ----
            >>> historical_prices_df = trading_robot.grab_historical_prices(
                start=start_date,
                end=end_date,
                bar_size=1,
                bar_type='minute'
            )
            >>> price_data_frame = pd.DataFrame(data=historical_prices)
            >>> indicator_client = Indicators(price_data_frame=price_data_frame)
            >>> indicator_client.macd(fast_period=12, slow_period=26)
        """

        locals_data = locals()
        del locals_data['self']

        column_name = 'macd'
        self._current_indicators[column_name] = {}
        self._current_indicators[column_name]['args'] = locals_data
        self._current_indicators[column_name]['func'] = self.macd

        # Calculate the Fast Moving MACD.
        self._frame['macd_fast'] = self._frame['close'].transform(
            lambda x: x.ewm(span=fast_period, min_periods=fast_period).mean()
        )

        # Calculate the Slow Moving MACD.
        self._frame['macd_slow'] = self._frame['close'].transform(
            lambda x: x.ewm(span=slow_period, min_periods=slow_period).mean()
        )

        # Calculate the difference between the fast and the slow.
        self._frame['macd'] = self._frame['macd_fast'] - \
            self._frame['macd_slow']

        # Calculate the Exponential moving average of the macd.
        self._frame['macd_signal'] = self._frame['macd'].transform(
            lambda x: x.ewm(span=3, min_periods=3).mean()
        )

        return self._frame

    def vwap(self) -> pd.DataFrame:
        """Calculates the VWAP.
        Arguments:
        ----
        period {int} -- The number of periods to use when calculating 
            the VWAP.
        Returns:
        ----
        {pd.DataFrame} -- A Pandas data frame with the VWAP included.
        Usage:
        ----
            >>> historical_prices_df = trading_robot.grab_historical_prices(
                start=start_date,
                end=end_date,
                bar_size=1,
                bar_type='minute'
            )
            >>> price_data_frame = pd.DataFrame(data=historical_prices)
            >>> indicator_client = Indicators(price_data_frame=price_data_frame)
            >>> indicator_client.VWAP(period=9)
        """

        locals_data = locals()
        del locals_data['self']

        column_name = 'vwap'
        self._current_indicators[column_name] = {}
        self._current_indicators[column_name]['args'] = locals_data
        self._current_indicators[column_name]['func'] = self.vwap

        # Calculate the Typical Price.
        self._frame['typical_price'] = (
            self._frame['high'] + self._frame['low'] + self._frame['close']) / 3

        # Multiply the Typical Price by the period Volume.
        self._frame['new_typical_price'] = self._frame['typical_price'] * \
            self._frame['volume']

        # Create a Cumulative Total of Typical Price
        self._frame['cumulative_total_typical_price'] = self._frame['new_typical_price'] + \
            self._frame['new_typical_price'].cumsum()

        # Create a Cumulative Total of Volume
        self._frame['cumulative_total_volume'] = self._frame['volume'] + \
            self._frame['volume'].cumsum()

        # Calculate the VWAP
        self._frame['vwap'] = self._frame['cumulative_total_typical_price'] / \
            self._frame['cumulative_total_volume']

        # Clean up before sending back.
        self._frame.drop(
            labels=['typical_price', 'new_typical_price',
                    'cumulative_total_typical_price', 'cumulative_total_volume'],
            axis=1,
            inplace=True
        )

        return self._frame

    def standard_deviation(self, period: int) -> pd.DataFrame:
        """Calculates the Standard Deviation.
        Arguments:
        ----
        period {int} -- The number of periods to use when calculating 
            the standard deviation.
        Returns:
        ----
        {pd.DataFrame} -- A Pandas data frame with the Standard Deviation included.
        Usage:
        ----
            >>> historical_prices_df = trading_robot.grab_historical_prices(
                start=start_date,
                end=end_date,
                bar_size=1,
                bar_type='minute'
            )
            >>> price_data_frame = pd.DataFrame(data=historical_prices)
            >>> indicator_client = Indicators(price_data_frame=price_data_frame)
            >>> indicator_client.standard_deviation(period=9)
        """

        locals_data = locals()
        del locals_data['self']

        column_name = 'standard_deviation'
        self._current_indicators[column_name] = {}
        self._current_indicators[column_name]['args'] = locals_data
        self._current_indicators[column_name]['func'] = self.standard_deviation

        # Calculate the Standard Deviation.
        self._frame[column_name] = self._frame['close'].transform(
            lambda x: x.ewm(span=period).std()
        )

        return self._frame

    def chaikin_oscillator(self, period: int) -> pd.DataFrame:
        """Calculates the Chaikin Oscillator.
        Arguments:
        ----
        period {int} -- The number of periods to use when calculating 
            the Chaikin Oscillator.
        Returns:
        ----
        {pd.DataFrame} -- A Pandas data frame with the Chaikin Oscillator included.
        Usage:
        ----
            >>> historical_prices_df = trading_robot.grab_historical_prices(
                start=start_date,
                end=end_date,
                bar_size=1,
                bar_type='minute'
            )
            >>> price_data_frame = pd.DataFrame(data=historical_prices)
            >>> indicator_client = Indicators(price_data_frame=price_data_frame)
            >>> indicator_client.chaikin_oscillator(period=9)
        """

        locals_data = locals()
        del locals_data['self']

        column_name = 'chaikin_oscillator'
        self._current_indicators[column_name] = {}
        self._current_indicators[column_name]['args'] = locals_data
        self._current_indicators[column_name]['func'] = self.chaikin_oscillator

        # Calculate the Money Flow Multiplier.
        money_flow_multiplier_top = 2 * \
            (self._frame['close'] - self._frame['high'] - self._frame['low'])
        money_flow_multiplier_bot = (self._frame['high'] - self._frame['low'])

        # Calculate Money Flow Volume
        self._frame['money_flow_volume'] = (
            money_flow_multiplier_top / money_flow_multiplier_bot) * self._frame['volume']

        # Calculate the 3-Day moving average of the Money Flow Volume.
        self._frame['money_flow_volume_3'] = self._frame['money_flow_volume'].transform(
            lambda x: x.ewm(span=3, min_periods=2).mean()
        )

        # Calculate the 10-Day moving average of the Money Flow Volume.
        self._frame['money_flow_volume_10'] = self._frame['money_flow_volume'].transform(
            lambda x: x.ewm(span=10, min_periods=9).mean()
        )

        # Calculate the Chaikin Oscillator.
        self._frame[column_name] = self._frame['money_flow_volume_3'] - \
            self._frame['money_flow_volume_10']

        # Clean up before sending back.
        self._frame.drop(
            labels=['money_flow_volume_3',
                    'money_flow_volume_10', 'money_flow_volume'],
            axis=1,
            inplace=True
        )

        return self._frame

    def kst_oscillator(self, r1: int, r2: int, r3: int, r4: int, n1: int, n2: int, n3: int, n4: int) -> pd.DataFrame:
        """Calculates the Mass Index indicator.
        Arguments:
        ----
        period {int} -- The number of periods to use when calculating 
            the mass index. (default: {9})
        Returns:
        ----
        {pd.DataFrame} -- A Pandas data frame with the Mass Index included.
        Usage:
        ----
            >>> historical_prices_df = trading_robot.grab_historical_prices(
                start=start_date,
                end=end_date,
                bar_size=1,
                bar_type='minute'
            )
            >>> price_data_frame = pd.DataFrame(data=historical_prices)
            >>> indicator_client = Indicators(price_data_frame=price_data_frame)
            >>> indicator_client.mass_index(period=9)
        """

        locals_data = locals()
        del locals_data['self']

        column_name = 'kst_oscillator'
        self._current_indicators[column_name] = {}
        self._current_indicators[column_name]['args'] = locals_data
        self._current_indicators[column_name]['func'] = self.kst_oscillator

        # Calculate the ROC 1.
        self._frame['roc_1'] = self._frame['close'].diff(
            r1 - 1) / self._frame['close'].shift(r1 - 1)

        # Calculate the ROC 2.
        self._frame['roc_2'] = self._frame['close'].diff(
            r2 - 1) / self._frame['close'].shift(r2 - 1)

        # Calculate the ROC 3.
        self._frame['roc_3'] = self._frame['close'].diff(
            r3 - 1) / self._frame['close'].shift(r3 - 1)

        # Calculate the ROC 4.
        self._frame['roc_4'] = self._frame['close'].diff(
            r4 - 1) / self._frame['close'].shift(r4 - 1)

        # Calculate the Mass Index.
        self._frame['roc_1_n'] = self._frame['roc_1'].transform(
            lambda x: x.rolling(window=n1).sum()
        )

        # Calculate the Mass Index.
        self._frame['roc_2_n'] = self._frame['roc_2'].transform(
            lambda x: x.rolling(window=n2).sum()
        )

        # Calculate the Mass Index.
        self._frame['roc_3_n'] = self._frame['roc_3'].transform(
            lambda x: x.rolling(window=n3).sum()
        )

        # Calculate the Mass Index.
        self._frame['roc_4_n'] = self._frame['roc_4'].transform(
            lambda x: x.rolling(window=n4).sum()
        )

        self._frame[column_name] = 100 * (self._frame['roc_1_n'] + 2 * self._frame['roc_2_n'] +
                                          3 * self._frame['roc_3_n'] + 4 * self._frame['roc_4_n'])
        self._frame[column_name + "_signal"] = self._frame['column_name'].transform(
            lambda x: x.rolling().mean()
        )

        # Clean up before sending back.
        self._frame.drop(
            labels=['roc_1', 'roc_2', 'roc_3', 'roc_4',
                    'roc_1_n', 'roc_2_n', 'roc_3_n', 'roc_4_n'],
            axis=1,
            inplace=True
        )

        return self._frame


# #KST Oscillator
# def KST(df, r1, r2, r3, r4, n1, n2, n3, n4):
#     M = df['Close'].diff(r1 - 1)
#     N = df['Close'].shift(r1 - 1)
#     ROC1 = M / N
#     M = df['Close'].diff(r2 - 1)
#     N = df['Close'].shift(r2 - 1)
#     ROC2 = M / N
#     M = df['Close'].diff(r3 - 1)
#     N = df['Close'].shift(r3 - 1)
#     ROC3 = M / N
#     M = df['Close'].diff(r4 - 1)
#     N = df['Close'].shift(r4 - 1)
#     ROC4 = M / N
#     KST = pd.Series(pd.rolling_sum(ROC1, n1) + pd.rolling_sum(ROC2, n2) * 2 + pd.rolling_sum(ROC3, n3) * 3 + pd.rolling_sum(ROC4, n4) * 4, name = 'KST_' + str(r1) + '_' + str(r2) + '_' + str(r3) + '_' + str(r4) + '_' + str(n1) + '_' + str(n2) + '_' + str(n3) + '_' + str(n4))
#     df = df.join(KST)
#     return df

    def refresh(self):
        """Updates the Indicator columns after adding the new rows."""

        # First update the groups since, we have new rows.
        self._price_groups = self._stock_frame.symbol_groups

        # Grab all the details of the indicators so far.
        for indicator in self._current_indicators:

            # Grab the function.
            indicator_argument = self._current_indicators[indicator]['args']

            # Grab the arguments.
            indicator_function = self._current_indicators[indicator]['func']

            # Update the function.
            indicator_function(**indicator_argument)
