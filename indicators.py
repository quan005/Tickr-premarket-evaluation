import operator
import talib
import math
import pandas as pd
import numpy as np
from statistics import mean
from datetime import time, datetime, timezone, timedelta
from typing import Any, List, Dict, Union, Optional, Tuple

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

    def s_r_levels(self, dataframe):
        price_dic = {}
        keys = []

        highPrice = dataframe['high']
        lowPrice = dataframe['low']
        openPrice = dataframe['open']
        closePrice = dataframe['close']

        high = highPrice.max()
        low = lowPrice.min(skipna=True)
        current_price = closePrice[-1]
        current_price_diff_above = round(current_price + 5, 2)
        current_price_diff_below = round(current_price - 5, 2)

        # Compare current price to each following price
        for a in range(0, len(highPrice) - 1):

            h_current = highPrice[a]
            h_price_diff_below = math.floor(h_current)
            h_price_diff_above = h_price_diff_below + 1
            h_key = f'{h_current}'
            if h_key in price_dic.keys():
                pass
            else:
                price_dic[f'{h_current}'] = {
                    'count': 0,
                    'mean': []
                }

            price_dic[f'{h_current}']['mean'].append(h_current)
            keys.append(h_current)

            for j in range(a + 1, len(highPrice) - 1):
                h_next_price = highPrice[j]
                h_next_price_below = math.floor(h_next_price)
                h_next_price_above = h_next_price_below + 1

                if len(keys) > 0:
                    added = False
                    j_count = 0

                    # Check whether price is already in the price_dic
                    while j_count < len(keys) - 1:

                        # if price is in price_dic increment by one
                        if keys[j_count] < h_next_price_above and keys[j_count] >= h_next_price_below:
                            price_dic[f'{keys[j_count]}']['count'] += 1
                            price_dic[f'{keys[j_count]}']['mean'].append(
                                h_next_price)
                            added = True
                            break
                        else:
                            j_count += 1

                    if added == False:
                        if h_next_price < h_price_diff_above and h_next_price >= h_price_diff_below:
                            price_dic[f'{h_current}']['count'] += 1
                            price_dic[f'{h_current}']['mean'].append(
                                h_next_price)
                            keys.append(h_current)
                        else:
                            pass
                    elif added == True:
                        break

                else:
                    # If price is not in price_dic
                    if h_next_price < h_price_diff_above and h_next_price >= h_price_diff_below:
                        price_dic[f'{h_current}']['count'] += 1
                        price_dic[f'{h_current}']['mean'].append(h_next_price)
                        keys.append(h_current)
                        continue
                    else:
                        continue

        # Compare current price to each following price
        for b in range(0, len(lowPrice) - 1):

            l_current = lowPrice[b]
            l_price_diff_below = math.floor(l_current)
            l_price_diff_above = l_price_diff_below + 1
            l_key = f'{l_current}'
            if l_key in price_dic.keys():
                pass
            else:
                price_dic[f'{l_current}'] = {
                    'count': 0,
                    'mean': []
                }

            price_dic[f'{l_current}']['mean'].append(l_current)
            keys.append(l_current)

            for k in range(b + 1, len(lowPrice) - 1):
                l_next_price = lowPrice[k]
                l_next_price_below = math.floor(l_next_price)
                l_next_price_above = l_next_price_below + 1

                if len(keys) > 0:
                    added = False
                    k_count = 0

                    # Check whether price is already in the price_dic
                    while k_count < len(keys) - 1:

                        # if price is in price_dic increment by one
                        if keys[k_count] < l_next_price_above and keys[k_count] >= l_next_price_below:
                            price_dic[f'{keys[k_count]}']['count'] += 1
                            price_dic[f'{keys[k_count]}']['mean'].append(
                                l_next_price)
                            added = True
                            break
                        else:
                            k_count += 1

                    if added == False:
                        if l_next_price < l_price_diff_above and l_next_price >= l_price_diff_below:
                            price_dic[f'{l_current}']['count'] += 1
                            price_dic[f'{l_current}']['mean'].append(
                                l_next_price)
                            keys.append(l_current)
                        else:
                            pass
                    elif added == True:
                        break

                else:
                    # If price is not in price_dic
                    if l_next_price < l_price_diff_above and l_next_price >= l_price_diff_below:
                        price_dic[f'{l_current}']['count'] += 1
                        price_dic[f'{l_current}']['mean'].append(l_next_price)
                        keys.append(l_current)
                        continue
                    else:
                        continue

        # Compare current price to each following price
        for c in range(0, len(openPrice)):

            o_current = openPrice[c]
            o_price_diff_below = math.floor(o_current)
            o_price_diff_above = o_price_diff_below + 1
            o_key = f'{o_current}'
            if o_key in price_dic.keys():
                pass
            else:
                price_dic[f'{o_current}'] = {
                    'count': 0,
                    'mean': []
                }

            price_dic[f'{o_current}']['mean'].append(o_current)
            keys.append(o_current)

            for l in range(c + 1, len(openPrice)):
                o_next_price = openPrice[l]
                o_next_price_below = math.floor(o_next_price)
                o_next_price_above = o_next_price_below + 1

                if len(keys) > 0:
                    added = False
                    o_count = 0

                    # Check whether price is already in the price_dic
                    while o_count < len(keys) - 1:

                        # if price is in price_dic increment by one
                        if keys[o_count] < o_next_price_above and keys[o_count] >= o_next_price_below:
                            price_dic[f'{keys[o_count]}']['count'] += 1
                            price_dic[f'{keys[o_count]}']['mean'].append(
                                o_next_price)
                            added = True
                            break
                        else:
                            o_count += 1

                    if added == False:
                        if o_next_price < o_price_diff_above and o_next_price >= o_price_diff_below:
                            price_dic[f'{o_current}']['count'] += 1
                            price_dic[f'{o_current}']['mean'].append(
                                o_next_price)
                            keys.append(o_current)
                        else:
                            pass
                    elif added == True:
                        break

                else:
                    # If price is not in price_dic
                    if o_next_price < o_price_diff_above and o_next_price >= o_price_diff_below:
                        price_dic[f'{o_current}']['count'] += 1
                        price_dic[f'{o_current}']['mean'].append(o_next_price)
                        keys.append(o_current)
                        continue
                    else:
                        continue

        # Compare current price to each following price
        for d in range(0, len(closePrice) - 1):

            c_current = closePrice[d]
            c_price_diff_below = math.floor(c_current)
            c_price_diff_above = c_price_diff_below + 1
            c_key = f'{c_current}'
            if c_key in price_dic.keys():
                pass
            else:
                price_dic[f'{c_current}'] = {
                    'count': 0,
                    'mean': []
                }

            price_dic[f'{o_current}']['mean'].append(o_current)
            keys.append(o_current)

            for m in range(d + 1, len(closePrice) - 1):
                c_next_price = closePrice[m]
                c_next_price_below = math.floor(c_next_price)
                c_next_price_above = c_next_price_below + 1

                if len(keys) > 0:
                    added = False
                    m_count = 0

                    # Check whether price is already in the price_dic
                    while m_count < len(keys) - 1:

                        # if price is in price_dic increment by one
                        if keys[m_count] < c_next_price_above and keys[m_count] >= c_next_price_below:
                            price_dic[f'{keys[m_count]}']['count'] += 1
                            price_dic[f'{keys[m_count]}']['mean'].append(
                                c_next_price)
                            added = True
                            break
                        else:
                            m_count += 1

                    if added == False:
                        if c_next_price < c_price_diff_above and c_next_price >= c_price_diff_below:
                            price_dic[f'{c_current}']['count'] += 1
                            price_dic[f'{c_current}']['mean'].append(
                                c_next_price)
                            keys.append(c_current)
                        else:
                            pass
                    elif added == True:
                        break

                else:
                    # If price is not in price_dic
                    if c_next_price < c_price_diff_above and c_next_price >= c_price_diff_below:
                        price_dic[f'{c_current}']['count'] += 1
                        price_dic[f'{c_current}']['mean'].append(c_next_price)
                        keys.append(c_current)
                        continue
                    else:
                        continue

        # price_dic = {price_key: {count: count_value for count, count_value in price_val.items() if isinstance(count_value, int) and count_value > 5}
        #              for price_key, price_val in price_dic.items() if float(price_key) >= current_price_diff_below and float(price_key) <= current_price_diff_above}
        print('price_dic', price_dic)

        key_levels = [high, low]

        for price in price_dic.items():
            print('price', price)
            if float(price[0]) >= current_price_diff_below and float(price[0]) <= current_price_diff_above and price[1]['count'] > 5:

                price_mean = round(mean(price[1]['mean']), 2)

                key_levels.append(price_mean)

        if 0.0 in key_levels:
            key_levels.remove(0.0)

        print('key_levels', key_levels)

        key_levels.sort(reverse=True)

        return key_levels

    def get_demand_zones(self, dataframe, ticker):
        demand_zone_dic = {}
        demand_zones = []

        highPrice = dataframe['high']
        lowPrice = dataframe['low']
        openPrice = dataframe['open']
        closePrice = dataframe['close']
        row_index = dataframe.index
        dateAndTime = list(row_index)

        # iterate through the dataframes
        for i in range(0, len(closePrice) - 1):
            if i > 4 and i < len(closePrice) - 5:
                four_date_ahead_start = str(dateAndTime[i + 4][1])
                four_date_previous = str(dateAndTime[i - 4][1])
                df_sorted = dataframe.sort_index()
                dataframe_slice = df_sorted.loc[(ticker, four_date_previous):(
                    ticker, four_date_ahead_start)]
                close_series = dataframe_slice['close']
                open_series = dataframe_slice['open']
                volume_series = dataframe_slice['volume']
                demand_check = self.isDemandCheck(
                    close_series, open_series, volume_series)

                # find the point with a significant price increase (which is any price increase of .40% and above)
                if demand_check and i < (len(closePrice) - 5):

                    # find the lowest low of the previous three candles
                    area_low = lowPrice[i]
                    area_top = openPrice[i] + .50
                    date_time = str(dateAndTime[i][1])

                    for j in range(1, 3):
                        if lowPrice[i - j] < area_low:
                            area_low = lowPrice[i - j]
                        else:
                            pass

                    # check if demand_zones list is empty, if so add the first demand zone
                    if len(demand_zones) < 1:
                        # create a string from the index of the candle to use as a key in the demand_zone_dic
                        new_demand_zone_string = f'{i}'
                        # create a array using the area_low and area_top
                        new_demand_zone_array = (
                            area_low, area_top, date_time)
                        demand_zones.append(new_demand_zone_array)
                        demand_zone_dic[new_demand_zone_string] = {
                            'interaction': 0,
                            'array': new_demand_zone_array
                        }
                    else:
                        k = 0
                        in_zone = False
                        one_previous = f'{i - 1}'
                        two_previous = f'{i - 2}'
                        three_previous = f'{i - 3}'
                        four_previous = f'{i - 4}'
                        # check if the zone is already in the demand zone
                        if one_previous in demand_zone_dic or two_previous in demand_zone_dic or three_previous in demand_zone_dic or four_previous in demand_zone_dic:
                            in_zone = True
                        # while k < len(demand_zones) - 1 and in_zone == False:
                        #     # if new zone interacts with an old zone (new_area_low is within an old demand zone)
                        #     if area_low <= demand_zones[k][0] and area_top <= demand_zones[k][1] or area_low <= demand_zones[k][0] and area_top >= demand_zones[k][1] or area_low > demand_zones[k][0] and area_low < demand_zones[k][1]:

                        #         # create a string from the old zone low (k[0]) and top (k[1]) seperated by a '-', to use as a key in the demand_zone_dic
                        #         old_zone_string = f'{demand_zones[k][0]}-{demand_zones[k][1]}'

                        #         # check the old demand zone to see how many interactions it has in the demand_zone_dic
                        #         # if its less than 2 then add 1 to the interaction and continue
                        #         if demand_zone_dic[old_zone_string]['interaction'] < 3:
                        #             demand_zone_dic[old_zone_string]['interaction'] + 1
                        #         # if its 2 or more then continue
                        #         elif demand_zone_dic[old_zone_string]['interaction'] == 3:
                        #             demand_zone_dic[old_zone_string]['interaction'] = 0

                        #         in_zone = True

                        #     else:
                        #         k += 1

                        if in_zone == False:
                            # create a array using the area_low and area_top
                            new_demand_zone_array = (
                                area_low, area_top, date_time)

                            # create a string from the index of the candle to use as a key in the demand_zone_dic
                            new_demand_zone_string = f'{i}'

                            # add new demand zone to demand_zone_dic using new_demand_zone_string as the key and 0 as the default value representing the zone interactions
                            demand_zone_dic[new_demand_zone_string] = {
                                'interaction': 0,
                                'array': new_demand_zone_array
                            }

                            # append new_demand_zone_array to the demand_zones list
                            demand_zones.append(new_demand_zone_array)
                # to early into the dataframe continue until there is more previous data to compare
                else:
                    continue
            else:
                pass

        # fliter demand_zone_dic and return list of
        final_demand_zones = []
        demand_limit = 0

        for key, value in sorted(demand_zone_dic.items(), key=lambda x: x[1]['array'][2], reverse=True):
            if value['interaction'] <= 2 and demand_limit < 15 or value['interaction'] <= 2 and len(demand_zone_dic.items()) < 15:
                final_demand_zones.append(value['array'])
                demand_limit += 1
            else:
                pass

        return final_demand_zones

    def get_supply_zones(self, dataframe, ticker):
        supply_zone_dic = {}
        supply_zones = []

        highPrice = dataframe['high']
        lowPrice = dataframe['low']
        openPrice = dataframe['open']
        closePrice = dataframe['close']
        row_index = dataframe.index
        dateAndTime = list(row_index)

        # iterate through the dataframes
        for i in range(0, len(closePrice) - 1):
            if i > 4 and i < len(closePrice) - 5:
                four_date_ahead_start = str(dateAndTime[i + 4][1])
                four_date_previous = str(dateAndTime[i - 4][1])
                df_sorted = dataframe.sort_index()
                dataframe_slice = df_sorted.loc[(ticker, four_date_previous):(
                    ticker, four_date_ahead_start)]
                close_series = dataframe_slice['close']
                open_series = dataframe_slice['open']
                volume_series = dataframe_slice['volume']
                supply_check = self.isSupplyCheck(
                    close_series, open_series, volume_series)

                # find the point with a significant price increase (which is any price increase of .40% and above)
                if supply_check and i < (len(closePrice) - 5):

                    # find the highest high of the previous three candles
                    area_top = highPrice[i]
                    area_low = openPrice[i] - .50
                    date_time = str(dateAndTime[i][1])

                    for j in range(1, 3):
                        if highPrice[i - j] < area_top:
                            area_top = highPrice[i - j]
                        else:
                            pass

                    # check if supply_zones list is empty, if so add the first demand zone
                    if len(supply_zones) < 1:
                        # create a string from the index of the candle to use as a key in the supply_zone_dic
                        new_supply_zone_string = f'{i}'
                        # create a array using the area_top and area_low and the
                        new_supply_zone_array = [area_top, area_low, date_time]
                        supply_zones.append(new_supply_zone_array)
                        supply_zone_dic[new_supply_zone_string] = {
                            'interaction': 0,
                            'array': new_supply_zone_array
                        }
                    else:
                        k = 0
                        in_zone = False
                        one_previous = f'{i - 1}'
                        two_previous = f'{i - 2}'
                        three_previous = f'{i - 3}'
                        four_previous = f'{i - 4}'
                        # check if the zone is already in the supply zone
                        if one_previous in supply_zone_dic or two_previous in supply_zone_dic or three_previous in supply_zone_dic or four_previous in supply_zone_dic:
                            in_zone = True
                        # while k < len(demand_zones) - 1 and in_zone == False:
                        #     # if new zone interacts with an old zone (new_area_low is within an old demand zone)
                        #     if area_low <= demand_zones[k][0] and area_top <= demand_zones[k][1] or area_low <= demand_zones[k][0] and area_top >= demand_zones[k][1] or area_low > demand_zones[k][0] and area_low < demand_zones[k][1]:

                        #         # create a string from the old zone low (k[0]) and top (k[1]) seperated by a '-', to use as a key in the demand_zone_dic
                        #         old_zone_string = f'{demand_zones[k][0]}-{demand_zones[k][1]}'

                        #         # check the old demand zone to see how many interactions it has in the demand_zone_dic
                        #         # if its less than 2 then add 1 to the interaction and continue
                        #         if demand_zone_dic[old_zone_string]['interaction'] < 3:
                        #             demand_zone_dic[old_zone_string]['interaction'] + 1
                        #         # if its 2 or more then continue
                        #         elif demand_zone_dic[old_zone_string]['interaction'] == 3:
                        #             demand_zone_dic[old_zone_string]['interaction'] = 0

                        #         in_zone = True

                        #     else:
                        #         k += 1

                        if in_zone == False:
                            # create a array using the area_top and area_low
                            new_supply_zone_array = [
                                area_top, area_low, date_time]

                            # create a string from the index of the candle to use as a key in the supply_zone_dic
                            new_supply_zone_string = f'{i}'

                            # add new supply zone to supply_zone_dic using new_supply_zone_string as the key and 0 as the default value representing the zone interactions
                            supply_zone_dic[new_supply_zone_string] = {
                                'interaction': 0,
                                'array': new_supply_zone_array
                            }

                            # append new_supply_zone_array to the demand_zones list
                            supply_zones.append(new_supply_zone_array)
                # to early into the dataframe continue until there is more previous data to compare
                else:
                    continue
            else:
                pass

        # fliter supply_zone_dic and return list of
        final_supply_zones = []
        supply_limit = 0

        for key, value in sorted(supply_zone_dic.items(), key=lambda x: x[1]['array'][2], reverse=True):
            if value['interaction'] <= 2 and supply_limit < 15 or value['interaction'] <= 2 and len(supply_zone_dic.items()) < 15:
                final_supply_zones.append(value['array'])
                supply_limit += 1
            else:
                pass

        return final_supply_zones

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
            # last = check_for_pattern.tail(1).values[0]
            last = sum(check_for_pattern.values)

            if last > 0:
                return 'BULLISH'
            elif last < 0:
                return 'BEARISH'

        return 'NO OPPURTUNITY'

    def get_support_resistance(self, sr_list: list, close_price_series):
        # Find which support/resistance lines
        # are surrounding the current price.
        for i in range(len(sr_list)):

            if close_price_series[-1] <= sr_list[i] and close_price_series[-1] >= sr_list[i + 1]:
                return {
                    'resistance': sr_list[i],
                    'support': sr_list[i + 1],
                }
            else:
                pass

        return 'Theres is no Support / Resistance levels'

    def isPriceSideways(self, close_price_series):

        within_range = 0

        for i in range(-1, -36):
            j = i - 3
            diff_above = close_price_series[i] + .45
            diff_below = close_price_series[i] - .45

            if close_price_series[j] < diff_above and close_price_series[j] > diff_below:
                within_range += 1
            else:
                pass

        diff_percent = within_range / 36

        if diff_percent > .4:
            return True
        else:
            return False

    def isDemandCheck(self, close_price_series, open_price_series, volume_series):

        main_close_price = 4
        main_close_price_diff_above = close_price_series[main_close_price] + .45
        main_close_price_diff_below = close_price_series[main_close_price] - .45
        one_previous_close_price = main_close_price - 1
        one_previous_close_price_diff_above = close_price_series[one_previous_close_price] + .45
        one_previous_close_price_diff_below = close_price_series[one_previous_close_price] - .45
        two_previous_close_price = main_close_price - 2
        two_previous_close_price_diff_above = close_price_series[two_previous_close_price] + .45
        two_previous_close_price_diff_below = close_price_series[two_previous_close_price] - .45
        three_previous_close_price = main_close_price - 3
        three_previous_close_price_diff_above = close_price_series[three_previous_close_price] + .45
        three_previous_close_price_diff_below = close_price_series[three_previous_close_price] - .45
        four_previous_close_price = main_close_price - 4
        one_candle_ahead = main_close_price + 1
        two_candle_ahead = main_close_price + 2
        three_candle_ahead = main_close_price + 3
        three_candle_increase = (
            open_price_series[three_candle_ahead] * .0035) + open_price_series[three_candle_ahead]
        three_candle_close = close_price_series[three_candle_ahead]
        four_candle_ahead = main_close_price + 4
        four_candle_increase = (
            open_price_series[four_candle_ahead] * .0035) + open_price_series[four_candle_ahead]
        four_candle_close = close_price_series[four_candle_ahead]

        # find the point with a significant price increase (which is any price increase of .40% and above)
        if close_price_series[main_close_price] >= ((open_price_series[main_close_price] * .004) + open_price_series[main_close_price]) and volume_series[main_close_price] >= 500000:
            # check if candle ahead have higher highs
            if close_price_series[one_candle_ahead] > close_price_series[main_close_price] or close_price_series[one_candle_ahead] > close_price_series[main_close_price] and close_price_series[two_candle_ahead] > close_price_series[one_candle_ahead] or three_candle_close >= three_candle_increase or four_candle_close >= four_candle_increase:
                if close_price_series[one_previous_close_price] < close_price_series[main_close_price] and close_price_series[one_previous_close_price] < close_price_series[two_previous_close_price]:
                    return True
                elif close_price_series[one_previous_close_price] < close_price_series[main_close_price] and close_price_series[two_previous_close_price] <= one_previous_close_price_diff_above and close_price_series[two_previous_close_price] >= one_previous_close_price_diff_below:
                    return True
                elif close_price_series[one_previous_close_price] < close_price_series[main_close_price] and close_price_series[two_previous_close_price] < close_price_series[one_previous_close_price]:
                    return True
                elif close_price_series[one_previous_close_price] <= main_close_price_diff_above and close_price_series[one_previous_close_price] >= main_close_price_diff_below and close_price_series[two_previous_close_price] > close_price_series[one_previous_close_price]:
                    return True
                elif close_price_series[one_previous_close_price] <= main_close_price_diff_above and close_price_series[one_previous_close_price] >= main_close_price_diff_below and close_price_series[two_previous_close_price] < close_price_series[one_previous_close_price]:
                    return True
                elif close_price_series[one_previous_close_price] < close_price_series[main_close_price] and close_price_series[two_previous_close_price] < close_price_series[one_previous_close_price]:
                    return True
                elif close_price_series[one_previous_close_price] > close_price_series[main_close_price] and close_price_series[two_previous_close_price] < close_price_series[one_previous_close_price]:
                    return True
                else:
                    return False
            else:
                return False
        else:
            return False

    def isSupplyCheck(self, close_price_series, open_price_series, volume_series):

        main_close_price = 4
        main_close_price_diff_above = close_price_series[main_close_price] + .45
        main_close_price_diff_below = close_price_series[main_close_price] - .45
        one_candle_ahead = main_close_price + 1
        two_candle_ahead = main_close_price + 2
        three_candle_ahead = main_close_price + 3
        three_candle_close = close_price_series[three_candle_ahead]
        three_candle_decrease = (
            open_price_series[three_candle_ahead] * .003) - open_price_series[three_candle_ahead]
        four_candle_ahead = main_close_price + 4
        four_candle_close = close_price_series[four_candle_ahead]
        four_candle_decrease = (
            open_price_series[four_candle_ahead] * .003) - open_price_series[four_candle_ahead]
        one_previous_candle = main_close_price - 1
        one_previous_candle_close_price_diff_above = close_price_series[one_previous_candle] + .45
        one_previous_candle_close_price_diff_below = close_price_series[one_previous_candle] - .45
        two_previous_candle = main_close_price - 2
        two_previous_candle_close_price_diff_above = close_price_series[two_previous_candle] + .45
        two_previous_candle_close_price_diff_below = close_price_series[two_previous_candle] - .45
        three_previous_candle = main_close_price - 3
        three_previous_candle_close_price_diff_above = close_price_series[
            three_previous_candle] + .45
        three_previous_candle_close_price_diff_below = close_price_series[
            three_previous_candle] - .45
        four_previous_candle = main_close_price - 4

        # find the point with a significant price decrease (which is any price decrease of .30% and below)
        if close_price_series[main_close_price] <= (open_price_series[main_close_price] - (open_price_series[main_close_price] * .0046)) and volume_series[main_close_price] >= 500000:
            # check if candles ahead have lower highs
            if close_price_series[one_candle_ahead] < close_price_series[main_close_price] or close_price_series[one_candle_ahead] < close_price_series[main_close_price] and close_price_series[two_candle_ahead] < close_price_series[one_candle_ahead] or three_candle_close <= three_candle_decrease or four_candle_close <= four_candle_decrease:
                if close_price_series[one_previous_candle] > close_price_series[main_close_price] and close_price_series[two_previous_candle] < close_price_series[one_previous_candle]:
                    return True
                elif close_price_series[one_previous_candle] > close_price_series[main_close_price] and close_price_series[two_previous_candle] >= one_previous_candle_close_price_diff_below and two_previous_candle <= one_previous_candle_close_price_diff_above:
                    return True
                elif close_price_series[one_previous_candle] >= main_close_price_diff_below and close_price_series[one_previous_candle] <= main_close_price_diff_above and close_price_series[two_previous_candle] < close_price_series[one_previous_candle]:
                    return True
                elif close_price_series[one_previous_candle] > close_price_series[main_close_price] and close_price_series[two_previous_candle] > close_price_series[one_previous_candle]:
                    return True
                elif close_price_series[one_previous_candle] >= main_close_price_diff_below and close_price_series[one_previous_candle] <= main_close_price_diff_above and close_price_series[two_previous_candle] > close_price_series[one_previous_candle]:
                    return True
                elif close_price_series[one_previous_candle] > close_price_series[main_close_price] and close_price_series[two_previous_candle] > close_price_series[main_close_price]:
                    return True
                elif close_price_series[one_previous_candle] < close_price_series[main_close_price] and close_price_series[two_previous_candle] > close_price_series[one_previous_candle]:
                    return True
                else:
                    return False
            else:
                return False
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
