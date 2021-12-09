import pandas as pd
import time as time_true

from td.client import TDClient
from td.utils import TDUtilities

from datetime import datetime, time, timezone, timedelta

from typing import List, Dict, Union, Optional

from portfolio import Portfolio
from stock_frame import StockFrame

milliseconds_since_epoch = TDUtilities().milliseconds_since_epoch


class PyTrader:
    def __init__(self, client_id: str, redirect_uri: str, paper_trading: bool = True, credentials_path: Optional[str] = None, trading_account: Optional[str] = None) -> None:
        """Initalizes a new instance of the robot and logs into the API platform specified.
        Arguments:
        ----
        client_id {str} -- The Consumer ID assigned to you during the App registration. 
            This can be found at the app registration portal.
        redirect_uri {str} -- This is the redirect URL that you specified when you created your
            TD Ameritrade Application.
        Keyword Arguments:
        ----
        credentials_path {str} -- The path to the session state file used to prevent a full 
            OAuth workflow. (default: {None})
        trading_account {str} -- Your TD Ameritrade account number. (default: {None})
        """

        # Set the attirbutes
        self.trading_account = trading_account
        self.client_id = client_id
        self.redirect_uri = redirect_uri
        self.credentials_path = credentials_path
        self.session: TDClient = self._create_session()
        self.trades = {}
        self.historical_prices = {}
        self.options_chain = {}
        self.stock_frame: StockFrame = None
        self.paper_trading = paper_trading

        self._bar_size = None
        self._bar_type = None

    def _create_session(self) -> TDClient:
        """Start a new session.
        Creates a new session with the TD Ameritrade API and logs the user into
        the new session.
        Returns:
        ----
        TDClient -- A TDClient object with an authenticated sessions.
        """

        # Create a new instance of the client
        td_client = TDClient(
            client_id=self.client_id,
            redirect_uri=self.redirect_uri,
            credentials_path=self.credentials_path
        )

        # log the client into the new session
        td_client.login()

        return td_client

    def create_portfolio(self) -> Portfolio:
        """Create a new portfolio.
        Creates a Portfolio Object to help store and organize positions
        as they are added and removed during trading.
        Usage:
        ----
            >>> trading_robot = PyRobot(
            client_id=CLIENT_ID, 
            redirect_uri=REDIRECT_URI, 
            credentials_path=CREDENTIALS_PATH
            )
            >>> portfolio = trading_robot.create_portfolio()
            >>> portfolio
            <pyrobot.portfolio.Portfolio object at 0x0392BF88>
        Returns:
        ----
        Portfolio -- A pyrobot.Portfolio object with no positions.
        """

        # Initalize the portfolio.
        self.portfolio = Portfolio(account_number=self.trading_account)

        # Assign the Client
        self.portfolio.td_client = self.session

        return self.portfolio

    def grab_current_quotes(self) -> dict:
        """Grabs the current quotes for all positions in the portfolio.
        Makes a call to the TD Ameritrade Get Quotes endpoint with all
        the positions in the portfolio. If only one position exist it will
        return a single dicitionary, otherwise a nested dictionary.
        Usage:
        ----
            >>> trading_robot = PyRobot(
                client_id=CLIENT_ID, 
                redirect_uri=REDIRECT_URI, 
                credentials_path=CREDENTIALS_PATH
            )
            >>> trading_robot_portfolio.add_position(
            symbol='MSFT',
            asset_type='equity'
            )
            >>> current_quote = trading_robot.grab_current_quotes()
            >>> current_quote
            {
                "MSFT": {
                    "assetType": "EQUITY",
                    "assetMainType": "EQUITY",
                    "cusip": "594918104",
                    ...
                    "regularMarketPercentChangeInDouble": 0,
                    "delayed": true
                }
            }
            >>> trading_robot = PyRobot(
            client_id=CLIENT_ID, 
            redirect_uri=REDIRECT_URI, 
            credentials_path=CREDENTIALS_PATH
            )
            >>> trading_robot_portfolio.add_position(
            symbol='MSFT',
            asset_type='equity'
            )
            >>> trading_robot_portfolio.add_position(
            symbol='AAPL',
            asset_type='equity'
            )
            >>> current_quote = trading_robot.grab_current_quotes()
            >>> current_quote
            {
                "MSFT": {
                    "assetType": "EQUITY",
                    "assetMainType": "EQUITY",
                    "cusip": "594918104",
                    ...
                    "regularMarketPercentChangeInDouble": 0,
                    "delayed": False
                },
                "AAPL": {
                    "assetType": "EQUITY",
                    "assetMainType": "EQUITY",
                    "cusip": "037833100",
                    ...
                    "regularMarketPercentChangeInDouble": 0,
                    "delayed": False
                }
            }
        Returns:
        ----
        dict -- A dictionary containing all the quotes for each position.
        """

        # First grab all the symbols.
        symbols = self.portfolio.positions.keys()

        # Grab the quotes.
        quotes = self.session.get_quotes(instruments=list(symbols))

        return quotes

    def grab_single_historical_prices(self, symbol: str, start: datetime, end: datetime, period_type: str = 'day', bar_size: int = 1,
                                      bar_type: str = 'minute') -> List[dict]:
        """Grabs the historical prices for all the postions in a portfolio.
        Overview:
        ----
        Any of the historical price data returned will include extended hours
        price data by default.
        Arguments:
        ----
        start {datetime} -- Defines the start date for the historical prices.
        end {datetime} -- Defines the end date for the historical prices.
        Keyword Arguments:
        ----
        bar_size {int} -- Defines the size of each bar. (default: {1})
        bar_type {str} -- Defines the bar type, can be one of the following:
            `['minute', 'week', 'month', 'year']` (default: {'minute'})
        symbols {List[str]} -- A list of ticker symbols to pull. (default: None)
        Returns:
        ----
        {List[Dict]} -- The historical price candles.
        Usage:
        ----
            >>> trading_robot = PyRobot(
                client_id=CLIENT_ID,
                redirect_uri=REDIRECT_URI,
                credentials_path=CREDENTIALS_PATH
                )
            >>> start_date = datetime.today()
            >>> end_date = start_date - timedelta(days=30)
            >>> historical_prices = trading_robot.grab_historical_prices(
                    start=end_date,
                    end=start_date,
                    bar_size=1,
                    bar_type='minute'
                )
        """

        self._bar_size = bar_size
        self._bar_type = bar_type

        start = str(milliseconds_since_epoch(dt_object=start))
        end = str(milliseconds_since_epoch(dt_object=end))

        new_prices = []

        historical_prices_response = self.session.get_price_history(
            symbol=symbol,
            period_type=period_type,
            start_date=start,
            end_date=end,
            frequency_type=bar_type,
            frequency=bar_size,
            extended_hours=False
        )

        self.historical_prices[symbol] = {}
        self.historical_prices[symbol]['candles'] = historical_prices_response['candles']

        for candle in historical_prices_response['candles']:

            new_price_mini_dict = {}
            new_price_mini_dict['symbol'] = symbol
            new_price_mini_dict['open'] = candle['open']
            new_price_mini_dict['close'] = candle['close']
            new_price_mini_dict['high'] = candle['high']
            new_price_mini_dict['low'] = candle['low']
            new_price_mini_dict['volume'] = candle['volume']
            new_price_mini_dict['datetime'] = candle['datetime']
            new_prices.append(new_price_mini_dict)

        self.historical_prices['aggregated'] = new_prices

        return self.historical_prices

    def grab_historical_prices(self, start: datetime, end: datetime, period_type: str = 'day', bar_size: int = 1,
                               bar_type: str = 'minute', symbols: Optional[List[str]] = None) -> List[dict]:
        """Grabs the historical prices for all the postions in a portfolio.
        Overview:
        ----
        Any of the historical price data returned will include extended hours
        price data by default.
        Arguments:
        ----
        start {datetime} -- Defines the start date for the historical prices.
        end {datetime} -- Defines the end date for the historical prices.
        Keyword Arguments:
        ----
        bar_size {int} -- Defines the size of each bar. (default: {1})
        bar_type {str} -- Defines the bar type, can be one of the following:
            `['minute', 'week', 'month', 'year']` (default: {'minute'})
        symbols {List[str]} -- A list of ticker symbols to pull. (default: None)
        Returns:
        ----
        {List[Dict]} -- The historical price candles.
        Usage:
        ----
            >>> trading_robot = PyRobot(
                client_id=CLIENT_ID,
                redirect_uri=REDIRECT_URI,
                credentials_path=CREDENTIALS_PATH
                )
            >>> start_date = datetime.today()
            >>> end_date = start_date - timedelta(days=30)
            >>> historical_prices = trading_robot.grab_historical_prices(
                    start=end_date,
                    end=start_date,
                    bar_size=1,
                    bar_type='minute'
                )
        """

        self._bar_size = bar_size
        self._bar_type = bar_type

        start = str(milliseconds_since_epoch(dt_object=start))
        end = str(milliseconds_since_epoch(dt_object=end))

        new_prices = []

        if not symbols:
            symbols = self.portfolio.positions

        for symbol in symbols:

            historical_prices_response = self.session.get_price_history(
                symbol=symbol,
                period_type=period_type,
                start_date=start,
                end_date=end,
                frequency_type=bar_type,
                frequency=bar_size,
                extended_hours=False
            )

            self.historical_prices[symbol] = {}
            self.historical_prices[symbol]['candles'] = historical_prices_response['candles']

            for candle in historical_prices_response['candles']:

                new_price_mini_dict = {}
                new_price_mini_dict['symbol'] = symbol
                new_price_mini_dict['open'] = candle['open']
                new_price_mini_dict['close'] = candle['close']
                new_price_mini_dict['high'] = candle['high']
                new_price_mini_dict['low'] = candle['low']
                new_price_mini_dict['volume'] = candle['volume']
                new_price_mini_dict['datetime'] = candle['datetime']
                new_prices.append(new_price_mini_dict)

        self.historical_prices['aggregated'] = new_prices

        return self.historical_prices

    def get_symbol_latest_bar(self, symbol: str) -> List[dict]:
        """Returns the latest bar for the given symbol.
        Returns:
        ---
        {List[dict]} -- A simplified quote list.
        Usage:
        ----
            >>> trading_robot = PyRobot(
                client_id=CLIENT_ID,
                redirect_uri=REDIRECT_URI,
                credentials_path=CREDENTIALS_PATH
            )
            >>> latest_bars = trading_robot.get_latest_bar()
            >>> latest_bars
        """

        # Grab the info from the last quest.
        bar_size = self._bar_size
        bar_type = self._bar_type

        # Define the start and end date.
        end_date = datetime.today()
        start_date = end_date - timedelta(days=1)
        start = str(milliseconds_since_epoch(dt_object=start_date))
        end = str(milliseconds_since_epoch(dt_object=end_date))

        latest_prices = []

        # Grab the request.
        historical_prices_response = self.session.get_price_history(
            symbol=symbol,
            period_type='day',
            start_date=start,
            end_date=end,
            frequency_type=bar_type,
            frequency=bar_size,
            extended_hours=False
        )

        if 'error' in historical_prices_response:

            time_true.sleep(2)

            # Grab the request.
            historical_prices_response = self.session.get_price_history(
                symbol=symbol,
                period_type='day',
                start_date=start,
                end_date=end,
                frequency_type=bar_type,
                frequency=bar_size,
                extended_hours=False
            )

        # parse the candles.
        for candle in historical_prices_response['candles'][-1:]:

            new_price_mini_dict = {}
            new_price_mini_dict['symbol'] = symbol
            new_price_mini_dict['open'] = candle['open']
            new_price_mini_dict['close'] = candle['close']
            new_price_mini_dict['high'] = candle['high']
            new_price_mini_dict['low'] = candle['low']
            new_price_mini_dict['volume'] = candle['volume']
            new_price_mini_dict['datetime'] = candle['datetime']
            latest_prices.append(new_price_mini_dict)

        return latest_prices

    def get_latest_bar(self) -> List[dict]:
        """Returns the latest bar for each symbol in the portfolio.
        Returns:
        ---
        {List[dict]} -- A simplified quote list.
        Usage:
        ----
            >>> trading_robot = PyRobot(
                client_id=CLIENT_ID,
                redirect_uri=REDIRECT_URI,
                credentials_path=CREDENTIALS_PATH
            )
            >>> latest_bars = trading_robot.get_latest_bar()
            >>> latest_bars
        """

        # Grab the info from the last quest.
        bar_size = self._bar_size
        bar_type = self._bar_type

        # Define the start and end date.
        end_date = datetime.today()
        start_date = end_date - timedelta(days=1)
        start = str(milliseconds_since_epoch(dt_object=start_date))
        end = str(milliseconds_since_epoch(dt_object=end_date))

        latest_prices = []

        # Loop through each symbol.
        for symbol in self.portfolio.positions:

            # Grab the request.
            historical_prices_response = self.session.get_price_history(
                symbol=symbol,
                period_type='day',
                start_date=start,
                end_date=end,
                frequency_type=bar_type,
                frequency=bar_size,
                extended_hours=False
            )

            if 'error' in historical_prices_response:

                time_true.sleep(2)

                # Grab the request.
                historical_prices_response = self.session.get_price_history(
                    symbol=symbol,
                    period_type='day',
                    start_date=start,
                    end_date=end,
                    frequency_type=bar_type,
                    frequency=bar_size,
                    extended_hours=False
                )

            # parse the candles.
            for candle in historical_prices_response['candles'][-1:]:

                new_price_mini_dict = {}
                new_price_mini_dict['symbol'] = symbol
                new_price_mini_dict['open'] = candle['open']
                new_price_mini_dict['close'] = candle['close']
                new_price_mini_dict['high'] = candle['high']
                new_price_mini_dict['low'] = candle['low']
                new_price_mini_dict['volume'] = candle['volume']
                new_price_mini_dict['datetime'] = candle['datetime']
                latest_prices.append(new_price_mini_dict)

        return latest_prices

    def create_stock_frame(self, data: List[dict]) -> StockFrame:
        """Generates a new StockFrame Object.
        Arguments:
        ----
        data {List[dict]} -- The data to add to the StockFrame object.
        Returns:
        ----
        StockFrame -- A multi-index pandas data frame built for trading.
        """

        # Create the Frame.
        self.stock_frame = StockFrame(data=data)

        return self.stock_frame

    def get_accounts(self, account_number: str = None, all_accounts: bool = False) -> dict:
        """Returns all the account balances for a specified account.
        Keyword Arguments:
        ----
        account_number {str} -- The account number you want to query. (default: {None})
        all_accounts {bool} -- Specifies whether you want to grab all accounts `True` or not
            `False`. (default: {False})
        Returns:
        ----
        Dict -- A dictionary containing all the information in your account.
        Usage:
        ----
            >>> trading_robot = PyRobot(
                client_id=CLIENT_ID,
                redirect_uri=REDIRECT_URI,
                credentials_path=CREDENTIALS_PATH
            )
            >>> trading_robot_accounts = trading_robot.session.get_accounts(
                account_number="<YOUR ACCOUNT NUMBER>"
            )
            >>> trading_robot_accounts
            [
                {
                    'account_number': 'ACCOUNT_ID',
                    'account_type': 'CASH',
                    'available_funds': 0.0,
                    'buying_power': 0.0,
                    'cash_available_for_trading': 0.0,
                    'cash_available_for_withdrawl': 0.0,
                    'cash_balance': 0.0,
                    'day_trading_buying_power': 0.0,
                    'long_market_value': 0.0,
                    'maintenance_call': 0.0,
                    'maintenance_requirement': 0.0,
                    'short_balance': 0.0,
                    'short_margin_value': 0.0,
                    'short_market_value': 0.0
                }
            ]
        """

        # Depending on how the client was initalized, either use the state account
        # or the one passed through the function.
        if all_accounts:
            account = 'all'
        elif self.trading_account:
            account = self.trading_account
        else:
            account = account_number

        # Grab the accounts.
        accounts = self.session.get_accounts(
            account=account
        )

        # Parse the account info.
        accounts_parsed = self._parse_account_balances(
            accounts_response=accounts
        )

        return accounts_parsed

    def _parse_account_balances(self, accounts_response: Union[Dict, List]) -> List[Dict]:
        """Parses an Account response into a more simplified dictionary.
        Arguments:
        ----
        accounts_response {Union[Dict, List]} -- A response from the `get_accounts` call.
        Returns:
        ----
        List[Dict] -- A list of simplified account dictionaries.
        """

        account_lists = []

        if isinstance(accounts_response, dict):

            account_dict = {}

            for account_type_key in accounts_response:

                account_info = accounts_response[account_type_key]

                account_id = account_info['accountId']
                account_type = account_info['type']
                account_current_balances = account_info['currentBalances']
                # account_inital_balances = account_info['initialBalances']

                account_dict['account_number'] = account_id
                account_dict['account_type'] = account_type
                account_dict['cash_balance'] = account_current_balances['cashBalance']
                account_dict['long_market_value'] = account_current_balances['longMarketValue']

                account_dict['cash_available_for_trading'] = account_current_balances.get(
                    'cashAvailableForTrading', 0.0
                )
                account_dict['cash_available_for_withdrawl'] = account_current_balances.get(
                    'cashAvailableForWithDrawal', 0.0
                )
                account_dict['available_funds'] = account_current_balances.get(
                    'availableFunds', 0.0
                )
                account_dict['buying_power'] = account_current_balances.get(
                    'buyingPower', 0.0
                )
                account_dict['day_trading_buying_power'] = account_current_balances.get(
                    'dayTradingBuyingPower', 0.0
                )
                account_dict['maintenance_call'] = account_current_balances.get(
                    'maintenanceCall', 0.0
                )
                account_dict['maintenance_requirement'] = account_current_balances.get(
                    'maintenanceRequirement', 0.0
                )

                account_dict['short_balance'] = account_current_balances.get(
                    'shortBalance', 0.0
                )
                account_dict['short_market_value'] = account_current_balances.get(
                    'shortMarketValue', 0.0
                )
                account_dict['short_margin_value'] = account_current_balances.get(
                    'shortMarginValue', 0.0
                )

                account_lists.append(account_dict)

        elif isinstance(accounts_response, list):

            for account in accounts_response:

                account_dict = {}

                for account_type_key in account:

                    account_info = account[account_type_key]

                    account_id = account_info['accountId']
                    account_type = account_info['type']
                    account_current_balances = account_info['currentBalances']
                    # account_inital_balances = account_info['initialBalances']

                    account_dict['account_number'] = account_id
                    account_dict['account_type'] = account_type
                    account_dict['cash_balance'] = account_current_balances['cashBalance']
                    account_dict['long_market_value'] = account_current_balances['longMarketValue']

                    account_dict['cash_available_for_trading'] = account_current_balances.get(
                        'cashAvailableForTrading', 0.0
                    )
                    account_dict['cash_available_for_withdrawl'] = account_current_balances.get(
                        'cashAvailableForWithDrawal', 0.0
                    )
                    account_dict['available_funds'] = account_current_balances.get(
                        'availableFunds', 0.0
                    )
                    account_dict['buying_power'] = account_current_balances.get(
                        'buyingPower', 0.0
                    )
                    account_dict['day_trading_buying_power'] = account_current_balances.get(
                        'dayTradingBuyingPower', 0.0
                    )
                    account_dict['maintenance_call'] = account_current_balances.get(
                        'maintenanceCall', 0.0
                    )
                    account_dict['maintenance_requirement'] = account_current_balances.get(
                        'maintenanceRequirement', 0.0
                    )
                    account_dict['short_balance'] = account_current_balances.get(
                        'shortBalance', 0.0
                    )
                    account_dict['short_market_value'] = account_current_balances.get(
                        'shortMarketValue', 0.0
                    )
                    account_dict['short_margin_value'] = account_current_balances.get(
                        'shortMarginValue', 0.0
                    )

                    account_lists.append(account_dict)

        return account_lists
