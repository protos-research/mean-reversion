# -*- coding: utf-8 -*-
import sqlalchemy as sql
import pandas as pd
from datetime import timedelta



def load_fundamental_data(fundamental,tickers,start_date,end_date):
    
    ## Load Data:
    
    tickers = ['Date'] + tickers
    ticker_str = ', '.join("`{}`".format(ticker) for ticker in tickers)
    
    engine = sql.create_engine('mysql+pymysql://protos-public:protos-public@google-sheet-data.cfyqhzfdz93r.eu-west-1.rds.amazonaws.com:3306/public')
            
    if(fundamental == 'nva'):
        data = pd.read_sql("Select " + str(ticker_str) + " From nva", con=engine)
    if(fundamental == 'nvt'):
        data = pd.read_sql("Select " + str(ticker_str) + " From nvt", con=engine)
    if(fundamental == 'nvv'):
        data = pd.read_sql("Select " + str(ticker_str) + " From nvv", con=engine)
        
    ## Clean Data:
        
        data.set_index('Date', inplace=True)
        data.index = pd.to_datetime(data.index)
        date_filter = (data.index >= start_date) & (data.index <= end_date)
        data = data[date_filter]
        # frequency_filter = data['Date'] == ...
        # price = price[frequency_filter]
        data.fillna("NaN")
        try:
            data = data.apply(lambda x: x.str.replace(',',''))
        except: pass
        data = data.apply(pd.to_numeric, errors='coerce')
        
    return data    
    
    
    

def load_ohlc(tickers,start_date,end_date):
    
    ## Load Data:
    
    tickers = ['Date'] + tickers
    ticker_str = ', '.join("`{}`".format(ticker) for ticker in tickers)
    
    engine = sql.create_engine('mysql+pymysql://protos-public:protos-public@google-sheet-data.cfyqhzfdz93r.eu-west-1.rds.amazonaws.com:3306/public')
            
    opening = pd.read_sql("Select " + str(ticker_str) + " From open", con=engine)
    high = pd.read_sql("Select " + str(ticker_str) + " From high", con=engine)
    low = pd.read_sql("Select " + str(ticker_str) + " From low", con=engine)
    closing = pd.read_sql("Select " + str(ticker_str) + " From close", con=engine)
    
    ## Clean Data:
    
    data = [opening, high, low, closing]
    cleaned_data = []
    
    for i in data:
    
        i.set_index('Date', inplace=True)
        i.index = pd.to_datetime(i.index)
        date_filter = (i.index >= start_date) & (i.index <= end_date)
        i = i[date_filter]
        # frequency_filter = data['Date'] == ...
        # price = price[frequency_filter]
        i.fillna('NaN')
        i = i.apply(lambda x: x.str.replace(',',''))
        i = i.apply(pd.to_numeric, errors='coerce')
        
        cleaned_data.append(i)

    return cleaned_data


def get_signals(strategies,ohlc,data,parameters):
    
    for strategy in strategies:
        
        if(strategy == 'mean-reversion'): signals = mean_reversion(ohlc,parameters)
        
    return signals



def mean_reversion(ohlc,parameters):
    
    bollinger_bands = get_indicator('bollinger-bands',ohlc,parameters)
    #stochastic = get_indicator('stochastic',ohlc,parameters)
        
    ### Entry Signals: 
    
    ## Long Signals
    # Low price below lower bollinger band
    
    long_filter_today = ohlc[2].iloc[-1] < bollinger_bands[0].iloc[-1]
    long_signal_today = long_filter_today*1
    
    
    # Subtract yesterdays signal from todays or add them together:
    # Subtract: If yesterday had a signal, today should have no signal
    # Add: Only generate a signal if today AND yesterday had signals

    long_filter_prev2 = ohlc[2].iloc[ohlc[2].shape[0]-2] < bollinger_bands[0].iloc[bollinger_bands[0].shape[0]-2]
    long_signal_prev2 = long_filter_prev2*1
    
    long_signal = long_signal_today + long_signal_prev2 
    
    ## Short Signals
    # High price above upper bollinger band
    
    short_filter_today = ohlc[1].iloc[-1] > bollinger_bands[2].iloc[-1]
    short_signal_today = short_filter_today*(1)
    
    # Subtract yesterdays signal from todays or add them together:

    short_filter_prev2 = ohlc[1].iloc[ohlc[1].shape[0]-2] > bollinger_bands[2].iloc[bollinger_bands[2].shape[0]-2]
    short_signal_prev2 = short_filter_prev2*(1)
    
    short_signal = short_signal_today + short_signal_prev2 
    
    # Here: Add signals together: Only generate a signal if today and yesterday had signals
    long_signal = (long_signal == 2)*1
    short_signal = (short_signal == 2)*(1)
    
    
    return long_signal - short_signal
    
    
def get_indicator(indicator,ohlc,parameters):
    
    if(indicator == 'bollinger-bands'): return bollinger_bands(ohlc,parameters)
    if(indicator == 'stochastic'): return stochastic(ohlc,parameters)

def bollinger_bands(ohlc,parameters):
    
    # Lookback period for the middle band (simple moving average)
    n_day_lb = parameters['n_day_lb']
    # K = distance for upper and lower bands from average
    k = parameters['k']
    
    ## N-day SMA of closing prices
    middle_band = ohlc[3].rolling(n_day_lb).mean()
    
    # Upper Band with k times std dev
    upper_band = middle_band + k * ohlc[3].rolling(n_day_lb).std()
    if(ohlc[3].shape[0] > 30):
        upper_band += upper_band*(ohlc[3].iloc[-1] - ohlc[3].iloc[ohlc[3].shape[0]-30])/(ohlc[3].iloc[ohlc[3].shape[0]-30])
    
    # Lower Band with k times std dev
    lower_band = middle_band - k * ohlc[3].rolling(n_day_lb).std()
    if(ohlc[3].shape[0] > 30):
        lower_band += lower_band*(ohlc[3].iloc[-1] - ohlc[3].iloc[ohlc[3].shape[0]-30])/(ohlc[3].iloc[ohlc[3].shape[0]-30])

    
    return [lower_band,middle_band,upper_band]


def stochastic(ohlc,parameters):
    
    
    k_fast = 100*(ohlc[3] - ohlc[3].rolling(14).min())/(
            ohlc[3].rolling(14).max() - ohlc[3].rolling(14).min())
    
    d_fast = k_fast.rolling(3).mean()
    
    full_k = k_fast.rolling(3).mean()
    
    full_d = full_k.rolling(14).mean()
    
    return [full_k, full_d]
    
  
def risk_management(portfolio, signals, ohlc,param):
    
    max_pos_size = 0.2
    
    date = ohlc[3].iloc[-1].name
        
    # Create Target Allocation for New Trading Signals
    
    new_signals = check_existing_boxes(portfolio,ohlc,signals)

    entry_sizes = position_sizer(param[1],new_signals,ohlc,portfolio)*max_pos_size*portfolio.balance[-1]
            
    entry_allocation = (entry_sizes)/ohlc[3].iloc[-1]
    
            
    ## Exit Allocation: Exit Trade when leaving the box horizontally or vertically
    
    # Initialize Exit Allocation DataFrame
    ## Why do we initialize like this?
    ## if we do: time_exit_alloc = portfolio.positions
    # or if we do time_exit_alloc = entry_allocation
    # as we change time_exit_alloc we also change portfolio.positions / entry_allocation
    
    time_exit_alloc = pd.Series(index=portfolio.positions.index, name=date)
    #price_exit_alloc = pd.Series(index=portfolio.positions.index, name=date)
    #exit_alloc = pd.Series(index=portfolio.positions.index, name=date)

    # Calculate Exit Allocation DataFrame
    for ticker in portfolio.boxes:
        #### Time Stop-Loss
        try:
            if(ohlc[3].iloc[-1].name >= portfolio.boxes[ticker]['exit_date']): 
                time_exit_alloc[ticker] = -portfolio.positions[ticker]
                portfolio.boxes[ticker] = {}
            else: time_exit_alloc[ticker] = 0
        except:
            time_exit_alloc[ticker] = 0
                  
        #### Price Stop-Loss
        """
        # Long Positions
        if(portfolio['Positions'][ticker] > 0):
            if(ohlc[3].iloc[-1] < portfolio['Boxes'][ticker]['exit_price']):
                exit_allocation_price[ticker] = -portfolio['Positions'][ticker]
            else: exit_allocation_price[ticker] = 0
            
         # Short Positions
        if(portfolio['Positions'][ticker] < 0):
            if(ohlc[3].iloc[-1] > portfolio['Boxes'][ticker]['exit_price']):
                exit_allocation_price[ticker] = -portfolio['Positions'][ticker]
            else: exit_allocation_price[ticker] = 0
       
        # Final exit allocation if either time or price trigger a stoploss
        if(exit_allocation_time[ticker] != 0 and exit_allocation_price[ticker] != 0):
            if(exit_allocation_time[ticker] == exit_allocation_price[ticker]):
                exit_allocation[ticker] = exit_allocation_price[ticker]
        else: exit_allocation[ticker] = 0
"""           
    
    # Box new trades (generated by new signals): 
    # "Box a Trade in" with Time and Price Stop-Losses
    # Why not doing it at the beginning (where we calculate entry allocation)?
    # For the case of a trade expiring (time-stop-loss) and fresh trade signal 
    # on the same day, boxing it first would override the time-stop-loss, which is 
    # triggered by a pre-existing box.
    # Thus, we would ADD to an existing position without exiting the expired one.
    
    portfolio = box_it(portfolio, ohlc, new_signals,param[0])
    
        
    target_allocation = entry_allocation + time_exit_alloc            
    
    return target_allocation, portfolio


def check_existing_boxes(portfolio, ohlc, signals):
    
    for ticker in signals.index:
        try:
            if(ohlc[3].iloc[-1].name < portfolio.boxes[ticker]['exit_date']): 
                signals[ticker] = 0
        # If no Box exists for a given ticker        
        except:
            pass
        
    return signals

def position_sizer(method,signals,ohlc,portfolio):
    
    if(method == 'step'): return signals
        
    if(method == 'oscilator'): 
        
        # We want to scale generate larger positions as our equity curve 
        # starts to make money.
        
        if(len(portfolio.balance) > 20):
            
            # Max: Global equity curve high
            high = max(portfolio.balance)
            
            # Min: 20 day (lokal) equity curve low
            x_day_low = min(portfolio.balance[-20:])
            
            # For periods where we hold no positions and equity curve stays unchanged 
            if(high == x_day_low): return signals*0.1
            
            # Maintain a miniamal position size if strategy loses
            if((portfolio.balance[-1] - x_day_low)/(high - x_day_low) < 0.1):
                return signals*0.1
            
            else: return signals*(portfolio.balance[-1] - x_day_low)/(high - x_day_low)
        
        # For first 20 days in the backtest          
        else: return signals*0.1
                
            

def box_it(portfolio, ohlc, signals,param):
    
    ## Box in trades with price and time stop-losses.
    # We could also box a trade in with a target price
    
    entry_price = ohlc[3].iloc[-1]
    
    entry_date = ohlc[1].iloc[-1].name
    
    exit_date = ohlc[3].iloc[-1].name + timedelta(param)
    
    # average true range    
    true_range = ohlc[1] - ohlc[2]
    atr = ((true_range).rolling(20).mean()).iloc[-1]
        
    exit_price = ohlc[3].iloc[-1]*(1-signals*atr*2)
            
    for ticker in signals.index:
        if(signals[ticker] != 0):
            portfolio.boxes[ticker]= {'entry_date':entry_date,'exit_date':exit_date,
                     'exit_price':exit_price[ticker],'entry_price':entry_price[ticker] }
        
    return portfolio    
    
    
def execute_target_allocation(portfolio,target_alloc,ohlc,spread):
    
    for ticker in target_alloc.index:
        if(target_alloc[ticker] != 0):
            portfolio.positions[ticker] += target_alloc[ticker]

    
    new_balance = portfolio.balance[-1] - abs((ohlc[3].iloc[-1]*portfolio.positions*spread).sum())
    
    portfolio.balance.append(new_balance)
    
    return portfolio
    

def update_balance(portfolio,ohlc):
    
    new_balance = portfolio.balance[-1] + (portfolio.positions*(
            ohlc[3].iloc[-1] - ohlc[3].iloc[ohlc[3].shape[0]-2])).sum()
    
    portfolio.balance[-1] = new_balance
    
    # Updated Balance gets appended later in the execute_target_allocation function    
    
    return portfolio

  
    
class Portfolio(object):
     
    def __init__(self, ohlc,tickers,data,parameters, strategies):
        self.balance = [100]
        self.positions = get_signals(strategies,[i.iloc[:1] for i in ohlc],data,parameters)*0
        self.boxes = {i:{} for i in tickers}
        

        

    
    
    
    
    
    
    