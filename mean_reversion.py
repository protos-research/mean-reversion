import protos_edge as pe
import pandas as pd
import numpy as np


### Load Data: OHLC for Top 5
tickers = ['bitcoin','ethereum','bitcoin-cash','litecoin','ripple']
start_date = '2015-01-01'
end_date = '2018-07-18'

ohlc = pe.load_ohlc(tickers,start_date,end_date)

strategies = ['mean-reversion']
signal_parameters = {'n_day_lb':6,'k':2.2}
data = []


portfolio = pe.Portfolio(ohlc,tickers,data,signal_parameters, strategies)

# Spread above and below Market Price when buying / selling
spread = 0.00
# [Stop Loss in days, 'oscilator': dynamic position sizing VS 'step':fixed position sizing
risk_mgmt_param = [2,'oscilator']



track_balance = []
dates = []


for day in range(1,ohlc[0].shape[0]):
    
    # Update Balance
    if(day > 1):
        portfolio = pe.update_balance(portfolio,[i.iloc[:day] for i in ohlc])

    
    # Get Signals
    signals = pe.get_signals(strategies,[i.iloc[:day] for i in ohlc],data,signal_parameters)


    # Risk Management --> Target Allocation
    target_alloc, portfolio = pe.risk_management(portfolio,signals,[i.iloc[:day] for i in ohlc],risk_mgmt_param)


    # Execute Target Allocation
    portfolio = pe.execute_target_allocation(portfolio,target_alloc,[i.iloc[:day] for i in ohlc],spread)

    
    # Track returns for Plotting
    track_balance.append(portfolio.balance[-1])
    dates.append(ohlc[3].iloc[day-1].name)
    

balance = pd.DataFrame(track_balance, index=dates)
balance.plot()
returns = balance.pct_change()



print("Balance: " + str(portfolio.balance[-1]))
print("--------------------------------------")
print(portfolio.positions)
print("--------------------------------------")
print("Sharpe: " + str((returns.mean()/(returns.std())*np.sqrt(365)).values))
print("--------------------------------------")
print("Gain-to-Pain : " + str((returns.sum()/abs(returns[returns < 0].sum())).values))
print("--------------------------------------")
print("Max Drawdown : " + str())

