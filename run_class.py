import pandas as pd 
import MetaTrader5 as mt5

from class_analise import Prices_analises


df1 = Prices_analises() # Start the object

stock = "Bra50"  # Define the Stock

time_frame = mt5.TIMEFRAME_M5 # Define the time frame

bars = 99000 # Define how many bars you want

b = df1.get_symbols() # If you want to check what are the available symbols print this


df1.create_df(stock, time_frame, bars) # Create the Dataframe df_one inside of the class


a = df1.df_one # you can access the data frame to check the informations


df1.clean_df(1) # Clean Df and create a candle range using the variable as a scalar

df1.create_IFR(21) # Create IFT Indicator

df1.create_ROC() # Create ROC indicator

df1.create_OBV() # Create OBV Indicator

df1.create_VWAP(21) # Create VWAP indicator

df1.create_MAA({50 : "close", 200 :"open"}) # Create moving average indicator

#df1.delta_time(60, [ 21,30]) # Set delta_time if you want. if Not have to use filter_hour

a = 50
b = 200


df1.df_one.columns
condicao_1 = (df1.df_one[f'Average close {a} periods'].shift(3) < df1.df_one[f'Average open {b} periods'].shift(3))
condicao_2 = (df1.df_one[f'Average close {a} periods'].shift(2) < df1.df_one[f'Average open {b} periods'].shift(2))
condicao_3 = (df1.df_one[f'Average close {a} periods'].shift(1) < df1.df_one[f'Average open {b} periods'].shift(1))
condicao_4 = (df1.df_one[f'Average close {a} periods'] > df1.df_one[f'Average open {b} periods'])



df1.apply_trade_rules( "buy", condicao_1, condicao_2, condicao_3, condicao_4)

condicao_1 = (df1.df_one[f'Average close {a} periods'].shift(3) > df1.df_one[f'Average open {b} periods'].shift(3))
condicao_2 = (df1.df_one[f'Average close {a} periods'].shift(2) > df1.df_one[f'Average open {b} periods'].shift(2))
condicao_3 = (df1.df_one[f'Average close {a} periods'].shift(1) > df1.df_one[f'Average open {b} periods'].shift(1))
condicao_4 = (df1.df_one[f'Average close {a} periods'] < df1.df_one[f'Average open {b} periods'])

df1.apply_trade_rules( "sell", condicao_1, condicao_2, condicao_3, condicao_4)


"""

condicao_1 = (df1.df_one['ROC'].shift(1) < 0)
condicao_2 = (df1.df_one['ROC'] >= 0)

df1.apply_trade_rules( "buy", condicao_1,condicao_2) # Apply conditions to create a buy data frame


condicao_1 = (df1.df_one['ROC'].shift(1)  > 0)
condicao_2 = (df1.df_one['ROC']  <= 0)

df1.apply_trade_rules( "sell", condicao_1,condicao_2)# Apply conditions to create a sell data frame
"""


df1.filter_hour([15, 00], [ 20,30])


open_hour_limit = pd.Timestamp(2023, 8, 16, 21, 30).time()



"""
'Range target' as like the range function (start, finish, step)

'Range Stop' as like the range function (startm finish, step)

'Position' has 3 options ('buy', 'sell', 'buy/sell')

'risk_return_positive' is a boolean option ( True or False). in the tests of targest and stop you allow be only
positivi risk return (True) or indifferent (False)

'entry' is where is your entry trade, ('close', 'open') 
is used to filter the trades and create the data frames with your entrys

'open_control_position' if you want there to be a waiting period between entries. You can set inside of the code
default is 1 hour

'open_hour_limit' is the limit to open a position 


'day_target' is your stop gain or loss inside of a day have to be a tuple (GAIN, LOSS) in points ex: (100, 200)

'month_limit' is a stop loss or gain stop inside a month. you can consider as a unique value ( the sum of the month) 
or independent, who the strategy hit first  setting as 'separated',(gain, stop, 'both' / 'separated'
                                                                    
'delta_control' if you set delta_cotrol as True the trades that were opened while you had a position will be disregarded "
if you want  to open a position anyway you set as (None type) 

'strategy_name' name that will save you strategy in the DB

'save_db' True or False if you want to connect with Azure Db, if is a different DB you have to adapt the code 

'risk_free' Tax rate  to calculate a Sharpe ratio (it will be calculated annually)

'seed_money' the account balance in points of the asset you are analyzing in order to calculate the percentage return, etc. 

'month_or_year' if year you will receive 

"""



values = {
    'range_target': (500, 800, 50),
    'range_stop': (250, 350, 50),
    'position': "buy/sell",
    'risk_return_positive': True,
    'entry': "close",
    'open_control_position': True,
    'open_hour_limit': open_hour_limit,
    'day_target' : [None, None],
    'month_limit' : [1500, 1500, "both"],
    'delta_control' : True,
    "strategy_name" : "moving average",
    "save_db" : True,
    "risk_free_rate_" : 0.12,
    "seed_money" : 10000,
    "month_or_year" : "year"
    
}

#a = df1.df_analyse_result
testee = df1.df_sell     # if you want to check the sell data frame
test22 = df1.df_buy     # if you want to check the buy data frame


df1.set_stop_target(values)    # start the routine


test = df1.summary        # access the summary result of the strategies





risk_free_rate_ = 0.12
seed_money = 10000
month_or_year = "year"

result = df1.analyze_results(risk_free_rate_, seed_money, month_or_year)



