# -*- coding: utf-8 -*-
"""
Created on Thu Dec 28 13:08:59 2023

@author: muril
"""
import pyodbc
import pandas as pd 
import numpy as np 
from datetime import datetime
import time
import numpy as np 
import pandas as pd 
from datetime import datetime
import MetaTrader5 as mt5
import variables_class
import ta
from datetime import timedelta


class Prepare_Df():
    
    def __init__(self):
        self.df_buy = None
        self.df_sell = None

        
    def create_df(self,stock, time_frame,bars):
        
        """ Create the principal data Frame that will be used to run the trades
            You can decide how many bars you want to check setting the 'bars' variable.
        """
        
        today = datetime.today()
        
        self.df_one = mt5.copy_rates_from(stock, time_frame, today, bars) #Use MT5 to access the data
        self.df_one = pd.DataFrame( self.df_one)    
        self.df_one["time"] = pd.to_datetime(self.df_one["time"], unit = "s" )
        self.df_one =  self.df_one[['time', 'open', 'high', 'low', 'close',"tick_volume"]] # Keep only this columns

    def __create_range(self, min_value, max_value,scale):
        
        "Create the candle interval to be more precise in the trades."
        
        return list(range(min_value, max_value + 1,scale)) #Create the candle interval to be more precise
    
    def clean_df(self,scale):
        """
       
        A few checks are made here to ensure that the date types are correct and in sorted.
        Create the range here too. ( If you stock use a differente scale check the functon (__create_range)
                                    
        """
        
        self.df_one['time'] = pd.to_datetime(self.df_one['time'],format='%d/%m/%Y %H:%M')
        self.df_one['Hour'] = self.df_one['time'].dt.time
        self.df_one['time'] = self.df_one['time'].dt.date
        self.df_one.sort_values(['time','Hour'],inplace=True,ascending=True)
        self.df_one.set_index('time',inplace=True)
        self.df_one.dropna(inplace=True)
        self.df_one['close'] =  self.df_one['close'].astype(int)
        self.df_one['high'] =  self.df_one['high'].astype(int)
        self.df_one['low'] =  self.df_one['low'].astype(int)
        self.df_one['open'] =  self.df_one['open'].astype(int)
        self.df_one['tick_volume'] =  self.df_one['tick_volume'].astype(int)
        self.df_one['Candle'] = self.df_one.apply(lambda row: self.__create_range(row['low'], row['high'],scale), axis=1)
        
        
        
    def delta_time(self,time_delta = None, end = None):
        
        """
        Always consider the opening of the day and start your trades with the chosen delta. 
        This ensures that even during daylight saving time or any change in the opening,
        you start from the same distance as the opening. 
        
        You can set the end time trade. 
        
        """
        
        
        if time_delta is not None:
            
            self.df_one["day"] = self.df_one.index
            self.df_one['data_hora'] = pd.to_datetime(self.df_one['day'].astype(str) + ' ' + self.df_one['Hour'].astype(str))
            primeiras_horas = self.df_one.groupby(self.df_one['day']).first()
            self.df_one['diferenca'] = self.df_one['data_hora'] - self.df_one['day'].map(primeiras_horas['data_hora'])
            self.df_one = self.df_one[self.df_one['diferenca'] >= pd.Timedelta(minutes=time_delta)]
            self.df_one.drop(["day",'data_hora','diferenca'],inplace=True,axis=1)
            self.end_hour = pd.Timestamp(year = 2023,month =1,day=1, hour=end[0], minute=end[1]).time()
        

    def create_IFR(self,window : int):
        
        "Create the IFR Indicator with your parameters"
        
        from ta.momentum import RSIIndicator
        rsi = RSIIndicator(self.df_one["close"], window=window)
        self.df_one["RSI"] = rsi.rsi()
        
        
    def create_ROC(self):
        
        "Create the ROC indicator with your parameters"
        
        from ta.momentum import ROCIndicator
        roc = ROCIndicator( self.df_one['close'])
        self.df_one['ROC'] = roc.roc()
            
    def create_SMA(self,window_low : int, window_fast : int):
        
        "Create the SMA indicator with your parameters"
        
        from ta.trend import SMAIndicator
        mms_rapida = SMAIndicator(self.df_one['close'], window=window_low)
        mms_lenta = SMAIndicator(self.df_one['close'], window=window_fast)
        self.df_one['MM_FAST'] = mms_rapida.sma_indicator()
        self.df_one['MM_LOW'] = mms_lenta.sma_indicator()
        

    def create_OBV(self):
        
        "Create the OBV indicator with your parameters"
        
        from ta.volume import OnBalanceVolumeIndicator
        obv = OnBalanceVolumeIndicator(close=self.df_one.close, volume=self.df_one.tick_volume)
        self.df_one['OBV'] = obv.on_balance_volume()
        
     
    def create_VWAP(self,window : int):
        
        "Create the VWAP indicator with your parameters"
        
        from ta.volume import VolumeWeightedAveragePrice
        vwap = VolumeWeightedAveragePrice(high=self.df_one.high, low=self.df_one.low, 
                                          close=self.df_one.close, volume=self.df_one.tick_volume, window=window)
        self.df_one['VWAP'] = vwap.volume_weighted_average_price()
         

    def create_MAA(self,averages : dict ):
        
        "Create the MAA indicator with your parameters"
        
        assert type(averages) is dict , """averages should be dict {average, colum},
                                    {7, 'close'}
                                    """
        for average,colum in averages.items() :
            self.df_one[f"Average {colum} {average} periods"] = self.df_one[colum].rolling(average).mean()


    def apply_trade_rules(self,signal,*rules):
        
        """
            Apply trade rules will check if you have more than one condition that has to be 
            used together, for example if you need to search for a condition on more than one candle.
           
            here 2 data frames are created df_buy and df_sell you you want to trade in the both sides.
        """
        
        
        combined_condition  = None
        
        for condition in rules:
            if combined_condition is None:
                combined_condition = condition
            else:
                combined_condition &= condition
        
        if combined_condition is not None:
            if signal == "buy":
                self.df_buy = self.df_one[combined_condition].copy()
                self.df_buy.loc[:, "signal"] = signal
                
                
            elif signal == "sell":
               self.df_sell= self.df_one[combined_condition].copy()
               self.df_sell.loc[:, "signal"] = signal
            
            
        else:
            raise ValueError("No conditions were specified to apply.")
        
    def filter_hour(self,start,end):
        
        """
        if you want to filter the possible trades in a specific period
        """
     
        
    
        self.start_hour = pd.Timestamp(year = 2023,month= 1,day =1, hour=start[0], minute=start[1]).time()
        self.end_hour = pd.Timestamp(year = 2023,month =1,day=1, hour=end[0], minute=end[1]).time()
        self.df_sell = self.df_sell[(self.df_sell["Hour"] > self.start_hour) & (self.df_sell["Hour"] < self.end_hour)]
        self.df_buy = self.df_buy[(self.df_buy["Hour"] > self.start_hour) & (self.df_buy["Hour"] < self.end_hour)]
        
        
        



class training_parametes(Prepare_Df):
    
    """
    Here is the core of the code. 
    will interact across all lines in the possibles trades and create the results. 
    
    """
    
    
    def check_variable_types(self, variables: dict):
        
        expected_types = {
            'range_target': tuple,
            'range_stop': tuple,
            'position': str,
            'risk_return_positive': bool,
            'entry': str,
            'open_control_position': bool
            
        }
        
        for var_name, var_type in expected_types.items():
            if not isinstance(variables.get(var_name), var_type):
                raise ValueError(f"{var_name} must be of the type {var_type.__name__}")

            
          
    def __stop_target_routine(self,possible_target,possible_stop,position,entry,open_control_position,
                              open_hour_limit, day_target = (None, None), month_limit= None, df_results = None,
                              delta_control = None):
        
        
        "SET these two variables to iniciace the code. They will save the value of entry time to be used after"
        entry_buy = pd.Timestamp(2023, 8, 16, 0, 1).time()
        entry_sell = pd.Timestamp(2023, 8, 16, 0, 1).time()
        
        "save the sequence of results"
        sequence_result = []
        
        
        "create this dictionary to be a Data Frame for future analysis"
        
        self.close_hour = {"day" : [],
                           "hour_entry" : [],
                           "hour_out"  : [],
                           "results" : [],
                           "signal" : []}
        
        
        "check control to build the dataframe with the operations"
        if position == "buy/sell" :
            self.df_all_operations = pd.concat([self.df_buy,self.df_sell])
            self.df_all_operations = self.df_all_operations.sort_values(["time","Hour"])
            
            self.df_trades = self.df_all_operations.copy()
            
        elif position == "sell":
            
            self.df_trades = self.df_sell.copy()

        elif position == "buy":
            
            self.df_trades = self.df_buy.copy()
                 
        "set the variables used to control day stop loss and gain stop "         
        gain_day, stop_day = day_target
        count_gain_day = 0
        count_stop_day = 0
        
        "set the variables used to control month stop loss and gain stop "  
        month_limit_gain, month_limit_loss , union = month_limit
        month_limit_gain_count = 0
        month_limit_loss_count = 0
        month_limit = 0
        
        "set the variables with the first day, month and hour to be revalued in the future"
        day_check =  pd.to_datetime(self.df_trades.iloc[0].name).day
        month_check = pd.to_datetime(self.df_trades.iloc[0].name).month
        hour_close = self.df_trades["Hour"][0]
        
        
        "Here's the start of the cycle in open negotiations"
        for index, row_1 in self.df_trades.iterrows():
            
            
            df_daily= self.df_one.loc[index]
            day = index
            month = index.month
            time_entry = row_1['Hour']
            signal = row_1["signal"]
            trade = df_daily[df_daily['Hour'] == time_entry][f"{entry}"]
            hour_open = row_1["Hour"] 
           
            
            "create the targets and stop"
            if signal == "buy":
                
                target = trade + possible_target
                
                stop = trade+(possible_stop*-1)
            
            elif signal == "sell":
                target = trade + (possible_target*-1)
                
                stop = trade + possible_stop
            
            
            "consider the stop and gain targets separately or together"
            if union == "separated":
                
                if (month_limit_gain_count >= month_limit_gain and month_check == month) or \
                 ( month_limit_loss_count >= month_limit_loss and month_check == month):
                    continue
                
            elif union == "both":    
                
                if (month_limit >= month_limit_gain and month_check == month) or \
                    (month_limit <= -month_limit_gain and month_check == month):
                    continue
                
            "reset the monthly target"    
            if month_check != month:
                month_limit_gain_count = 0
                month_limit_loss_count = 0
                month_limit = 0
                
            "reset the day target"      
            if day != day_check:
                count_gain_day = 0
                count_stop_day = 0
            
            "if you set it to use the daily target "
            if gain_day is not None :
              
                if (count_gain_day >= gain_day) and day == day_check:
                    continue
                
            "if you set it to use the daily stop "    
            if stop_day is not None:
                
                if (count_stop_day >= stop_day) and day == day_check:
                    continue
                
                
            
                
                
            "transform the times to be used and controlled in the difference"
           
            data_atual = datetime.now().date()    
            datetime_time_entry = datetime.combine(data_atual, time_entry)
            datetime_entry_buy = datetime.combine(data_atual, entry_buy)
            datetime_entry_sell = datetime.combine(data_atual, entry_sell)
            #hour_open = datetime.combine(data_atual, hour_open)
            #hour_close = datetime.combine(data_atual, hour_close)
            
            
            "if you set delta_cotrol as True the trades that were opened while you had a position will be disregarded "
            if delta_control is not None:
                
                if hour_open < hour_close and day == day_check:
                    continue
                
            "control time open"   
            if time_entry >= open_hour_limit:
                
                continue 
            
            
            "control the space between the trade in both positions"
            if open_control_position == True and day == day_check:
               
                
                if signal == "buy" and datetime_time_entry - datetime_entry_buy < pd.Timedelta(hours=1) :
                    
                    continue
                
                elif signal == "sell" and datetime_time_entry - datetime_entry_sell < pd.Timedelta(hours=1):
                    
                    continue
                    
                              
                    
            day_check =  day
            month_check = month
            
            "filter for all candles after the entry time "
            trade_sequence = df_daily[df_daily["Hour"] > time_entry]
            
            
            if signal == "buy":
                entry_buy = time_entry
                
            elif signal == "sell":
                entry_sell = time_entry
           
            "saves the information from each operation "
            for idx, row in trade_sequence.iterrows():
                if stop.isin(row['Candle']).any():
                    self.close_hour["day"].append(day)
                    self.close_hour["hour_entry"].append(time_entry)
                    self.close_hour["hour_out"].append(row["Hour"])
                    self.close_hour["results"].append(possible_stop*-1)
                    self.close_hour["signal"].append(signal)
                    sequence_result.append(possible_stop*-1)
                    count_stop_day += possible_stop  
                    month_limit_loss_count += possible_stop
                    hour_close = row["Hour"]
                    month_limit -= possible_stop
                    break
                
                elif target.isin(row['Candle']).any():
                    self.close_hour["day"].append(day)
                    self.close_hour["hour_entry"].append(time_entry)
                    self.close_hour["hour_out"].append(row["Hour"])
                    self.close_hour["results"].append(possible_target)
                    self.close_hour["signal"].append(signal)
                    sequence_result.append(possible_target)
                    count_gain_day += possible_target 
                    month_limit_gain_count += possible_target 
                    hour_close = row["Hour"]
                    month_limit += possible_target
                    break
               
                
                elif row["Hour"] >= self.end_hour:
                    if signal == "buy" and row["close"] > trade[0]:
                        self.close_hour["day"].append(day)
                        self.close_hour["hour_entry"].append(time_entry)
                        self.close_hour["hour_out"].append(row["Hour"])
                        self.close_hour["results"].append(row["close"]-trade[0])
                        self.close_hour["signal"].append(signal)
                        sequence_result.append(row["close"]-trade[0])
                        count_gain_day += row["close"]-trade[0]
                        month_limit_gain_count += row["close"]-trade[0]
                        hour_close = row["Hour"]
                        month_limit += row["close"]-trade[0]
                        break
                    
                    elif signal == "buy" and row["close"] < trade[0]:
                        self.close_hour["day"].append(day)
                        self.close_hour["hour_entry"].append(time_entry)
                        self.close_hour["hour_out"].append(row["Hour"])
                        self.close_hour["results"].append((row["close"]-trade[0])*-1)
                        self.close_hour["signal"].append(signal)
                        sequence_result.append(trade[0]-row["close"])
                        count_stop_day += trade[0]-row["close"]  
                        month_limit_loss_count += trade[0]-row["close"] 
                        hour_close = row["Hour"]
                        month_limit += trade[0]-row["close"]  
                        break
                    
                    elif signal == "sell" and row["close"] < trade[0]:
                        self.close_hour["day"].append(day)
                        self.close_hour["hour_entry"].append(time_entry)
                        self.close_hour["hour_out"].append(row["Hour"])  
                        self.close_hour["results"].append(trade[0]-row["close"])
                        self.close_hour["signal"].append(signal)
                        sequence_result.append(trade[0]-row["close"])
                        count_gain_day += trade[0]-row["close"]
                        month_limit_gain_count += trade[0]-row["close"]
                        hour_close = row["Hour"]
                        month_limit += trade[0]-row["close"]
                        break
                    
                    elif signal == "sell" and row["close"] > trade[0]:
                        self.close_hour["day"].append(day)
                        self.close_hour["hour_entry"].append(time_entry)
                        self.close_hour["hour_out"].append(row["Hour"])
                        self.close_hour["results"].append((trade[0]-row["close"])*-1)
                        self.close_hour["signal"].append(signal)
                        sequence_result.append(row["close"]-trade[0])
                        count_stop_day += row["close"]-trade[0]  
                        month_limit_loss_count += row["close"]-trade[0]
                        hour_close = row["Hour"]
                        month_limit += row["close"]-trade[0]
                        break
        
        
        self.total_gain = sum(self.close_hour["results"])
        
        "saves each dataframe for each set of options"
        df_results.append(self.close_hour)
        
        
        print("result " , self.total_gain, "stop ", possible_stop,"gain ", possible_target)
        
        self.summary["target"].append(possible_target)
        self.summary["stop"].append(possible_stop*-1)
        self.summary["total"].append(self.total_gain )
        self.summary["sequence_result"].append(sequence_result)
    
                
                
    def set_stop_target(self,variables : dict):
        
        
        self.check_variable_types(variables)
        
        
        range_target = variables.get('range_target')
        range_stop = variables.get('range_stop')
        position = variables.get('position')
        risk_return_positive = variables.get('risk_return_positive')
        entry = variables.get('entry')
        open_control_position = variables.get('open_control_position')
        open_hour_limit = variables.get('open_hour_limit')
        day_target = variables.get('day_target')
        month_limit = variables.get("month_limit")
        delta_control = variables.get("delta_control")         
        strategy_name = variables.get("strategy_name")
        save_db = variables.get("save_db")    
        risk_free_rate_ = variables.get("risk_free_rate_")
        seed_money = variables.get("seed_money")
        month_or_year = variables.get("month_or_year")
        
        
        self.summary = {'target' : [] , "stop" : [], "total" : [], 
                       "sequence_result" : []}
        
        self.setup = {"target" : [], "stop" : [] ,  "position" : [],
                      "risk_return_positive" : [], "entry" : [] ,"open_control_position" : [],
                      "open_hour_limit" : [], "day_target" : [], "month_limit" : [], 
                      "delta_control" : [], "strategy_name" : []}
        
        
        targets = [int(x) for x in range(*range_target)]
        stops = [int(x) for x in range(*range_stop)]

        
           
            
        for possible_target in targets:
            for possible_stop in stops:
                
                #controlamos se o risco retorno do target é de pelo menos 1 positivo
                if risk_return_positive == True and possible_target/(possible_stop) <1:
                    
                    continue
                
                else:
                    
                    self.df_results = []
                    if save_db == False:
                        self.__stop_target_routine(possible_target,possible_stop,position,entry,open_control_position,
                                                   open_hour_limit,day_target,month_limit,self.df_results, delta_control)
            
                        
                        self.setup["target"].append(possible_target)
                        self.setup["stop"].append(possible_stop)
                        self.setup["position"].append(position)
                        self.setup["risk_return_positive"].append(str(risk_return_positive))
                        self.setup["entry"].append(entry)
                        self.setup["open_control_position"].append(str(open_control_position))
                        self.setup["open_hour_limit"].append(str(open_hour_limit))
                        self.setup["day_target"].append(str(day_target))
                        self.setup["month_limit"].append(str(month_limit))
                        self.setup["delta_control"].append(str(delta_control))
                        self.setup["strategy_name"].append(strategy_name)
                    
                    elif save_db == True:
                        self.df_results = []
                        self.setup = {"target" : [], "stop" : [] ,  "position" : [],
                                      "risk_return_positive" : [], "entry" : [] ,"open_control_position" : [],
                                      "open_hour_limit" : [], "day_target" : [], "month_limit" : [], 
                                      "delta_control" : [], "strategy_name" : []}
                        self.__stop_target_routine(possible_target,possible_stop,position,entry,open_control_position,
                                                   open_hour_limit,day_target,month_limit,self.df_results, delta_control)
            
                        
                        self.setup["target"].append(possible_target)
                        self.setup["stop"].append(possible_stop)
                        self.setup["position"].append(position)
                        self.setup["risk_return_positive"].append(str(risk_return_positive))
                        self.setup["entry"].append(entry)
                        self.setup["open_control_position"].append(str(open_control_position))
                        self.setup["open_hour_limit"].append(str(open_hour_limit))
                        self.setup["day_target"].append(str(day_target))
                        self.setup["month_limit"].append(str(month_limit))
                        self.setup["delta_control"].append(str(delta_control))
                        self.setup["strategy_name"].append(strategy_name)
                        self.df_analyse_result = self.analyze_results(risk_free_rate_, seed_money, month_or_year)
                        self.export_db()
        
    
class Prices_analises(training_parametes):
    
    
    def __init__(self):
        #Initialize the connection 
        self.df_one = None
        self.inicialize = mt5.initialize()
        
    
    def get_symbols(self):
        #if necessary you can ask to check all the available symbols
        
        self.symbols_dict = {}
        symbols = mt5.symbols_get()     
        for symbol in symbols:
            self.symbols_dict[symbol.name] = symbol.path
        return  self.symbols_dict    
        
    def get_time_frames(self):
        # check for all availables time frames
        return variables_class.times


    def __calculate_sharpe_ratio(self,returns, risk_free_rate):     
        returns = returns.copy()
        returns["tax_reference"] = risk_free_rate    
        sharpe_ratio = (returns["percentagem_return"] - returns["tax_reference"]) / returns["std_result"]    
        return sharpe_ratio



    def analyze_results(self,risk_free_rate_,seed_money,month_or_year ):
        """
        will interact with all dataframes to create news data about the results
        
        """
        
        
        
        count_df = 0
        all_dfs = []
        
        "will interact with everyone in the ta frames "
        for df in self.df_results:
            
            "convert to data frame"
            df_result_dataframe = pd.DataFrame(df)
            df_result_dataframe["day"] = pd.to_datetime(df_result_dataframe["day"])
            
            
            total_trades = len(df_result_dataframe["results"])
            gains = len(df_result_dataframe[df_result_dataframe["results"] > 0])
            loss = len(df_result_dataframe[df_result_dataframe["results"] < 0])
            
            
            win_percent = round(gains/total_trades,3)*100
            
            risk_return = abs(df_result_dataframe[df_result_dataframe["results"] > 0]["results"].mean() \
                    / df_result_dataframe[df_result_dataframe["results"] < 0]["results"].mean())
            
            "possible set if per month or year "    
            if month_or_year == "month" :
                
                df_result_dataframe[f"{month_or_year}"] = df_result_dataframe["day"].dt.month  
                
            elif   month_or_year == "year":  
                
                df_result_dataframe[f"{month_or_year}"] = df_result_dataframe["day"].dt.year  
                
            year_result = df_result_dataframe.groupby(f"{month_or_year}").agg({"results" : ["sum" ,"std"]})
            year_result.columns = ['sum_result', 'std_result']

            df_result_dataframe["cum_sum"] = (df_result_dataframe["results"]).cumsum()
            
            "check the maximum downgrade and the maximum distance to recover from that downgrade "
            
            pick_value = -999999
            drawdown = 0
            
            
            day_pick1 = df_result_dataframe.iloc[0]["day"]
            control_diff = {"diff" : timedelta(days=1), "day_start" : None, "day_finish" : None}
            
            for idx, value in df_result_dataframe.iterrows()  :
                
                if value["cum_sum"] > pick_value:
                    
                    pick_value = value["cum_sum"] 
                    
                    
                    
                    
                    if control_diff["diff"] < ( value["day"] - day_pick1) :
                        
                        control_diff["diff"] = ( value["day"] - day_pick1)
                        control_diff["day_start"] = day_pick1
                        control_diff["day_finish"] = value["day"] 
                       
                        
                    day_pick1 = value["day"]     
                        
                    
                elif idx ==   df_result_dataframe.iloc[len(df_result_dataframe)-1].name  :
                    
                    if control_diff["diff"] < ( value["day"] - day_pick1) :
                        control_diff["diff"] = ( value["day"] - day_pick1)
                        control_diff["day_start"] = day_pick1
                        control_diff["day_finish"] = value["day"] 
                        
                        
                        
                    
                else:
                    dif_pick = pick_value - value["cum_sum"]
                    
                    if dif_pick > drawdown:
                        drawdown = dif_pick    

                   

            "risk free rate inserted by the user"
            risk_free_rate = risk_free_rate_
            
            "transform the result in point to percentage considering the user's input value"
            year_result["percentagem_return"] =( year_result["sum_result"]/seed_money) *100   
            
            "calculate sharpe ratio"
            sharpe = self.__calculate_sharpe_ratio(year_result,risk_free_rate)
           
            
            year_result["sharpe_value"] =sharpe
       
            
            year_result["idx_df"] = count_df
            year_result["win_percent"] = win_percent
            year_result["risk_return"] = risk_return
            year_result["drawdown"] = drawdown
            year_result["max_days"] = control_diff["diff"]
            year_result["day_start"] = control_diff["day_start"]
            year_result["day_finish"] = control_diff["day_finish"]
            
            count_df +=1
    
            all_dfs.append(year_result)
        
        all_dfs = pd.concat(all_dfs)

        return all_dfs





    def export_db(self):
        
        server = ''
        database = ''
        username = ''
        password = ''
        driver= '{ODBC Driver 17 for SQL Server}'  # Este driver pode variar dependendo do seu ambiente

        # Crie uma conexão
        conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+password)

        cur = conn.cursor()
        
        
        
        sql_insert = ("INSERT INTO tb_ct_versions(set_target,set_loss,position,\
                      return_positive,entry_,open_control_position,open_hour_limit,day_target,\
                          month_limit,delta_control,strategy_name) VALUES (?,?,?,?,?,?,?,?,?,?,?)")
        
        
        check_last_version = ("select top 1 version_table from tb_ct_versions order by version_table DESC")   
               
        sql_insert_TB_TRADES_SIGNALS = ("INSERT INTO TB_TRADES_SIGNALS(version_table,open_,high,low,close_,tick_volume,\
                                        hour_,rsi,roc,obv,vwap,average_1, average2, signal, day_)\
                                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")    
                                        
        sql_insert_tb_trades_realized = ("INSERT INTO tb_trades_realized(version_table,day_,hour_entry,hour_out,results,signal,cumsum)\
                                        VALUES (?,?,?,?,?,?,?)")   
                                        
        sql_insert_tb_analyse_results = ("INSERT INTO tb_analyse_results(version_table,sum_result,std_result,\
                                         percentage_return,sharpe_value,win_percent,risk_return,drawdown_max,max_days,day_start,day_finish,grouped_date)\
                                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)")    

        
        
        
        i=0
        
        while i < len(self.setup["target"]):
            lista_insert = []
            
            for key in self.setup.keys():
                lista_insert.append(self.setup[key][i])
            cur.execute(sql_insert,lista_insert ) 
            conn.commit()
            cur.execute(check_last_version)     
            res = cur.fetchall() 
            version = res[0][0]  
            
            ciclo_save = self.df_trades.drop("Candle",axis=1)
            
            for idx, value in ciclo_save.iterrows():
                idx = str(idx)
                value["Hour"] = str(value["Hour"])
                value["Hour"] = str(value["Hour"])
                value["VWAP"] =  round(value["VWAP"],3)
                value = value.to_list()
                value.insert(0,version)
                value.append(idx)
                cur.execute(sql_insert_TB_TRADES_SIGNALS, value) 
                conn.commit()
            
            results_df = pd.DataFrame(self.df_results[0])
            
            results_df["cumsum"] = results_df["results"].cumsum()
            for idx, value in results_df.iterrows():

                value["day"] = str(value["day"])
                value["hour_entry"] = str(value["hour_entry"])
                value["hour_out"] =  str(value["hour_out"])
                
                value = value.to_list()
                value.insert(0,version)

                cur.execute(sql_insert_tb_trades_realized, value) 
                conn.commit()
                
            df_analyses =  self.df_analyse_result.copy()
            df_analyses = df_analyses.drop("idx_df",axis=1)
            
            for idx, value in df_analyses.iterrows():
                idx = str(idx)
                value["max_days"] = str(value["max_days"] )
                value["day_start"] = str(value["day_start"])
                value["day_finish"] =  str(value["day_finish"])
                
                
                value = value.to_list()
                value.insert(0,version)
                value.append(idx)
                cur.execute(sql_insert_tb_analyse_results, value) 
                conn.commit()
                
            
            i += 1    
            
        print("saved")
            
            

"------------------------------------------------------------------------------------------"




