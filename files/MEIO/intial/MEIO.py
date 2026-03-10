# -*- coding: utf-8 -*-
"""
Created on Thu Jul 20 12:44:21 2023

@author: BrunoZindy
"""
import pandas as pds
import math;
#import logging;
#from Optimization.IO.z_conversion import ZConversion;
from Optimization.IO.Distributions import Distribution as dist;
from Optimization.IO.targetDictionary import targetDictionary;
from string import Template
#from scipy.stats import poisson , norm
import time
#import numpy
#import dask.dataframe as dd
#import dask
#from dask import delayed
import datetime
from dateutil import relativedelta
today = datetime.date.today()
#from scipy.stats import poisson , norm
#import numpy as np
from scipy.special import ndtri
import numpy as np



large_quantity=99999999

#logging.basicConfig(filename='c:\temp\MEIO.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')

# the 0 fill rate assumption may be slightly wrong


class MEIO:
    def __init__(self, dbConnection, alchemy_engine, trace_Logger, basic_Logger, consider_eoq=True, line_fill_rate=True, precisionJump=0):
        
        np.seterr(all='warn', over='call')

        self.basicLogger=basic_Logger
        self.traceLogger=trace_Logger
        
        self.longJumpMEO=True if precisionJump>0 else False
        self.bigJump=.95
        self.fillRateIncrement=precisionJump
        self.ASLjump=True
        self.distribution_threshold=25

        self.dbConnection    = dbConnection
        self.alchemyEngine=alchemy_engine
        self.schema_custo="custom"
        self.MEO_output_table="io_sku_output"
        self.MEO_logger_table="io_logger_output"
        self.MEO_output_group="io_group_output"
        self.loggerDF=pds.DataFrame(columns = ['timestamp', 'indexrow', 'group_achieved_fill_rate', 'new_group_fill_rate', 'group_direct_demand_rate', 'sku_direct_line_demand_rate', 'sku_fill_rate_increase', 'unit_cost', 'committed_buffer', 'new_buffer', 'current_sku_fill_rate', 'new_fill_rate', 'group_achieved_budget', 'row'])
        self.target_dictionary=targetDictionary(dbConnection=dbConnection, line_fill_rate=line_fill_rate, traceLogger=self.traceLogger, basicLogger=self.basicLogger, meioPointer=self,  dfLogger=self.loggerDF)
        #self.basicLogger.warning("MEIO - init - self.target_dictionary " + str(self.target_dictionary) )
        self.st = time.time()
        self.relativeTime = time.time()
        self.consider_eoq=consider_eoq
        self.line_fill_rate=line_fill_rate

        
        self.sql_read_attributes_select="""select 
        att.item_id, att.site_id, att.total_demand_rate/""" +  ("att.avgsize" if self.line_fill_rate else "1")  +""" total_demand_rate , att.direct_demand_rate/""" +  ("att.avgsize" if self.line_fill_rate else "1")  +""" direct_demand_rate , """ + ("att.avgsize" if self.line_fill_rate else "1")  + " avgsize  ," + ("att.eoq" if self.consider_eoq else "1") + """ eoq, att.repl_site_ids, att.leg_lead_time, 0 committed_buffer, -1.0 new_buffer
, case when skucount > $DistributionThreshod then 'Normal' else 'Poisson' end distribution,  0.0 current_fill_rate , att.unit_cost, 0 initial_asl_quantity, att.total_lead_time
, """ +  ("att.line_stddev" if self.line_fill_rate else " att.qty_stddev ")  +""" dmd_stddev
, stddev_lt, total_fcst_monthly/""" +  ("att.avgsize" if self.line_fill_rate else "1")  +"""  total_fcst_monthly , on_hand
, -1.0 new_fill_rate
, null dependant_changes, att.j_target_groups
, att.group_participation, 0.0 marginal_value, null scenario_id, att.wait_time, att.sku_max_fill_rate, att.sku_min_fill_rate, att.sku_max_sl_qty, att.sku_min_sl_qty, att.sku_max_sl_slices, att.sku_min_sl_slices, att.use_existing_inventory, least (coalesce(att.sku_max_fill_rate,0), att.sku_min_fill_rate) sku_tgt_fillrate, att.components, att.kits, att.wait_time current_wait_time 
 , att.indirect_demand_rate/""" +  ("att.avgsize" if self.line_fill_rate else "1")  +""" indirect_demand_rate, skucount
 , false sku_init_set
 , parent_ids
, case when skucount > $DistributionThreshod then 1 else 0 end normal_dist
, case when skucount > $DistributionThreshod then 0 else 1 end poisson_dist 
, dmd_coefficient_of_variation
, varcoeff_max covCap """
        
        
        self.sql_read_attributes=self.sql_read_attributes_select + """
,  mad
 from custom.IO_sku_attributes att where leg_lead_time>0 
 and att.total_demand_rate>0 """
 
    def  err_handler(self, type, flag):
        self.basicLogger.warning("err_handler: error -" + str(type) + "- flag: " + str(flag)  )
 
    def capStandardDeviation (self, hist_std_dev, monthly_forecast, coefficient_of_variation_cap, lead_time_stddev, lead_time):
        self.basicLogger.warning("capStandardDeviation - monthly_forecast - " + str(monthly_forecast) + "- coefficient_of_variation_cap - " + str(coefficient_of_variation_cap)  + "- hist_std_dev - " + str(hist_std_dev)  )
        cappedStandardDeviation_dmd=min(hist_std_dev, monthly_forecast*coefficient_of_variation_cap)
        cappedStandardDeviation_dmd=max(cappedStandardDeviation_dmd, monthly_forecast)
        
        
        cappedStandardDeviation_lt=min(lead_time_stddev, lead_time*coefficient_of_variation_cap)
        cappedStandardDeviation_lt=max(cappedStandardDeviation_lt, lead_time)
        self.basicLogger.warning("capStandardDeviation - monthly_forecast - " + str(monthly_forecast) + "- hist_std_dev - " + str(hist_std_dev)  + "- lead_time_stddev - " + str(lead_time_stddev)  + "- monthly_forecast - " + str(monthly_forecast)  + "- coefficient_of_variation_cap - " + str(coefficient_of_variation_cap)  + "- lead_time_stddev - " + str(lead_time_stddev)  + "- lead_time - " + str(lead_time)  + "- cappedStandardDeviation_dmd - " + str(cappedStandardDeviation_dmd)  + "- cappedStandardDeviation_lt - " + str(cappedStandardDeviation_lt)  )

        return (cappedStandardDeviation_dmd, cappedStandardDeviation_lt)
        
 
    def applySkuMin(self, forecast_ovr_lt, eoq, distribution, avg_size, lead_time, unit_cost, tgt_min_rop_qty, tgt_max_rop_qty, sku_max_fill_rate, sku_min_fill_rate, lead_time_stdev, dmd_stddev, mad):
        tested_fill_rate=0
        returned_fill_rate=0
        startingROPlineQty=math.ceil(tgt_min_rop_qty/avg_size) 
        returned_buffer=startingROPlineQty*avg_size
        
        self.basicLogger.warning("applySkuMin - eoq - " + str(eoq) + "- avg_size - " + str(avg_size)  + "- forecast_ovr_lt - " + str(forecast_ovr_lt) + "- tested_fill_rate: " + str(tested_fill_rate) + "- startingROPlineQty: " + str(startingROPlineQty)  + "- tgt_max_rop_qty: " + str(tgt_max_rop_qty)+ "- tgt_min_rop_qty: " + str(tgt_min_rop_qty) + "- dmd_stddev: " + str(dmd_stddev)  + "- distribution: " + str(distribution) + "- returned_buffer: " + str(returned_buffer) + "- sku_min_fill_rate: " + str(sku_min_fill_rate)  )
        counter=0
        while(True):
            tested_fill_rate=MEIO.new_fill_rate_calc(self, returned_buffer, forecast_ovr_lt, eoq, distribution, avg_size, lead_time, lead_time_stdev, dmd_stddev, mad)
            self.basicLogger.warning("applySkuMin - tested_fill_rate: " + str(tested_fill_rate) + "- returned_buffer: " + str(returned_buffer) + "- counter: " + str(counter)   )

            if tested_fill_rate>sku_min_fill_rate or returned_buffer>tgt_max_rop_qty :
                if counter==0:
                    returned_fill_rate=tested_fill_rate
                break
            else:
                returned_buffer += avg_size
                returned_fill_rate=tested_fill_rate
                counter+=1
        
        self.basicLogger.warning("applySkuMin - returned_buffer - " + str(returned_buffer) + "- returned_fill_rate - " + str(returned_fill_rate)   )

        return (returned_buffer, returned_fill_rate)

 
    def initialJump(self, forecast_ovr_lt, eoq, distribution, avg_size, lead_time, unit_cost, tgt_max_rop_qty, sku_max_fill_rate, lead_time_stdev, dmd_stddev, mad):
        
        self.basicLogger.warning("initialJump - eoq - " + str(eoq) + "- avg_size - " + str(avg_size)  + "- forecast_ovr_lt - " + str(forecast_ovr_lt)  + "- tgt_max_rop_qty: " + str(tgt_max_rop_qty) + "- dmd_stddev: " + str(dmd_stddev) )
        
        # the first step will be at least avgsize or up to fcstlt-eoq/2
        returned_buffer=max(avg_size,math.ceil(forecast_ovr_lt*avg_size - math.ceil(eoq/2)))
        returned_fill_rate=MEIO.new_fill_rate_calc(self, returned_buffer, forecast_ovr_lt, eoq, distribution, avg_size, lead_time, lead_time_stdev, dmd_stddev, mad)
        
        self.basicLogger.warning("initialJump - returned_buffer: " + str(returned_buffer) + "- returned_fill_rate: " + str(returned_fill_rate)   )

        return (returned_buffer, returned_fill_rate)
 
        
    def group_completion_for_sku (self,  sku_direct_demand_rate, sku_new_fill_rate, sku_current_fill_rate, sku_new_buffer, sku_current_buffer, unit_cost, j_target_groups, sku_group_participation, sku_max_fill_rate, sku_investment):
        
        group_marginal_gain=0
        total_marginal_gain=0
        sku_fill_rate_increase=(sku_new_fill_rate - sku_current_fill_rate)
        #sku_investment=unit_cost*(sku_new_buffer-sku_current_buffer)
                    
        # ASSESS GROUP FILL
        all_groups_filled=True
        if j_target_groups is not None:
            for target_group in j_target_groups:
            # 1- estimate fill rate increase usng the total_forecast of the sku as its weight
                #self.basicLogger.warning("MarginalValue - target_group['io_tgt_group'] - "  + str(target_group['io_tgt_group']) )
                tgt_group_name=target_group['io_tgt_group']
                group_completed, group_fill_rate_tgt, group_achieved_fill_rate, group_total_demand_rate, group_direct_demand_rate, group_max_budget_tgt, group_achieved_budget=self.target_dictionary.currentGroupValues(tgt_group_name )
          
                # we check that not all targets were hit
                                
                if (not group_completed):
                    new_group_fill_rate=(group_achieved_fill_rate*group_direct_demand_rate + sku_direct_demand_rate*sku_fill_rate_increase)/group_direct_demand_rate
                    group_fill_rate_increase=new_group_fill_rate-group_achieved_fill_rate
                    group_marginal_gain=group_fill_rate_increase*(group_direct_demand_rate - (sku_direct_demand_rate* (sku_group_participation-1)/sku_group_participation))
                    total_marginal_gain+=group_marginal_gain
                    
                    self.basicLogger.warning("group_completion_for_sku -  " +  str(tgt_group_name) + "- group_achieved_fill_rate : " + str(group_achieved_fill_rate) + "- group_achieved_budget : " + str(group_achieved_budget) + "- group_max_budget_tgt : " + str(group_max_budget_tgt)   + " - group_marginal_gain : " + str(group_marginal_gain) + " - total_marginal_gain : " + str(total_marginal_gain)  + " - sku_max_fill_rate : " + str(sku_max_fill_rate) + " - group_direct_demand_rate : " + str(group_direct_demand_rate) + " - sku_direct_demand_rate : " + str(sku_direct_demand_rate)  + " - sku_new_fill_rate : " + str(sku_new_fill_rate) + " - sku_current_fill_rate : " + str(sku_current_fill_rate)+ " - sku_fill_rate_increase : " + str(sku_fill_rate_increase) + " - sku_group_participation : " + str(sku_group_participation)+ " - sku_investment : " + str(sku_investment) + " - new_group_fill_rate : " + str(new_group_fill_rate) + " - group_fill_rate_increase : " + str(group_fill_rate_increase)  ) 
                    all_groups_filled=False        
                    
        # We are looking for the improvement for each group
        # 2- weigh each group increase with total_fcst/number of groups+current demand_rate in the group
        # 3- add marginal value of each group
        # this part should not be excluded if the min fr was not reached
        # all other mins are reached immediately at the beginning (min qty)
        # the min fill rate depends on other pars 
        # so we will actually fill the remaining fill rates at the very end as a post process, skipping the complete MEO choices
        # only if we are really above max should we exclude the part with no marginal value
        # this is in particular to take into account cases where we consider a new fill rate of 1
        if all_groups_filled or sku_new_fill_rate>sku_max_fill_rate:
            total_marginal_gain=0
            
        return (total_marginal_gain)



    def EffectiveLtFcst (self, initial_total_demand_rate, initial_direct_demand_rate, leg_lead_time, parent_wait_time):
        
        #self.basicLogger.warning("*comm*EffectiveLtFcst - initial_total_demand_rate -  " +  str(initial_total_demand_rate) + "- initial_direct_demand_rate : " + str(initial_direct_demand_rate) + "- leg_lead_time : " + str(leg_lead_time)  + " - parent_wait_time : "  + str(parent_wait_time)  ) 

        total_lead_time=leg_lead_time + parent_wait_time
        effective_total_lt_fcst=initial_total_demand_rate*total_lead_time
        effective_direct_lt_fcst=initial_direct_demand_rate*total_lead_time
        
        
        self.basicLogger.warning("EffectiveLtFcst - effective_direct_lt_fcst :  " +  str(effective_direct_lt_fcst)  + "- total_lead_time : " + str(total_lead_time) + "- initial_total_demand_rate : " +  str(initial_total_demand_rate) + "- initial_direct_demand_rate : " + str(initial_direct_demand_rate) + "- leg_lead_time : " + str(leg_lead_time)  + " - parent_wait_time : "  + str(parent_wait_time)   + " - effective_total_lt_fcst : "  + str(effective_total_lt_fcst)   ) 

        return (effective_total_lt_fcst, effective_direct_lt_fcst)
        
    def Replenishingloclt (self, total_wait_time, new_fill_rate):
        return total_wait_time*(1-new_fill_rate)

    def kit_probabilistic_waiting_Lt(self, dependantkitIndex, modif_component_id, new_fill_rate):
        #this relates to the 
        skuPointer=self.maindataframe
        #kit_item_id=dependantkitIndex[0]
        kit_site_id=dependantkitIndex[1]
        #self.basicLogger.warning("???kit_probabilistic_waiting_Lt - kit_item_id -  " +  str(kit_item_id) + "- kit_site_id : " + str(kit_site_id)   + "- dependantkitIndex : " + str(dependantkitIndex) + "- new_fill_rate : " + str(new_fill_rate)   ) 

        kit_depending_on_component_modified_row=skuPointer.loc[dependantkitIndex]
        
        components_list=kit_depending_on_component_modified_row[32]
        #self.basicLogger.warning("kit_probabilistic_waiting_Lt - components_list -  " +  str(components_list) + "- kit_depending_on_component_modified_row -  \n" +  str(kit_depending_on_component_modified_row) + " - dependantkitIndex -  " +  str(dependantkitIndex)  ) 

        #self.basicLogger.warning("kitProbabilisticLt -  components_list " + str(components_list) + " - \n map: " + str(map(lambda x: list([x,kit_site_id]), components_list ))+ " - index: " + str(skuPointer.index.isin(map(lambda x: list([x,kit_site_id]), components_list ) )) )

        components_df=skuPointer[skuPointer.index.isin(map(lambda x: list([x,kit_site_id]), components_list ) ) ].sort_values(by=['leg_lead_time'])
        #P_0=components_df[current_fill_rate].group(by=None).prod(False)
        #self.basicLogger.warning("kit_probabilistic_waiting_Lt - components_df -  \n" +  str(components_df)  ) 

        overall_lt_weighted=0
        for index, component_row in components_df.iterrows():        
            #self.basicLogger.warning("kit_probabilistic_waiting_Lt - component_row - " +  str(component_row)  ) 

            current_comp_prob_lt=1
            #self.basicLogger.warning("kitProbabilisticLt - components_df: \n" + str(components_df) + " - component: \n" + str(component) )
            current_comp_leg_lt=component_row[5]

            components_higher_lt_df=components_df[components_df.leg_lead_time>current_comp_leg_lt]
            #self.basicLogger.warning("kit_probabilistic_waiting_Lt - component_row1 - " +  str(component_row)  ) 

            #index[0] is the item_id
            current_comp_prob_lt*=(1-new_fill_rate) if (index[0]==modif_component_id) else (1-component_row[9])
            #self.basicLogger.warning("kit_probabilistic_waiting_Lt - component_row2 - \n" +  str(component_row[9]) + " - components_higher_lt_df : " + str(components_higher_lt_df) ) 

            for index_high, component_high_row in components_higher_lt_df.iterrows():
                #self.basicLogger.warning("???kit_probabilistic_waiting_Lt -  new_fill_rate - " +  str(new_fill_rate) + "- component_high_row[9] - " +  str(component_high_row[9])  ) 
                current_comp_prob_lt*=new_fill_rate if (index_high[0]==modif_component_id) else component_high_row[9]

            weighted_lt=current_comp_prob_lt*current_comp_leg_lt
            overall_lt_weighted+=weighted_lt
            
        #self.basicLogger.warning("kit_probabilistic_waiting_Lt kit_item_id " + str(kit_item_id) + " - kit_site_id " + str(kit_site_id)+ " - component modified : " + str(modif_component_id)+ " - component modified FR : " + str(new_fill_rate)+ " - component overall_lt_weighted FR : " + str(overall_lt_weighted) )

        return (overall_lt_weighted)
            
    
    def commit_logged_value (self, indexrow, commitInit=False ):
        self.basicLogger.warning("commit_logged_value - indexrow " + str(indexrow) )
        recompute_marginal_value_for_all_skus=False
        dfSkuPointer=self.maindataframe
        row=dfSkuPointer.loc[indexrow]
        #self.basicLogger.warning("commit_logged_value - row :\n" + str(row) )

        # 1-first we commit the top record
        # 1.1- get the targets we want to commit
        #total_fcst=row[0]

        total_demand_rate=row[0]
        direct_demand_rate=row[1]
        #avgsize=row[2]
        leg_lead_time=row[5]
        committed_buffer=row[6]
        new_buffer=row[7]
        current_sku_fill_rate=row[9]
        unit_cost=row[10]
        new_sku_fill_rate=row[17]
        
        list_of_dependant_changes=row[18]
        j_target_groups=row[19]
        
        group_participation=row[18]
        mv=row[19]
        marginal_value=row[21]

        #sku_fill_rate_increase=new_sku_fill_rate-current_sku_fill_rate
        #sku_budget_increase=(new_buffer-committed_buffer)* unit_cost
        self.basicLogger.warning("commit_logged_value - marginal_value " + str(marginal_value) + " - commitInit " + str(commitInit) + " - group_participation " + str(group_participation) + " - mv " + str(mv)  )

        if marginal_value==0 and not commitInit:
            # we did not achieve goals but have to stop here, nothing to be done anymore.
            return True

        # 1.2- commit the target at top level
        dfSkuPointer.at[indexrow, 'committed_buffer']=new_buffer
        dfSkuPointer.at[indexrow, 'current_fill_rate']=new_sku_fill_rate
        #self.basicLogger.warning("????commit_logged_value - dfSkuPointer.loc[indexrow] " + str(dfSkuPointer.loc[indexrow])  )


        #self.basicLogger.warning("============*comm* commit_logged_value MAIN" + str(indexrow) +"  - MARGINAL_VALUE: " + str(marginal_value) + " - new_sku_fill_rate: " + str(new_sku_fill_rate)   +" - current_sku_fill_rate: " + str(current_sku_fill_rate)   +"  - new_buffer: " + str(new_buffer)  + "  - committed_buffer: " + str(committed_buffer) + "  - direct_demand_rate: " + str(direct_demand_rate)+ "  - list_of_dependant_changes: " + str(list_of_dependant_changes) + " - current_fill_rate: " + str(row[9]) ) 
        
        sku_completed, all_completed=self.target_dictionary.commit_groups (j_target_groups, new_sku_fill_rate, current_sku_fill_rate,  direct_demand_rate, dfSkuPointer, str(indexrow)   , new_buffer, committed_buffer, unit_cost, row)
        
        
        self.basicLogger.warning("commit_logged_value in main " + str(indexrow) + " - sku_completed: " + str(sku_completed)+ " - all_completed: " + str(all_completed) )

        if not commitInit:
            if sku_completed:
                # set marginal value
                dfSkuPointer.at[indexrow, 'marginal_value']=-large_quantity
                if all_completed:
                    return True
            else: 
                recompute_marginal_value_for_all_skus=True
                
                
            #self.basicLogger.warning("????commit_logged_value - list_of_dependant_changes " + str(list_of_dependant_changes) + "type " + str(type(list_of_dependant_changes))+ "test " + str(not list_of_dependant_changes) )
            
            #testing the emptiness of the list
            if list_of_dependant_changes:
            
                for dependant_change in list_of_dependant_changes:
                    #self.basicLogger.warning("*comm* commit_logged_value - dependant_change " + str(dependant_change) )
                    if dependant_change:
    
                        indexrow_change=dependant_change[0]
                        #self.basicLogger.warning("????commit_logged_value - type(dependant_change) -" + str(type(dependant_change)) )
                        #self.basicLogger.warning("????commit_logged_value - indexrow_change " + str(indexrow_change) )
        
                        # getting the old values. Some will be overriden by the committed ones
                        dependant_row=dfSkuPointer.loc[indexrow_change]
                        
                        total_demand_rate=dependant_row[0]
                        direct_demand_rate=dependant_row[1]
                        leg_lead_time=dependant_row[5]
                        
                        #dep_committed_buffer=dependant_row[6]
                        #dep_new_buffer=dependant_row[7]
                        #initial_wait_time=dependant_row[23]
                        current_sku_fill_rate=dependant_row[9]
                        j_target_groups=dependant_row[19]
                        current_wait_time=dependant_row[34]
    
                        new_sku_fill_rate=dependant_change[1]
                        new_wait_time=dependant_change[2]
                        dfSkuPointer.at[indexrow_change,'current_fill_rate']=new_sku_fill_rate
                        #then we reset the fill rate for the next setup
                        #dfSkuPointer.at[indexrow_change,'new_fill_rate']=0
                        
                        dfSkuPointer.at[indexrow_change,'current_wait_time']=new_wait_time
                        
                        
                        effective_total_lt_fcst, effective_direct_lt_fcst=MEIO.EffectiveLtFcst(self, total_demand_rate, direct_demand_rate, leg_lead_time, new_wait_time)
                        # then we increase accordingly the groups targets
    
                        #sku_fill_rate_increase=new_sku_fill_rate-current_sku_fill_rate
                        self.basicLogger.warning("============*comm* commit_logged_value - DEPENDANTS: " + str(indexrow) + " - " + str(indexrow_change)  + " - effective_direct_lt_fcst: " + str(effective_direct_lt_fcst) + " - leg_lead_time: " + str(leg_lead_time)+ " - current_wait_time: " + str(current_wait_time) + " - new_wait_time: " + str(new_wait_time) + " - new_sku_fill_rate: " + str(new_sku_fill_rate) + " - current_sku_fill_rate: " + str(current_sku_fill_rate)   )
    
                        sku_completed, all_completed=self.target_dictionary.commit_groups (j_target_groups, new_sku_fill_rate, current_sku_fill_rate,  direct_demand_rate, dfSkuPointer, str(indexrow_change) + " - " + str(indexrow)   , new_buffer, committed_buffer, 0, dependant_row)
    
    
                        if sku_completed:
                            # set marginal value
                            dfSkuPointer.at[indexrow_change, 'marginal_value']=0
                            if all_completed:
                                return True
                        else: 
                            recompute_marginal_value_for_all_skus=True
            
                            
            #here we want to recompute teh marginal value for all skus inspected, taking into account all committed changes in both main and dependant skus
    
                 
            if recompute_marginal_value_for_all_skus==True:
                self.basicLogger.warning("commit_logged_value - MAIN + dependants committed, recompute the full stack " + str(indexrow) )
                #self.basicLogger.warning("????commit_logged_value - dfSkuPointer.loc[indexrow] " + str(dfSkuPointer.loc[indexrow])  )
                if list_of_dependant_changes:
                
                    for dependant_change in list_of_dependant_changes:
                        #self.basicLogger.warning("*comm* commit_logged_value - dependant_change " + str(dependant_change) )
                        if dependant_change:
                            indexrow_change=dependant_change[0]
                            #self.basicLogger.warning("????commit_logged_value - type(dependant_change) -" + str(type(dependant_change)) )
                            self.basicLogger.warning("????commit_logged_value - recompute marginal value for Dependants " + str(indexrow_change) )
            
                            # getting the old values. Some will be overriden by the committed ones
    
                            marginal_value, dependant_changes_list = MEIO.MarginalValue(self, indexrow_change, indexrow_change, None, None)
    
                MEIO.upperWayMarginalValue(self, indexrow)
                        
        return False

    def further_jump(self, current_fill_rate, committed_buffer, eoq,  effective_total__line_lt_fcst, avgsize, total_lead_time, dmd_stddev, mad, lead_time_stdev, distribution):
        demandVariation=0
        leadTimeDeviationPortion=0
        denominator=0
        leadTimeMonth=0
        
        #self.basicLogger.warning("further_jump- big jump0: - current_fill_rate: " + str(current_fill_rate) + " - eoq: " + str(eoq)  + " - committed_buffer: " + str(committed_buffer)  + " - effective_total__line_lt_fcst: " + str(effective_total__line_lt_fcst)  + " - avgsize: " + str(avgsize)   + " - total_lead_time: " + str(total_lead_time)  + " - dmd_stddev: " + str(dmd_stddev)  + " - mad: " + str(mad) + " - lead_time_stdev: " + str(lead_time_stdev) + " - self.bigJump: " + str(self.bigJump) + " - total_lead_time: " + str(total_lead_time) + " - leadTimeDeviationPortion: " + str(leadTimeDeviationPortion)   )

        if current_fill_rate<self.bigJump and self.longJumpMEO:
            leadTimeMonth=total_lead_time/30
            dailyRate=effective_total__line_lt_fcst/total_lead_time
            leadTimeDeviationPortion=((dailyRate) * (0 if np.isnan(lead_time_stdev) else lead_time_stdev) )**2 
            
            
            if np.isnan(mad) and (np.isnan(dmd_stddev) or dmd_stddev==0) or (distribution=='Poisson'):
                # here we shall rever to a poisson distribution
                #newBuffer=dist.normal_distribution_FR2ROP  (self, current_fill_rate+self.fillRateIncrement, total_lead_time, effective_total__line_lt_fcst, avgsize, lead_time_stdev, dmd_stddev)
                newBuffer=dist.poisson_distribution_FR2ROP (self, current_fill_rate+self.fillRateIncrement, eoq, effective_total__line_lt_fcst, avgsize)
                nextJump=newBuffer-committed_buffer
                
                nextJump=max(avgsize, nextJump)
                nextJump=math.ceil(nextJump/avgsize)*avgsize
                
                self.basicLogger.warning("further_jump- small jump poisson: - current_fill_rate: " + str(current_fill_rate) + " - eoq: " + str(eoq)  + " - committed_buffer: " + str(committed_buffer)  + " - effective_total__line_lt_fcst: " + str(effective_total__line_lt_fcst)  + " - avgsize: " + str(avgsize)   + " - total_lead_time: " + str(total_lead_time)  + " - dmd_stddev: " + str(dmd_stddev)  + " - mad: " + str(mad) + " - lead_time_stdev: " + str(lead_time_stdev) + " - self.bigJump: " + str(self.bigJump) + " - total_lead_time: " + str(total_lead_time) + " - leadTimeDeviationPortion: " + str(leadTimeDeviationPortion) + " - leadTimeMonth: " + str(leadTimeMonth)  + " - leadTimeDeviationPortion: " + str(leadTimeDeviationPortion)   + " - nextJump: " + str(nextJump) + " - mad: " + str(mad) + " - distribution: " + str(distribution)  )
                
            else:
                # GOLDEN FORMULA
                if mad is None or np.isnan(mad) :
                    demandVariation= dmd_stddev
                else :
                    demandVariation= 1.25* mad* effective_total__line_lt_fcst * math.sqrt(1/leadTimeMonth)
                
                demandVariationPortion= math.sqrt(demandVariation**2  *total_lead_time)
                racine=math.sqrt(demandVariationPortion + leadTimeDeviationPortion)
                zscore=ndtri(current_fill_rate+self.fillRateIncrement)
                newLineSafetyStock=zscore*racine
                #nextJump=(newLineSafetyStock+effective_total__line_lt_fcst)-committed_buffer
                nextJump=newLineSafetyStock+effective_total__line_lt_fcst*avgsize-committed_buffer
                
                nextJump=max(avgsize, nextJump)
                nextJump=math.ceil(nextJump/avgsize)*avgsize
                
                self.basicLogger.warning("further_jump- big jump1: - current_fill_rate: " + str(current_fill_rate) + " - eoq: " + str(eoq)  + " - committed_buffer: " + str(committed_buffer)  + " - effective_total__line_lt_fcst: " + str(effective_total__line_lt_fcst)  + " - avgsize: " + str(avgsize)   + " - total_lead_time: " + str(total_lead_time)  + " - dmd_stddev: " + str(dmd_stddev)  + " - mad: " + str(mad) + " - lead_time_stdev: " + str(lead_time_stdev) + " - self.bigJump: " + str(self.bigJump) + " - total_lead_time: " + str(total_lead_time) + " - leadTimeDeviationPortion: " + str(leadTimeDeviationPortion) + " - leadTimeMonth: " + str(leadTimeMonth)  + " - leadTimeDeviationPortion: " + str(leadTimeDeviationPortion)  + " - demandVariation: " + str(demandVariation)  + " - racine: " + str(racine)  + " - nextJump: " + str(nextJump)  + " - zscore: " + str(zscore)  + " - newLineSafetyStock: " + str(newLineSafetyStock)   )
        
        else:
            nextJump=avgsize
            self.basicLogger.warning("further_jump- small jump3 (avgsize) : - current_fill_rate: " + str(current_fill_rate) + " - eoq: " + str(eoq)  + " - committed_buffer: " + str(committed_buffer)  + " - effective_total__line_lt_fcst: " + str(effective_total__line_lt_fcst)  + " - avgsize: " + str(avgsize)   + " - total_lead_time: " + str(total_lead_time)  + " - dmd_stddev: " + str(dmd_stddev)  + " - mad: " + str(mad) + " - lead_time_stdev: " + str(lead_time_stdev) + " - self.bigJump: " + str(self.bigJump) + " - total_lead_time: " + str(total_lead_time) + " - leadTimeDeviationPortion: " + str(leadTimeDeviationPortion) + " - leadTimeMonth: " + str(leadTimeMonth)  + " - leadTimeDeviationPortion: " + str(leadTimeDeviationPortion)  + " - demandVariation: " + str(demandVariation)  + " - denominator: " + str(denominator)  + " - nextJump: " + str(nextJump)  )
            
        return nextJump


    
    def new_fill_rate_calc(self, buffer, forecast_ovr_lt, eoq, distribution, avg_size, lead_time, lead_time_stdev, dmd_stddev, mad):
        
        #self.basicLogger.warning("new_fill_rate_calc - buffer: " + str(buffer) + " - eoq: " + str(eoq)  + " - forecast_ovr_lt: " + str(forecast_ovr_lt)  + " - avg_size: " + str(avg_size) + " - distribution: " + str(distribution)  + " - lead_time_stdev: " + str(lead_time_stdev)  + " - dmd_stddev: " + str(dmd_stddev)  )

        if (distribution=="Normal"):
            # normal_distribution (ROP, lead_time, lead_time_stdev, avg_dmd, avg_dmd_stddev)
            #return MEIO.normal_distribution(self, buffer, lead_time, total_fcst_monthly, avg_size, lead_time_stdev, dmd_stddev)
            #return dist.normal_distribution_ROP2FR(self, buffer, eoq, lead_time, forecast_ovr_lt, avg_size, lead_time_stdev, dmd_stddev)
            return dist.golden_formula_ROP2FR(self, buffer, eoq, lead_time, forecast_ovr_lt, avg_size, lead_time_stdev, dmd_stddev, mad)
        else:
            #poisson_distribution (rop, eoq, mean, avgsize):
            return dist.poisson_distribution_ROP2FR (self, buffer, eoq, forecast_ovr_lt, avg_size)
        
    def upperWayMarginalValue (self, indexrow):
        #Look for components and for each
            #upperWayMarginalValue
        #Look for upper location and for each
            #upperWayMarginalValue
        try:
            row=self.maindataframe.loc[indexrow]
           
            item_id=indexrow[0]
            site_id=indexrow[1]
            components_list=row[32]
            parent_site_ids=row[38]
            self.basicLogger.warning("MEIO - upperWayMarginalValue - Main - "  + str(indexrow) + " -components_list: " + str(components_list) + " -parent_site_ids: " + str(parent_site_ids) )
    
            if components_list is not None:
                for component in components_list:
                    dependantIndex=tuple([component, site_id])
                    self.basicLogger.warning("MEIO - upperWayMarginalValue - components_list - "  + str(dependantIndex) + " - " + str(indexrow) )
                    MEIO.upperWayMarginalValue (self, dependantIndex)
                    
            if parent_site_ids is not None:
                for parent_site_id in parent_site_ids:
                    dependantIndex=tuple([item_id, parent_site_id])
                    self.basicLogger.warning("MEIO - upperWayMarginalValue - parent_site_ids - "  + str(dependantIndex) + " - " + str(indexrow) )
                    MEIO.upperWayMarginalValue (self, dependantIndex)
            
    
            MEIO.MarginalValue(self, indexrow, indexrow, None, None)
        except KeyError:
            #self.basicLogger.warning("MarginalValue - KEY NOT FOUND - dfSkuPointer: "  + str(dfSkuPointer))
            self.basicLogger.warning("upperWayMarginalValue - KEY NOT FOUND - indexrow: "  + str(indexrow)  )
    
    def minMaxROPQty(self, sku_max_sl_slices, large_quantity, initial_total_demand_rate, avgsize, sku_max_sl_qty, sku_min_sl_slices, use_existing_inventory, on_hand, sku_min_sl_qty, effective_total_lt_fcst):
        #input to the minmax rop quantities: 
        # sku_max_sl_slices, large_quantity, initial_total_demand_rate, avgsize
        # sku_max_sl_qty, sku_min_sl_slices, use_existing_inventory, on_hand
        # sku_min_sl_qty
        
        if (sku_max_sl_slices!=large_quantity and sku_max_sl_slices>0) :
            maxSlicesDayDuration= ((today+ relativedelta.relativedelta(months=1+sku_max_sl_slices, day=1)) - (today + relativedelta.relativedelta(months=1, day=1))).days
            sku_max_sl_slices_qty=maxSlicesDayDuration*initial_total_demand_rate*avgsize
        else:
            sku_max_sl_slices_qty=large_quantity
        max_sl_qty=min(sku_max_sl_qty, sku_max_sl_slices_qty)
        tgt_max_sl_qty=max_sl_qty
            
        if sku_min_sl_slices>0:
            minSlicesDayDuration= ((today +relativedelta.relativedelta(months=1+sku_min_sl_slices, day=1)) - (today + relativedelta.relativedelta(months=1, day=1))).days
            sku_min_sl_slices_qty=minSlicesDayDuration*initial_total_demand_rate*avgsize
        else:
            sku_min_sl_slices_qty=0
        onhand_min_qty=on_hand if use_existing_inventory else 0
        min_sl_qty=max(sku_min_sl_qty, onhand_min_qty, sku_min_sl_slices_qty)
        # the min has to be capped by the max
        tgt_min_sl_qty=min_sl_qty if min_sl_qty<max_sl_qty else max_sl_qty
        #somehow pushing this back for better tracking

        tgt_min_rop_qty=(tgt_min_sl_qty +effective_total_lt_fcst) if (tgt_min_sl_qty>0) else 0
        tgt_max_rop_qty=(tgt_max_sl_qty +effective_total_lt_fcst) if (tgt_max_sl_qty!=large_quantity) else large_quantity

        return (tgt_min_rop_qty, tgt_max_rop_qty)
        
    
    def MarginalValue (self, indexrow, mainindexrow, main_investment, override_wait_time):
        # when computing gain for sku, we should not commit the changes, it only gets committed if chosen
        # (1) - We compute the gain (FR) for the given sku (as being the increase of an avg size to the buffer)
        # (3) - The gain is calculated
        # (2) - On base of this, we recompute the lead time for the dependants
        #       for the location hierarchy, lead time dependant= leglt(dep)*(FR parent) + totalLt(parent) (1-FR parent)
        #self.basicLogger.warning("MEIO - MarginalValue - type - "  + str(type(dfSkuPointer)))
        #self.basicLogger.warning("MEIO - MarginalValue - type - "  + str(type(indexrow)))

        changes_at_this_level=[]
        
        dfSkuPointer=self.maindataframe

        #self.basicLogger.warning("************MarginalValue - parent_wait_time - "  + str(parent_wait_time))
        #self.basicLogger.warning("MarginalValue - dfSkuPointer - "  + str(dfSkuPointer))
        if (indexrow==mainindexrow):
            self.basicLogger.warning("MARGINALVALUE - MAIN - "  + str(indexrow) + " - " + str(mainindexrow)+ " - override_wait_time: " + str(override_wait_time)  )
        else:
            self.basicLogger.warning("MarginalValue - SUB - "  + str(indexrow) + " - " + str(mainindexrow)+ " - override_wait_time: " + str(override_wait_time) )

        #self.basicLogger.warning("MEIO - MarginalValue - dfSkuPointer.loc[indexrow] - "  + str(dfSkuPointer.loc[indexrow]))
        
        item_id=indexrow[0]
        site_id=indexrow[1]
        
        self.basicLogger.warning("MEIO - items intials: " +  str(item_id)+ " - "  +  str(site_id))

        row=dfSkuPointer.loc[indexrow]
 
        
        self.basicLogger.warning("MarginalValue - row - "  + str(row) + "\n // " +str(type(row)) )

        initial_total_demand_rate=row.iloc[0]
        initial_direct_demand_rate=row.iloc[1]
        avgsize=row.iloc[2]
        eoq=row.iloc[3]
        repl_site_ids=row.iloc[4]
        leg_lead_time=row.iloc[5]
        committed_buffer=row.iloc[6]
        new_buffer=row.iloc[7]
        distribution=row.iloc[8]
        current_fill_rate=row.iloc[9]
        unit_cost=row.iloc[10]
        #initial_asl_quantity=row[11]
        total_lead_time=row.iloc[12]
        dmd_stddev=row.iloc[13]
        lt_stddev=row.iloc[14]
        #total_fcst_monthly=row[15]
        on_hand=row.iloc[16]
        #bware, new fill rate should not be assigned, it is a pointer, else we will modify it in all cases
        #also new fill rate
        #new_fill_rate=row[17]
        #dependant_changes=row[18]
        j_target_groups=row.iloc[19]
        sku_group_participation=row.iloc[20]
        #initial_parent_wait_time=row[23]
        sku_max_fill_rate=row.iloc[24]
        #sku_min_fill_rate=row[25]
        sku_max_sl_qty=row.iloc[26]
        sku_min_sl_qty=row.iloc[27]
        
        sku_max_sl_slices=row.iloc[28]
        sku_min_sl_slices=row.iloc[29]
                
        use_existing_inventory=row.iloc[30]
        #sku_tgt_fillrate=row[31]
        #components_list=row[32]
        kits_list=row.iloc[33]
        current_wait_time=row.iloc[34]
        #sku_init_set=row[37]
        #coefficient_variation=row[41]
        coefficient_variation_cap=row.iloc[42]

        mad=row.iloc[43]
        #mad=row[44]
        
        wait_time=(override_wait_time if override_wait_time is not None else current_wait_time)
        effective_total_lt_fcst, effective_direct_lt_fcst=MEIO.EffectiveLtFcst(self, initial_total_demand_rate, initial_direct_demand_rate,  leg_lead_time, wait_time)
        #dmd_stddev=MEIO.capStandardDeviation( self, effective_total_lt_fcst, coefficient_variation)
        dfSkuPointer.at[indexrow, 'dmd_stddev']=dmd_stddev

        marginal_gain=0
        new_sku_fill_rate=0
        marginal_value=0
        #self.basicLogger.warning("MarginalValue - sku_tgt_qty " + str(sku_tgt_qty) + " - avgsize " + str(avgsize) + " - avgsize " + str(avgsize) )

        # 1- we find how many avgsizes the sku_tgt_qty target represents
        # since it will immediately be increased in the loop, we remove 1
        # but this all cannot go under 0 thus the max       
        dependant_changes_list=[]
        new_tested_buffer=-1
        dmd_stddev, lt_stddev=MEIO.capStandardDeviation(self, float(dmd_stddev), 30.0 * float(initial_total_demand_rate +  initial_direct_demand_rate), float(coefficient_variation_cap), float(lt_stddev), float(leg_lead_time + wait_time) )
        #self.basicLogger.warning("????MarginalValue - minquantity2 " + str(indexrow) + " - " + str(mainindexrow) + " - sku_min_sl_slices_qty: " + str(sku_min_sl_slices_qty)+ " - effective_total_lt_fcst: " + str(effective_total_lt_fcst)+ " - tgt_min_sl_qty: " + str(tgt_min_sl_qty)+ " - min_sl_qty: " + str(min_sl_qty)+ " - max_sl_qty: " + str(max_sl_qty)+ " - sku_max_sl_slices_qty: " + str(sku_max_sl_slices_qty)+ " - tgt_min_rop_qty: " + str(tgt_min_rop_qty)+ " - wait_time: " + str(wait_time) + " - current_wait_time: " + str(current_wait_time) + " - dmd_stddev: " + str(dmd_stddev)  + " - initial_total_demand_rate: " + str(initial_total_demand_rate)  )
        
        #self.basicLogger.warning("????MarginalValue - minquantity3 " + str(indexrow) + " - " + str(mainindexrow) + " - tgt_max_sl_qty: " + str(tgt_max_sl_qty)+ " - effective_total_lt_fcst: " + str(effective_total_lt_fcst)+ " - sku_max_sl_slices_qty: " + str(sku_max_sl_slices_qty)+ " - minSlicesDayDuration: " + str(minSlicesDayDuration)+ " - maxSlicesDayDuration: " + str(maxSlicesDayDuration)+ " - tgt_max_rop_qty: " + str(tgt_max_rop_qty)+ " - max_sl_qty: " + str(max_sl_qty)+ " - sku_max_sl_qty: " + str(sku_max_sl_qty)+ " - sku_min_sl_slices_qty: " + str(sku_min_sl_slices_qty) )

        #at first i am going with a simple linear ratio
        #adding an avgsize to the buffer and assessing the new fill rate
        #ASL loop, we need to find the first addition to the stocks that moves the marginal value needle
        #self.basicLogger.warning("MarginalValue - before if " + str(indexrow) + " - " + str(mainindexrow) + " - committed_buffer: " + str(committed_buffer)+ " - tgt_max_rop_qty: " + str(tgt_max_rop_qty)+ " - sku_max_fill_rate: " + str(sku_max_fill_rate)+ " - sku_min_fill_rate: " + str(sku_min_fill_rate)+ " - tgt_min_rop_qty: " + str(tgt_min_rop_qty)+ " - sku_init_set: " + str(sku_init_set))
        #self.basicLogger.warning ("1- " + str( indexrow)  +  str((indexrow==mainindexrow) and (committed_buffer==0) and (tgt_min_rop_qty>0 or sku_min_fill_rate>0) and (sku_init_set == False)))
        # check whether we are looking at the main parts and that there is a minimum to take into account INDEPENDENTLY OF THE MARGINAL VALUE
        if  (indexrow==mainindexrow and committed_buffer==0 ):
            # SET ASL
            tgt_min_rop_qty, tgt_max_rop_qty = MEIO.minMaxROPQty(self, sku_max_sl_slices, large_quantity, initial_total_demand_rate, avgsize, sku_max_sl_qty, sku_min_sl_slices, use_existing_inventory, on_hand, sku_min_sl_qty, effective_total_lt_fcst)

            # here we are going to apply all sku constraints that should apply immmediatly
            # min aggregated is greatest of min
            best_initial_buffer, best_initial_fill_rate=MEIO.initialJump(self, effective_total_lt_fcst,  eoq, distribution, int(avgsize), total_lead_time, unit_cost, tgt_max_rop_qty, sku_max_fill_rate,  lt_stddev, dmd_stddev, mad )
            new_tested_buffer=best_initial_buffer            
            #main_investment=(new_tested_buffer+eoq/2)*unit_cost
            main_investment=(new_tested_buffer)*unit_cost

            new_sku_fill_rate=best_initial_fill_rate
            self.basicLogger.warning("MarginalValue - MAIN ASL " + str(indexrow) + " - " + str(mainindexrow) + " - best_initial_buffer: " + str(best_initial_buffer) + " - best_initial_fill_rate: " + str(best_initial_fill_rate)+ " - effective_total_lt_fcst: " + str(effective_total_lt_fcst)+ " - eoq: " + str(eoq)+ " - avgsize: " + str(avgsize)+ " - unit_cost: " + str(unit_cost) )

        elif indexrow==mainindexrow and (committed_buffer>0 ):
            # here this is no case of ASL, we are jumt going one above the committed sku
            # in some cases, we are here because we had to recompute the part, which was pending ASL boost (newbuffer was set to fcstlt)
            # in such case, we will just reevaluate on the base of the newbuffer and we do not increase it

            nextJump=MEIO.further_jump(self, current_fill_rate, committed_buffer, eoq,  effective_total_lt_fcst, avgsize, total_lead_time, dmd_stddev, mad , lt_stddev, distribution)
            
            new_tested_buffer=max(new_buffer, committed_buffer+nextJump)
            self.basicLogger.warning("MarginalValue - MAIN NOT ASL " + str(indexrow) + " - " + str(mainindexrow) + " - new_tested_buffer: " + str(new_tested_buffer) + " - committed_buffer: " + str(committed_buffer)+ " - nextJump: " + str(nextJump) + " - avgsize: " + str(avgsize)  )

        else:
            # DEPENDANTS!!
            # this is not the main part, the tested buffer basically remains as the committed buffer
            new_tested_buffer=committed_buffer
            # if we are in a dependant sku that does not bring marginal value we do not want to repeat the loop as we are not changing the bugger for this and nothing will change.
            self.basicLogger.warning("MarginalValue - DEPENDANTS  " + str(indexrow) + " - " + str(mainindexrow) + "- new_tested_buffer: " + str(new_tested_buffer)  )
                
        if main_investment is None:
            main_investment= (new_tested_buffer-committed_buffer) * unit_cost
        
        self.basicLogger.warning("MarginalValue before new_fill_rate_calc : " + str(indexrow) + " - " + str(mainindexrow) + " - main_investment: " + str(main_investment) + " - new_tested_buffer: " + str(new_tested_buffer) + " - new_sku_fill_rate: " + str(new_sku_fill_rate) + " - effective_total_lt_fcst: " + str(effective_total_lt_fcst) + " - eoq: " + str(eoq) + " - distribution: " + str(distribution)  )



        #=========
        #SKU LEVEL
        #=========
        if not ( new_sku_fill_rate>0):
            new_sku_fill_rate= MEIO.new_fill_rate_calc (self, new_tested_buffer, effective_total_lt_fcst,  eoq, distribution, avgsize, total_lead_time, lt_stddev, dmd_stddev, mad )
        
        
        # gain as being increase of fill rate 
        self.basicLogger.warning("MarginalValue - SKU RESULTS " + str(indexrow) + " - " + str(mainindexrow) +  " - committed_buffer: " + str(committed_buffer) + " - tested buffer: "  + str(new_tested_buffer) + " - NEW_SKU_FILL_RATE: " + str(new_sku_fill_rate) + "- current_fill_rate: " + str(current_fill_rate)  + " - main_investment: " + str(main_investment)  + " - avgsize: " + str(avgsize) + " -  effective_direct_lt_fcst: " + str(effective_direct_lt_fcst))

        # HERE I NEED TO ENSURE THAT I CAN FIND THE POINTER TO THE DEPENDANT SKU VERY EASILY
        #self.basicLogger.warning("MarginalValue - marginal_value: " + str(marginal_value) + " - repl_site_ids: " + str(repl_site_ids))
        self.basicLogger.warning("MarginalValue - checking DEPENDANTS(kits/repl)- " + str(indexrow) + " - " + str(mainindexrow) + " -   repl_site_ids: " + str(repl_site_ids)+ " -  kits_list - " + str(kits_list))
        
        marginal_gain = MEIO.group_completion_for_sku (self, initial_direct_demand_rate, new_sku_fill_rate, current_fill_rate, new_tested_buffer, committed_buffer, unit_cost, j_target_groups, sku_group_participation, sku_max_fill_rate, main_investment)    
        
        self.basicLogger.warning("MarginalValue after group completion- " + str(indexrow) + " - " + str(mainindexrow) + " - marginal_gain : " + str(marginal_gain)+ " - total_lead_time : " + str(total_lead_time)+ " - new_sku_fill_rate : " + str(new_sku_fill_rate) )
        dependant_changes_list=[]
       
        #LOCATION EXPLORATION and recursive assessment of each found sku
        if repl_site_ids is not None:
            # here the total lead time is for the parent sku, the wait time will be a portion of that
            # then each dependant sku will add its leg time to that
            new_parent_wait_time_for_dependants=MEIO.Replenishingloclt (self,  total_lead_time, new_sku_fill_rate)

            for dependant_site_id in repl_site_ids:
                dependantIndex=tuple([item_id, dependant_site_id])
                #self.basicLogger.warning("MarginalValue - found dependant_site_id - " + str(item_id) + "/repl: " + str(dependant_site_id) + " - dependantIndex: " + str(dependantIndex)+ " - new_parent_wait_time_for_dependants: " + str(new_parent_wait_time_for_dependants) )

                #indexrow_dependant, DependantSkuPointer=MEIO.lookForSkuPointer (self, item_id, dependant_site_id)
                #if (dependantIndex in  dfSkuPointer):
                #    self.basicLogger.warning("MEIO - dependant_site_id: " + str(dependant_site_id))
                
                dependant_marginal_gain, dependant_changes= MEIO.MarginalValue(self, dependantIndex, mainindexrow, main_investment,  new_parent_wait_time_for_dependants)
                marginal_gain+=dependant_marginal_gain
                self.basicLogger.warning("MarginalValue computed for REPL : "  + str(dependantIndex) + " - " + str(mainindexrow)  + " -  dependant_marginal_gain: "  + str(dependant_marginal_gain) + " -  marginal_gain: " + str(marginal_gain)+ " -  dependant_changes: " + str(dependant_changes) + " - new_parent_wait_time_for_dependants: " + str(new_parent_wait_time_for_dependants) )
                if dependant_changes is not None:
                    dependant_changes_list.extend(dependant_changes)
        
        #KIT EXPLORATION and recursive assessment of each found sku
        #should be first level only
        if kits_list is not None:
            for dependant_kit in kits_list:
                dependantIndex=tuple([dependant_kit, site_id])
                
                # find the new lead time given the change in fill rate
                try:
                    new_parent_wait_time_for_dependants=MEIO.kit_probabilistic_waiting_Lt(self, dependantIndex, item_id, new_sku_fill_rate)
                    self.basicLogger.warning("MarginalValue - found dependant_kit - " + str(dependantIndex) + " - " + str(mainindexrow) + "/ - site_id: " + str(site_id) + " - dependantIndex: " + str(dependantIndex)+ " - probabilistic wait time: " + str(new_parent_wait_time_for_dependants) )
                    dependant_marginal_gain, dependant_changes= MEIO.MarginalValue(self, dependantIndex, mainindexrow, main_investment, new_parent_wait_time_for_dependants)
                    marginal_gain+=dependant_marginal_gain
                    self.basicLogger.warning("MarginalValue - computed for KIT - " + str(dependantIndex) + " - " + str(mainindexrow) + " - dependant_marginal_gain: " + str(dependant_marginal_gain) + " -  marginal_gain: " + str(marginal_gain)+ " -  dependant_changes: " + str(dependant_changes) + " - probabilistic wait time: " + str(new_parent_wait_time_for_dependants) )
                    if dependant_changes is not None:
                        dependant_changes_list.extend(dependant_changes)
                except Exception as e:
                    self.basicLogger.warning("MarginalValue - dependantIndex except: " + str(dependantIndex) + " - " + str(mainindexrow) + " - exception: " + str(e))

        # here we should prepare for the next pick (essentially, we should modify the marginal_value that is the picking criteria)
        # only do that for the part for which we modified the buffer, the other ones cannot be picked as nothing changed on the buffer

        if (indexrow== mainindexrow):
            # what changes for the main record is the buffer (we just increased it by an avgsize
            # and the corresponding fillrate
            # IT IS IMPORTANT TO NOTE THAT THESE FIELDS SHOULD ONLY RECORD DIRECT MODIFICATION TO THE SKU AS PER AN INCREASE OF ITS BUFFER
            #dfSkuPointer.at[indexrow, 'sku_init_set']=True

            #if marginal_value==0 and committed_buffer>0:
            #    marginal_value=-1
            marginal_value=marginal_gain/main_investment if marginal_value != large_quantity else large_quantity
            #self.basicLogger.warning("MarginalValue: " + str(indexrow) + " - marginal_value: " + str(marginal_value) + " - marginal_gain: " + str(marginal_gain) + " - sku_init_set: " + str(row[37]) )

            self.basicLogger.warning("MarginalValue - MAIN ITEM: "  + str(indexrow) + " - " + str(mainindexrow) + " - " + "marginal_gain: " + str(marginal_gain) + " - main_investment: " + str(main_investment) + " -  new_tested_buffer: " + str(new_tested_buffer)+ " -  committed_buffer: " + str(committed_buffer) + " -  new_sku_fill_rate : " + str(new_sku_fill_rate) + " -  sku_max_fill_rate : " + str(sku_max_fill_rate) + " -  MARGINAL_VALUE : " + str(marginal_value) + " -  dependant_changes_list: " + str(dependant_changes_list))
            
            # here the condition is a bit complex to reflect that we do not want to exclude parts for which the first jump immediately made it to the limit (often as 1.0)
            # but the second time around a change has to exist
            if (new_sku_fill_rate<=sku_max_fill_rate and current_fill_rate<new_sku_fill_rate):
                # we did not exceed the max fill rate
                dfSkuPointer.at[indexrow, 'marginal_value']=marginal_value
                dfSkuPointer.at[indexrow, 'new_buffer']=new_tested_buffer
                dfSkuPointer.at[indexrow, 'new_fill_rate']=new_sku_fill_rate
                dfSkuPointer.at[indexrow, 'dependant_changes']=dependant_changes_list
                # changes_in_sku= (indexrow, 0, new_fill_rate, total_lead_time)
                # if this is the result of applySkuMin, we commit the value directly and recompute the marginal value
                
            else:
                # we went too far with this change, the sku is now eliminated from the race
                # here we did exceed the maxfillrate
                marginal_value=0
                dfSkuPointer.at[indexrow, 'marginal_value']=-large_quantity
                
        else:
            # not the main item: set the modification for the dependant records
            changes_in_sku= (indexrow, new_sku_fill_rate, wait_time)
            dependant_changes_list.append(changes_in_sku)
            self.basicLogger.warning("MarginalValue - NOT MAIN  - " + str(indexrow) + " - wait_time: " + str(wait_time) + "  - changes_at_this_level: " + str(changes_at_this_level) + "  - dependant_changes_list: " + str(dependant_changes_list) + " - marginal_gain: " + str(marginal_gain)  + " - marginal_value: " + str(marginal_value) ) 
  
        #now in this function each part and dependant part should log its change if the marginal_value for the main part is chosen
        # we save the dependant changes in a specific structure
        # what changes for a related record is the 
        # - apparent lead time
        # - resulting fill rate
        # buffer will not change
        #MEIO.log_dependant_changes (new_fill_rate, total_lead_time, indexrow, mainindexrow)    
            
        self.basicLogger.warning("MarginalValue - RETURN : "  + str(indexrow) + " - " + str(mainindexrow) + " - " + "marginal_value: " + str(marginal_value) + " -  new_tested_buffer : " + str(new_tested_buffer) + " -  new_sku_fill_rate: " + str(new_sku_fill_rate) + " -  dependant_changes_list: " + str(dependant_changes_list))
    
        return marginal_gain, dependant_changes_list 

        

    def initialize_marginal_value_fast (self):
        # here we will go through the levels comp to kits 0->n
        # ketteq_custom.io_kit_processing_lt_lvl
        # then ketteq_custom.io_loc_lt_hier
        # lvl: 1->n (2)
                
        init_sql_param=self.sql_read_attributes_select + """
         from custom.io_initialize_skus att 
         where sku_min_fill_rate >0 or sku_min_sl_qty >0  or sku_min_sl_slices >0 or (use_existing_inventory =true and on_hand >0)
         order by lvl
        """
        
        init_sql=Template(init_sql_param).substitute(DistributionThreshod= self.distribution_threshold)
        self.initSKU   = pds.read_sql(init_sql, self.dbConnection , index_col=['item_id','site_id'] );

        
        ### ZERO STOCK TESTING
        for indexrow, row in self.initSKU.iterrows():
            #self.basicLogger.warning("mainloop : - logging initial fill rate - " + str(indexrow) + " - " + str(row) )
            initial_total_demand_rate=row[0]
            initial_direct_demand_rate=row[1]

            avgsize=row[2]
            eoq=row[3]
            leg_lead_time=row[5]

            distribution=row[8]
            unit_cost=row[10]

            total_lead_time=row[12]
            #dmd_stddev=row[13]
            lt_stddev=row[14]
            #total_fcst_monthly=row[15]

            on_hand=row[16]

            sku_max_fill_rate=row[24]
            sku_min_fill_rate=row[25]
            sku_max_sl_qty=row[26]
            sku_min_sl_qty=row[27]
            
            sku_max_sl_slices=row[28]
            sku_min_sl_slices=row[29]
                    
            use_existing_inventory=row[30]
            current_wait_time=row[34]
            coefficient_of_variation=row[41]
            coefficient_variation_cap=row[42]

            mad=row[43]
            #mad=row[43]
            dmd_stddev=row[45]
            

            effective_total_lt_fcst, effective_direct_lt_fcst=MEIO.EffectiveLtFcst(self, initial_total_demand_rate, initial_direct_demand_rate,  leg_lead_time, current_wait_time)

            dmd_stddev, lt_stddev=MEIO.capStandardDeviation(self, float(dmd_stddev), 30.0 * float(initial_total_demand_rate +  initial_direct_demand_rate), float(coefficient_variation_cap), float(lt_stddev), float(leg_lead_time + current_wait_time) )

            dmd_stddev=MEIO.capStandardDeviation( self, effective_total_lt_fcst, coefficient_of_variation)

            tgt_min_rop_qty, tgt_max_rop_qty = MEIO.minMaxROPQty(self, sku_max_sl_slices, large_quantity, initial_total_demand_rate, avgsize, sku_max_sl_qty, sku_min_sl_slices, use_existing_inventory, on_hand, sku_min_sl_qty, effective_total_lt_fcst)

            #the following function determines the fr associated with the min sku quantity provided
            # it also provides the min quantity if a min fill rate was provided
            minSet_buffer, minSet_fill_rate=MEIO.applySkuMin(self, effective_total_lt_fcst,  eoq, distribution, int(avgsize), total_lead_time, unit_cost, tgt_min_rop_qty, tgt_max_rop_qty, sku_max_fill_rate, sku_min_fill_rate, lt_stddev, dmd_stddev, mad)

            #new_sku_fill_rate=MEIO.new_fill_rate_calc(self, 0, initial_total_demand_rate*total_lead_time, eoq, distribution, avgsize, total_lead_time, lt_stdev, dmd_stddev)
            self.basicLogger.warning("initialize_marginal_value - : COMMIT NEW SKU " + str(indexrow) + " - (minSet_fill_rate)': " + str(minSet_fill_rate) + " - (minSet_buffer)': " + str(minSet_buffer) )

            
            #here we are simply going to update the maindataframe with the FR and quantities that work for that
            #rowMainDataFrame=self.maindataframe.loc([indexrow])
            dfSkuPointer=self.maindataframe
            # new and commiteed fill rate

            #dfSkuPointer.at[indexrow, 'current_fill_rate']=minSet_fill_rate
            dfSkuPointer.at[indexrow, 'new_fill_rate']=minSet_fill_rate
            #rowMainDataFrame[17]=minSet_fill_rate
            #rowMainDataFrame[9]=minSet_fill_rate
            
            #committed and new buffer
            #dfSkuPointer.at[indexrow, 'committed_buffer']=minSet_buffer
            dfSkuPointer.at[indexrow, 'new_buffer']=minSet_buffer
            #rowMainDataFrame[6]=minSet_buffer
            #rowMainDataFrame[7]=minSet_buffer
            
            # the commit is very important and required to log the progress (FR/newbuffer) into the groups
            # we send a specific parameter (init that specifies that in this specific case, marginal value can be null)
            MEIO.commit_logged_value(self, indexrow, True)
            
            
        self.basicLogger.warning("initialize_marginal_value - : REBASING - self.maindataframe: " + str(self.maindataframe) )

        for i in self.maindataframe.index:
            marginal_gain, dependant_changes_list=MEIO.MarginalValue(self,  i, i, None, None)
            self.basicLogger.warning("initialize_marginal_value - : NEW SKU " + str(i) + " - (marginal_gain)': " + str(marginal_gain) + " - (dependant_changes_list)': " + str(dependant_changes_list)+ " - new_buffer': " + str(self.maindataframe.loc[i].iloc[7]) )
        
            

    def saveResults (self):
        #current_scenario_id=self.maindataframe.at[1, 'scenario_id'];
        current_scenario_id=int(self.maindataframe.iloc[0, 22]);
        self.basicLogger.warning("saveResults - : current_scenario_id " + str(current_scenario_id) )

        #here we delete the data for the particular scenario if the table was already created
        try:
            deleteStatement="DELETE FROM custom.io_sku_output WHERE custom.io_sku_output.scenario_id = " + str(current_scenario_id)
            self.basicLogger.warning("saveResults - : deleteStatement " + str(deleteStatement) )
    
            #print ("SCENARIO DONE delete statement: ", deleteStatement);
            self.dbConnection.execute(deleteStatement);
        except Exception as err:
            self.basicLogger.warning("saveResults - : exception " + str(err) )

            pass
        
        #here we delete the data for the particular scenario if the table was already created
        try:
            #self.dbConnection.execute("DELETE FROM ketteq_custom.io_group_output");
            deleteStatement="DELETE FROM custom.io_group_output"
            self.basicLogger.warning("saveResults - : deleteStatement " + str(deleteStatement) )
    
            #print ("SCENARIO DONE delete statement: ", deleteStatement);
            self.dbConnection.execute(deleteStatement);

        except:
            pass
        
        #self.maindataframe.drop (columns=['repl_site_ids'], axis=1, inplace=True);
        try:
            self.maindataframe.drop (columns=['dependant_changes'], axis=1, inplace=True);
            self.maindataframe.drop (columns=['j_target_groups'], axis=1, inplace=True);
        except:
            pass
        
        meo_row_count=self.maindataframe.to_sql(name=self.MEO_output_table, schema=self.schema_custo, con=self.alchemyEngine, if_exists='append', index=True);
        self.basicLogger.warning("saveResults - : saved the output MEO table: " + str(meo_row_count))

        #added the logger
        logger_row_count=self.loggerDF.to_sql(name=self.MEO_logger_table, schema=self.schema_custo, con=self.alchemyEngine, if_exists='replace', index=True);
        self.basicLogger.warning("saveResults - : saved the logger table: " + str(logger_row_count))

        group_row_count=pds.DataFrame.from_dict(self.target_dictionary.dictionary).T.to_sql(name=self.MEO_output_group, schema=self.schema_custo, con=self.alchemyEngine, if_exists='append')
        self.basicLogger.warning("saveResults - : saved the group target table: " + str(group_row_count))



    def MainLoop (self):
        
        main_data_sql=Template(self.sql_read_attributes).substitute(DistributionThreshod= self.distribution_threshold)
        self.basicLogger.warning("mainloop : - main_data_sql - " + str(main_data_sql) )
        

        self.maindataframe   = pds.read_sql(main_data_sql, self.dbConnection , index_col=['item_id','site_id'] );
        
        self.maindataframe.astype({"new_fill_rate":"float64"});
        self.maindataframe.astype({"current_fill_rate":"float64"});
        self.maindataframe.astype({"marginal_value":"float64"});
        
            
        self.maindataframe.sort_index()

        #MEIO.add_sku_min_fill_rate()
        #self.basicLogger.warning("mainloop - size of the considered data - " + str(range(len(self.maindataframe))) )

        goalsReached=False
        # INITIALIZE all skus with potential move and gain        
        # get the start time

        # MEIO.initial_jump(self)
        MEIO.initialize_marginal_value_fast (self)
        
        self.basicLogger.warning("-------------------------mainloop - done with initial setup of the marginal value-------------------------------- " )
        
        #initial_index=0
        
        while not goalsReached:
            # find the top ranked sku
            i = self.maindataframe['marginal_value'].idxmax()
            #self.basicLogger.warning("mainloop - goalsReached - index: " + str(i) +" - row: \n" + str(self.maindataframe.loc[(i)]) )
            # Here I need to commit the value
            goalsReached=MEIO.commit_logged_value (self, i)
            #self.basicLogger.warning("mainloop - after commitloggedvalue - index: " + str(i) + " - goalsReached: " + str(goalsReached) )


        MEIO.saveResults(self)

        # look for all skus for which min fill rate was not reached
        #df_not_achieved_sku_tgt=self.maindataframe[self.maindataframe.new_fill_rate<self.maindataframe.sku_tgt_fillrate]
        #MEIO.add_sku_min_fill_rate(self, df_not_achieved_sku_tgt)
        
        
