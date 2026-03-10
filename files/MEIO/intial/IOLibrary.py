# -*- coding: utf-8 -*-
"""
Created on Mon Jul  3 16:23:06 2023

@author: BrunoZindy
import pandas as pds
import math;
from Optimization.IO.z_conversion import ZConversion;
from scipy.stats import poisson
"""


import logging;
from sqlalchemy import  create_engine, DDL;
from string import Template

#logging.basicConfig(filename='c:\temp\IOLibrary.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
history_horizon_days="interval '2 year'"
min_PO_count=8
large_quantity=99999999
hits_measure='direct_demand'
direct_demand_measure='direct_demand'
directDemandColumn="calc_qty"

# this max will be capped later with the max coef of variation



sql_list_order_type="""
    select 
    '   (''' || replace(FIRST_VALUE (CONFIG) over () ->> 'salesOrderTypeName', ',', ''',''') || ''')' salesordertype
    from ketteq_import.integration_engine ie 
    where type='DEMAND_HISTORY'
    limit 1 """

sql_daily_slices="""
    select start_on from ketteq.slice sli
    join ketteq.slice_type slt on slt.id = sli.slice_type_id 
    where slt.xuid='day'
    and start_on > current_date - $HistoryHorizonDays
    and start_on < current_date
    """






#this is the basis for the io_fcst table
sql_fcst_over_lt="""
select
        		item_id,
        		site_id,
        		lead_time 
        		--sum(case when minstart_on = start_on then qty else 0 end) forecast    ,
         , avg(total_fcst) total_fcst_monthly
         , avg(direct_fcst) direct_fcst_monthly
         , avg(indirect_fcst) indirect_fcst_monthly
        ,sum((case 
        	when lead_time >= cumdays then coalesce(total_fcst,0)
        when lead_time> cumdays-daysinmonth and lead_time<cumdays then ((lead_time - (cumdays-daysinmonth))/daysinmonth) * total_fcst
        else 0
        end)/lead_time)  total_demand_rate
 ,sum((case 
        	when lead_time >= cumdays then coalesce(direct_fcst,0)
        when lead_time> cumdays-daysinmonth and lead_time<cumdays then ((lead_time - (cumdays-daysinmonth))/daysinmonth) * direct_fcst
        else 0
        end)/lead_time)  direct_demand_rate
 ,sum((case 
        	when lead_time >= cumdays then coalesce(repl_fcst,0)
        when lead_time> cumdays-daysinmonth and lead_time<cumdays then ((lead_time - (cumdays-daysinmonth))/daysinmonth) * repl_fcst
        else 0
        end)/lead_time)  repl_demand_rate
 ,sum((case 
        	when lead_time >= cumdays then coalesce(kit_fcst,0)
        when lead_time> cumdays-daysinmonth and lead_time<cumdays then ((lead_time - (cumdays-daysinmonth))/daysinmonth) * kit_fcst
        else 0
        end)/lead_time)  kit_demand_rate
 ,sum((case 
        	when lead_time >= cumdays then coalesce(indirect_fcst,0)
        when lead_time> cumdays-daysinmonth and lead_time<cumdays then ((lead_time - (cumdays-daysinmonth))/daysinmonth) * indirect_fcst
        else 0
        end)/lead_time)  indirect_demand_rate
        --, avg(mape) mape, avg(mad) mad, avg(hist_std_dev) hist_std_dev
        	from
        		(
        		select
        			df.item_id,
        			df.site_id,
        			df.total_fcst  ,
        			df.direct_fcst  ,
        			df.indirect_fcst  ,
        			df.kit_fcst  ,
        			df.repl_fcst  ,
        			max( isi.lead_time) over (partition by df.item_id) lead_time ,
        			df.date ,
        			date_part( 'days', (df.date + interval '1 month - 1 day') ) daysinmonth ,
        			sum(date_part( 'days', (df.date + interval '1 month - 1 day') )) over (partition by  df.item_id, df.site_id 	order by 	df.date ) cumdays    
                    ,3 mape,3 mad,3 hist_std_dev
        		from
        			custom.io_total_forecast df
                join plan.item i on i.id=df.item_id
                -- this was used to get the average lead time based on POs - pos.avg_lt
                --left outer join custom.io_po_stddev pos on pos.site_id=df.site_id and pos.item_id=df.item_id 
        		left outer join custom.io_lead_time isi on
        			isi.item_id = df.item_id
        			and isi.site_id = df.site_id
        		--join ketteq.slice sli on
        		--	sli.id = df.date
        ) tmp
        	group by
        		item_id,
        		site_id,
        		lead_time 
        having sum((case 
             	when lead_time >= cumdays then coalesce(total_fcst,0)
             when lead_time> cumdays-daysinmonth and lead_time<cumdays then ((lead_time - (cumdays-daysinmonth))/daysinmonth) * total_fcst
             else 0
             end)/lead_time) >0
                """


sql_sku_attributes="""
select
        	i.id item_id  ,
        	s.id site_id   ,
        	fcst.total_demand_rate  ,
        	fcst.direct_demand_rate  ,
        	fcst.indirect_demand_rate   ,
        	fcst.repl_demand_rate   ,
        	fcst.kit_demand_rate   ,
            coalesce (ihs.avgsize,1) avgsize,
            ihs.hitcount skucount,
            fcst.total_fcst_monthly,
        	coalesce(isi.unit_cost) unit_cost   ,
        	greatest( sqrt( (2*fcst.total_demand_rate*invItem.order_cost) / (isi.unit_cost*invItem.holding_rate) ) ,1) eoq    ,
        	coalesce(i2.qty ,0) on_hand
            , ikc.components
            , ick.kits
            , irs.site_ids repl_site_ids
            , irgs.parent_ids 
            , coalesce( llk.leg_lead_time, llh.leg_lead_time, lt.lead_time) + coalesce(llk.wait_time, llh.wait_time) total_lead_time
            , coalesce(llk.wait_time, llh.wait_time) wait_time
            , coalesce( llk.leg_lead_time, llh.leg_lead_time, lt.lead_time) leg_lead_time
            , coalesce (ihs.qty_stddev, ihs.avg_monthly_quantity) qty_stddev
            , coalesce (ihs.qty_stddev, ihs.avg_monthly_quantity) line_stddev
            , ihs.avg_monthly_quantity
            , ihs.coefficient_of_variation  dmd_coefficient_of_variation 
            , 3 stddev_lt
            , itps.targets j_target_groups
            , coalesce(itps.sku_max_fill_rate, 200) sku_max_fill_rate
            , coalesce(itps.sku_min_fill_rate,0) sku_min_fill_rate
            , case when itps.sku_max_sl_qty >0 then itps.sku_max_sl_qty else """ + str(large_quantity) + """ end sku_max_sl_qty
            , case when itps.sku_min_sl_qty >0 then itps.sku_min_sl_qty else -1 end sku_min_sl_qty
            , case when itps.sku_max_sl_slices>0 then itps.sku_max_sl_slices else """ + str(large_quantity) + """  end sku_max_sl_slices
            , case when itps.sku_min_sl_slices > 0 then itps.sku_min_sl_slices else -1 end sku_min_sl_slices
            , coalesce (itps.use_existing_inventory, False) use_existing_inventory
            , coalesce (itps.varcoeff_max,0) varcoeff_max
            , json_array_length(itps.targets) group_participation
            -- ****************************************************************************
            --BZ: THIS NEEEDS TO BE CORRECTED, JUST HAVE THAT FOR NOW TO KEEP ON DEBUGGING
            , coalesce(ihs.qty_stddev, ihs.avg_monthly_quantity) /1.25 mad
            -- ****************************************************************************
        from
            custom.io_fcst fcst
        join custom.io_scope ioscope on ioscope.item_id=fcst.item_id and ioscope.site_id=fcst.site_id
        left join plan.inv_item invItem on invItem.item_id=fcst.item_id and invItem.site_id=fcst.site_id
        left join plan.on_hand i2 on i2.item_id = fcst.item_id 	and i2.site_id = fcst.site_id and type_id=(SELECT id FROM plan.on_hand_type oht where xuid='Unallocated' )
        left join plan.item_site isi on  	isi.item_id = fcst.item_id and isi.site_id = fcst.site_id      
        left join custom.io_lead_time lt on  	lt.item_id = fcst.item_id and lt.site_id = fcst.site_id      
        join plan.item i on  	i.id = fcst.item_id
        join plan.site s on  	s.id = fcst.site_id
        left outer join custom.io_kit_to_components ikc on ikc.site_id=fcst.site_id and ikc.kit_id=fcst.item_id
        left outer join custom.io_component_to_kits ick on ick.site_id=fcst.site_id and ick.comp=fcst.item_id
        left outer join custom.io_replenished_skus irs on irs.site_repl=fcst.site_id and irs.item_id=fcst.item_id
        left outer join custom.io_replenishing_skus irgs on irgs.site_id=fcst.site_id and irgs.item_id=fcst.item_id
        left outer join custom.io_loc_lt_hier llh on llh.site_id=fcst.site_id and llh.item_id=fcst.item_id
        left outer join custom.io_kit_lt llk on llk.site_id=fcst.site_id and llk.kit_id=fcst.item_id
        left outer join custom.io_history_stddev ihs on ihs.site_id=fcst.site_id and ihs.item_id=fcst.item_id
        left outer join custom.io_targets_per_sku itps on itps.site_id=fcst.site_id and itps.item_id=fcst.item_id 
        $WILSON_EOQ_TABLE
        where
        	 isi.unit_cost>0 
             --and llh.leg_lead_time>0 
             and (coalesce(llk.leg_lead_time, llh.leg_lead_time) + coalesce(llk.wait_time, llh.wait_time))>0
             and itps.tgt_group is not null"""
             
#NG: we need ONLY ONE lead time per sku. There cannot be 2 options
sql_lead_time="""
    SELECT item_id , site_id 
                    , max(supplier_id) supplier_id  , max(rt.xuid) type_xuid  , max(bom_alternate) bom_alternate  
                    , max(lead_time) + max(pick_pack_time) + max(transit_time) + max(inspection_time)+ max(safety_lead_time)lead_time
                    FROM plan.route rou
                    JOIN plan.route_type rt ON rou.type_id =rt.id 
                    GROUP BY item_id , site_id
"""

# MOVED TO NG
sql_superseeded_kit="""	select bl.site_id 
        	, coalesce(chkit.item_id, bl.item_id)  kit
        	, coalesce (chcomp.item_id, bl.child_item_id  ) comp 
        	, bl.child_qty  
        	from plan.bill_of_material   bl
        	left join plan.item_chain chcomp on chcomp.child_item_id =bl.child_item_id and chcomp.child_site_id =bl.site_id 
        	left join plan.item_chain chkit on chkit.child_item_id =bl.item_id and chkit.child_site_id =bl.site_id
        	join custom.io_scope scopekit on scopekit.item_id=coalesce(chkit.item_id, bl.item_id) and scopekit.site_id=bl.site_id
            join custom.io_scope scopecomp on scopecomp.item_id=coalesce(chcomp.item_id, bl.child_item_id) and scopecomp.site_id=bl.site_id
            join plan.item_site iss on   bl.item_id = iss.item_id and bl.site_id = iss.site_id
            --join ketteq_planning.planner_buyer pb on pb.id = iss.buyer_id
        where coalesce(chkit.item_id, bl.item_id) <> coalesce (chcomp.item_id, bl.child_item_id  ) """

# these are not the components of anything
# MOVED TO NG
sql_topkit = """select comp.site_id, comp.item_id kit, comp.child_item_id comp, comp.child_qty quantity , comp.item_id  initial_kit
        , case when kit.item_id is null then 'topkit' else 'components' end initial_level
        from plan.bill_of_material comp
        left outer join plan.bill_of_material kit    on kit.site_id=comp.site_id and  kit.child_item_id=comp.item_id
        where kit.item_id is null
        """

# these are not the kits of anything ==> basically lower level components
# MOVED TO NG
sql_lowcomp = """
        select kit.site_id, kit.item_id kit, kit.child_item_id comp, kit.child_qty quantity , kit.child_item_id initial_comp 
        , case when comp.item_id  is null then 'component' else 'kit' end initial_level
        from plan.bill_of_material kit
        left outer join plan.bill_of_material comp on comp.site_id=kit.site_id and  kit.child_item_id=comp.item_id
        where comp.item_id is null
        """

# MOVED TO NG
sql_initialize_skus="""
select
	isa.* ,
	coalesce(tmp.lvl,	99)  lvl
from
	custom.io_sku_attributes isa
left outer join
  (
	select
		item_id,
		site_id,
		min(lvl) lvl
	from
		(
		select
			kit item_id,
			site_id,
			lvl
		from
			custom.io_kit_processing_lt_lvl
	union
		select
			item_id,
			site_id,
			lvl +(
			select
				max(lvl)
			from
				custom.io_kit_processing_lt_lvl)
		from
			custom.io_loc_lt_hier
) tmpint
	group by
		item_id,
		site_id
) tmp on
	isa.item_id = tmp.item_id
	and isa.site_id = tmp.site_id
order by
	lvl
"""

# MOVED TO NG
sql_delete_kit_dep=""" delete from custom.io_demand_forecast_kit  """

# MOVED TO NG
sql_delete_kit_dh_roll=""" delete from custom.io_demand_kit_rollup  """



# MOVED TO NG
#initially modified table for mape
sql_forecast_total="""     	
SELECT dhm.item_id , dhm.site_id , df.date , df.""" + directDemandColumn + """ quantity
FROM dp_plan.dp_forecast df 
JOIN dp_plan.dp_header_map dhm ON dhm.dp_item_id =df.dp_item_id  AND dhm.dp_site_id =df.dp_site_id 
"""
        


sql_demand_total=""" 
select 
     scope.item_id item_id 
    , scope.site_id site_id 
    , scope.date date 
    , sum(coalesce(dh.quantity,0) + coalesce(dout.quantity,0) ) quantity
    from custom.io_scope_slice scope 
    left outer join ketteq_planning.chain_demand_history dh on scope.item_id=dh.item_id and scope.site_id=dh.site_id and scope.date=dh.date 
    join ketteq_planning.measure mf on mf.id=dh.measure_id        
    left outer join ketteq_planning.demand_outlier  dout on  dout.item_id =scope.item_id  
    and dout.site_id =scope.site_id 
    and dh.date = scope.date 
    and dh.measure_id =dout.measure_id 
    where mf.use_in_total =True
    group by 
    scope.item_id 
	, scope.site_id 
	, scope.date 
        """



# MOVED TO NG
sql_create_repl_dep="""
        select df.item_id, lvl.site_repl site_id, df.date, sum(df.quantity) quantity 
        from custom.io_demand_forecast df
        join custom.IO_repl_lvl lvl on lvl.item_id=df.item_id and lvl.site_id=df.site_id
        group by df.item_id, lvl.site_repl, df.date
        """

# MOVED TO NG
sql_insert_repl_dep="""
insert into custom.io_demand_forecast_repl ( item_id, site_id, date, quantity)
        select df.item_id, lvl.site_repl, df.date, sum(df.quantity) quantity 
        from custom.io_demand_forecast df
        join custom.IO_repl_lvl lvl on lvl.item_id=df.item_id and lvl.site_id=df.site_id
        group by df.item_id, lvl.site_repl, df.date
        """


# MOVED TO NG
sql_insert_repl_dep_dh="""insert into custom.io_demand_total_repl (item_id, site_id, date, quantity)
select
	lvl.item_id,
	lvl.site_repl,
	coalesce(repl.site_id ,	df.site_id),
	sum(coalesce(df.quantity, 0)+coalesce(repl.quantity, 0)) quantity
from
	custom.io_demand_total df
full outer join custom.io_demand_total_repl repl on
	 repl.item_id = df.item_id
	and repl.site_id = df.site_id
	and repl.date = df.date
join custom.io_repl_lvl lvl on
	lvl.item_id = coalesce(repl.item_id ,
	df.item_id)
	and lvl.site_id = coalesce(repl.site_id,
	df.site_id)
group by
	lvl.item_id,
	lvl.site_repl,
	coalesce(repl.site_id ,
	df.date)        """

# MOVED TO NG
sql_insert_repl_dep_dh="""insert into custom.io_demand_total_repl (item_id, site_id, date, quantity)
select
	lvl.item_id,
	lvl.site_repl,
	coalesce(repl.site_id ,	df.date),
	sum(coalesce(df.quantity, 0)+coalesce(repl.quantity, 0)) quantity
from
	custom.io_demand_total df
full outer join custom.io_demand_total_repl repl on
	 repl.item_id = df.item_id
	and repl.site_id = df.site_id
	and repl.date = df.date
join custom.io_repl_lvl lvl on
	lvl.item_id = coalesce(repl.item_id ,
	df.item_id)
	and lvl.site_id = coalesce(repl.site_id,
	df.site_id)
group by
	lvl.item_id,
	lvl.site_repl,
	coalesce(repl.site_id ,
	df.date)        """

# MOVED TO NG
sql_create_repl_dep_dh="""
select
	lvl.item_id,
	lvl.site_repl,
	coalesce(repl.site_id ,	df.date),
	sum(coalesce(df.quantity, 0)+coalesce(repl.quantity, 0)) quantity
from
	custom.io_demand_total df
full outer join custom.io_demand_total_repl repl on
	repl.item_id = df.item_id
	and repl.site_id = df.site_id
	and repl.date = df.date
join custom.io_repl_lvl lvl on
	lvl.item_id = coalesce(repl.item_id ,
	df.item_id)
	and lvl.site_id = coalesce(repl.site_id,
	df.site_id)
group by
	lvl.item_id,
	lvl.site_repl,
	coalesce(repl.site_id ,
	df.date)        """


# MOVED TO NG
sql_delete_repl_dep=""" delete from custom.io_demand_forecast_repl  """

# MOVED TO NG
sql_delete_repl_dep_dh=""" delete from custom.io_demand_total_repl  """

# MOVED TO NG
sql_kit_next_lvl="""select distinct lvl.site_id, lvl.kit initial_kit, ksd.kit, ksd.comp, lvl.quantity, initial_level
        from custom.io_kit_fcst_lvl lvl
        join custom.io_kit_bom_superseeded ksd on lvl.site_id=ksd.site_id and lvl.comp=ksd.kit"""
        
        
# MOVED TO NG
sql_kit_lt_next_lvl="""select distinct lvl.initial_comp, lvl.initial_level, lvl.site_id, ksd.kit, ksd.comp, lvl.quantity
        from custom.io_kit_lt_lvl lvl
        join custom.io_kit_bom_superseeded ksd on lvl.site_id=ksd.site_id and lvl.kit=ksd.comp"""



# MOVED TO NG
sql_kit_to_components="""
    select iks.site_id, iks.KIT kit_id , array_agg(comp ) components
    from custom.io_kit_ForecastProcessing_lvl_master iks 
    where exists (select 1 from custom.io_fcst fcst
                  join custom.io_scope scope on scope.item_id=fcst.item_id and scope.site_id=fcst.site_id
                  where fcst.item_id=iks.comp  and fcst.site_id=iks.site_id
                  )
    and exists (select 1 from custom.io_fcst fcst
                  join custom.io_scope scope on scope.item_id=fcst.item_id and scope.site_id=fcst.site_id
                  where fcst.item_id=iks.kit  and fcst.site_id=iks.site_id
                  ) 
    group by  iks.site_id, iks.KIT
    """ 
        
        
# MOVED TO NG
sql_repl_hier="""
SELECT item_id , site_id , source_site_id site_repl, lead_time + pick_pack_time lead_time  
FROM plan.route ro
WHERE lead_time>0 AND source_site_id>0
    """
        
# MOVED TO NG
sql_repl_hier_bottom_locs = """
    select hier.*
       from custom.IO_repl_hier hier
       where not exists (select 1 from custom.IO_repl_hier hier2
       where hier2.item_id=hier.item_id and hier2.site_repl=hier.site_id)
       and site_id<>site_repl"""
       

# MOVED TO NG
sql_repl_next_lvl="""select lvl.item_id, rsd.site_id, rsd.site_repl
        from custom.IO_repl_lvl lvl
        join custom.IO_repl_hier rsd on lvl.item_id=rsd.item_id and lvl.site_repl=rsd.site_id
        where rsd.site_repl is not null """


#used in io_fcst
# MOVED TO NG
sql_total_forecast="""
    select 
    df.item_id, 
    df.site_id, 
    df.date
    , sum(coalesce(df.quantity,0) +coalesce(kit.quantity,0) +coalesce(repl.quantity,0) ) total_fcst
    , sum(coalesce(kit.quantity,0) +coalesce(repl.quantity,0) ) indirect_fcst
    , sum(coalesce(df.quantity,0) ) direct_fcst
    , sum(kit.quantity) kit_fcst
    , sum(repl.quantity) repl_fcst
    --, avg(mape) mape, avg(mad) mad, avg(hist_std_dev) hist_std_dev
    from  custom.io_demand_forecast df
    left outer join custom.io_demand_forecast_kit kit on kit.item_id=df.item_id and kit.site_id=df.site_id  and kit.date=df.date 
    left outer join custom.io_demand_forecast_repl repl on repl.item_id=df.item_id and repl.site_id=df.site_id and repl.date=df.date   
    group by 
    df.item_id, 
    df.site_id, 
    df.date
    having sum(coalesce(df.quantity,0) +coalesce(kit.quantity,0) +coalesce(repl.quantity,0) ) >0
    """


# MOVED TO NG
sql_total_demand="""
        select 
        , coalesce (dt.item_id,  repl.item_id, kit.item_id) item_id
        , coalesce (dt.site_id,  repl.site_id, kit.site_id) site_id
        , coalesce (dt.date,  repl.date, kit.date) date
        , sum(coalesce(dt.quantity,0) +coalesce(kit.quantity,0) +coalesce(repl.quantity,0) ) total_qty
        , sum(coalesce(kit.quantity,0) +coalesce(repl.quantity,0) ) indirect_qty
        , sum(coalesce(dt.quantity,0) ) direct_qty
        , sum(kit.quantity) kit_qty
        , sum(repl.quantity) repl_qty
        from
        custom.io_demand_total dt
        full outer join custom.io_demand_kit_rollup kit on kit.item_id=dt.item_id and kit.site_id=dt.site_id  and kit.date=dt.date 
        full outer join custom.io_demand_total_repl repl on repl.item_id=dt.item_id and repl.site_id=dt.site_id and repl.date=dt.date 
        group by 
        coalesce (dt.item_id,  repl.item_id, kit.item_id) 
        , coalesce (dt.site_id,  repl.site_id, kit.site_id) 
        , coalesce (dt.date,  repl.date, kit.date)         
        """


# MOVED TO NG
sql_component_to_kits="""
    select iks.site_id , comp comp,  array_agg(kit) kits
    from custom.io_kit_processing_lt_lvl_master iks 
    where exists (select 1 from custom.io_fcst fcst
                  join custom.io_scope scope on scope.item_id=fcst.item_id and scope.site_id=fcst.site_id
                  where fcst.item_id=iks.kit  and fcst.site_id=iks.site_id
                  )
    and exists (select 1 from custom.io_fcst fcst
                  join custom.io_scope scope on scope.item_id=fcst.item_id and scope.site_id=fcst.site_id
                  where fcst.item_id=iks.comp  and fcst.site_id=iks.site_id
                  )
    group by  iks.site_id, comp
    """ 


# MOVED TO NG
sql_replenished_skus="""
    select hier.item_id , hier.site_repl , array_agg(hier.site_id) site_ids
    from custom.io_repl_hier hier
    join custom.io_fcst fcst on fcst.item_id=hier.item_id and fcst.site_id=hier.site_id
    join custom.io_scope scope on scope.item_id=fcst.item_id and scope.site_id=fcst.site_id
    where hier.site_repl is not null 
    group by hier.item_id, hier.site_repl
    """

# MOVED TO NG
sql_replenishing_skus="""
    select hier.item_id , hier.site_id , array_agg(hier.site_repl) parent_ids
    from custom.io_repl_hier hier
    join custom.io_fcst fcst on fcst.item_id=hier.item_id and fcst.site_id=hier.site_repl
    join custom.io_scope scope on scope.item_id=fcst.item_id and scope.site_id=fcst.site_id
    where hier.site_id is not null 
    group by hier.item_id, hier.site_id
    """


# MOVED TO NG
sql_loc_lt_hier="""
        with recursive lochier as (
select hier.item_id, hier.site_id , hier.site_repl 
    ,1 lvl
    , hier.lead_time leg_lead_time
    , coalesce(llk.wait_time,0) wait_time
   from custom.IO_repl_hier hier
   left outer join custom.io_kit_lt llk on llk.kit_id=hier.item_id and llk.site_id=hier.site_id
   	where site_repl is null
    union 
    select ioh.item_id , ioh.site_id , ioh.site_repl 
    , lh.lvl+1
    , ioh.lead_time leg_lead_time
    , lh.leg_lead_time+lh.wait_time wait_time
    from lochier lh
    join custom.IO_repl_hier ioh on lh.item_id=ioh.item_id and lh.site_id=ioh.site_repl
     ) 
    select item_id, site_id, site_repl, leg_lead_time,   wait_time, lvl 
    from lochier lh
    where exists (select 1 from custom.io_scope scope where scope.item_id=lh.item_id and scope.site_id=lh.site_id)
    or exists (select 1 from custom.io_scope scope where scope.item_id=lh.item_id and scope.site_id=lh.site_repl)
    """


# MOVED TO NG
sql_kit_lt_hier="""
    with recursive lochier as (
    select hier.item_id, hier.site_id , hier.site_repl , hier.lead_time, 1 lvl
    , hier.lead_time leg_lead_time
           from custom.IO_repl_hier hier
           	where site_repl is null
    union 
    select ioh.item_id , ioh.site_id , ioh.site_repl , ioh.lead_time+lh.lead_time
    , lh.lvl+1
    , ioh.lead_time leg_lead_time
    from lochier lh
    join custom.IO_repl_hier ioh on lh.item_id=ioh.item_id and lh.site_id=ioh.site_repl
     ) 
    select * from lochier 
    """


sql_history_stddev= """
     SELECT distinct
		COALESCE(so.item_id, da.item_id) item_id
		,COALESCE(so.site_id, da.site_id) site_id
		--, cd.date
		--, coalesce(	so.qty , 0) qty
		, da.startdate
		, avg(	CASE WHEN cd.date>= da.startdate THEN coalesce(so.qty , 0) END ) OVER (PARTITION BY da.item_id 	, da.site_id) avg_monthly_quantity
		, sum(	CASE WHEN cd.date>= da.startdate THEN coalesce(so.qty , 0) END ) OVER (PARTITION BY da.item_id 	, da.site_id) sumDH
		, case when hitcount>0 then sum(	CASE WHEN cd.date>= da.startdate THEN coalesce(so.qty , 0) END ) OVER (PARTITION BY da.item_id 	, da.site_id) /hitcount else 1 end avgsize
        , hitcount
		, count(	CASE WHEN cd.date>= da.startdate THEN coalesce(so.qty , 0) END ) OVER (PARTITION BY da.item_id 	, da.site_id) periodNumber
		, count(	CASE WHEN cd.date>= da.startdate THEN so.qty  END ) OVER (PARTITION BY da.item_id 	, da.site_id) nonNullPeriod
		, stddev(	CASE WHEN cd.date>= da.startdate THEN coalesce(so.qty , 0) END ) OVER (PARTITION BY so.item_id 	, so.site_id) qty_stddev
		, (stddev(	CASE WHEN cd.date>= da.startdate THEN coalesce(so.qty , 0) END ) OVER (PARTITION BY da.item_id 	, da.site_id) :: float)/ (avg( CASE WHEN cd.date>= da.startdate THEN coalesce(so.qty , 0) END ) OVER (PARTITION BY da.item_id 	, da.site_id) :: float) coefficient_of_variation
FROM
	plan.calendar cal
jOIN plan.calendar_date cd ON 	cd.calendar_id = cal.id AND cal.xuid = 'Month'
FULL OUTER JOIN (
                SELECT  item_id, site_id, min(date) startdate, sum(qty) hitcount 
                FROM plan.demand_actual da  
                join plan.measure m on m.id=da.measure_id 
                where m.name = '""" + hits_measure + """'
                GROUP BY item_id, site_id
                HAVING sum(da.qty)>0 ) da ON 1=1
full OUTER JOIN plan.demand_actual so ON so.date = cd.date AND da.item_id=so.item_id AND da.site_id=so.site_id AND so.qty>0
 JOIN plan.measure m ON m.id = so.measure_id  and	  m.name = '""" + direct_demand_measure + """'
    """


#BEWARE there is some JCI hardcode here
sql_po_history_stddev="""
    select distinct pos.item_id, pos.site_id
    , syslt_3::int avg_lt, coalesce(stdevn_3, syslt_3) stddev_lt 
    --, supplier_id
    from jci.historical_data_aggregated pos
    join ketteq_planning.item_site is2 on is2.item_id =pos.item_id and is2.site_id =pos.site_id and is2.supplier_id =pos.supplier_id 
    where syslt_3>0     and status='Active'
"""
 

# MOVED TO NG
sql_scope="""
select distinct sd.item_id, sd.site_id
    from custom.io_optimization_group iog
    join custom.io_optimization_group_skugroup iogs on iog.id =iogs.optimization_group_id 
    join custom.io_item_site_group sd on sd.group_id =iogs.group_id
    intersect 
select distinct sd.item_id, sd.site_id
    from custom.io_optimization_group iog
    join custom.io_opt_tgt_group iootg on iootg.optimization_group_id=iog.id 
    join custom.io_target_group_skugroup itgs on itgs.target_group_id =iootg.target_group_id 
    join custom.io_item_site_group sd on sd.group_id =itgs.group_id
    """
# MOVED TO NG
sql_create_dep_kit_forecast_rollup="""
   select  NULL::int8 item_id
    , NULL::int8 site_id
    ,  NULL::date date
    ,  NULL::int8 quantity
            """


 
# the link to the dependant demand should be a full outer job. TO BE CORRECTED.
# MOVED TO NG
sql_insert_dep_kit_forecast_rollup="""
    insert into custom.io_demand_forecast_kit ( item_id, site_id, date, quantity)
    select  lvl.comp item_id
    --, lvl.kit kit_id
    , df.site_id
    ,  df.date
    ,  sum((coalesce(df.quantity,0) + coalesce(depcompdem.quantity,0) + coalesce(kitindirectrepl.quantity,0)) * lvl.child_qty) quantity
    from custom.io_demand_forecast df
    left outer join custom.io_demand_forecast_kit depcompdem on depcompdem.item_id=df.item_id and depcompdem.site_id=df.site_id and  depcompdem.date=df.date
    left outer join custom.io_demand_forecast_repl kitindirectrepl on kitindirectrepl.item_id=df.item_id and kitindirectrepl.site_id=df.site_id and  kitindirectrepl.date=df.date
    join custom.io_kit_bom_superseeded lvl on lvl.kit=df.item_id and lvl.site_id=df.site_id
    join custom.io_kit_forecastprocessing_lvl kpl on  kpl.site_id=lvl.site_id and kpl.comp=lvl.comp
    where kpl.lvl=$processingLevel
    group by  lvl.comp,  df.site_id,  df.date
    having sum((coalesce(df.quantity,0) + coalesce(depcompdem.quantity,0) + coalesce(kitindirectrepl.quantity,0)) * lvl.child_qty)>0
    """
    
    # the link to the dependant demand should be a full outer job. TO BE CORRECTED.
# MOVED TO NG
sql_insert_dep_kit_demand_rollup="""
    insert into custom.io_demand_kit_rollup ( item_id, site_id, date, quantity)
    select  lvl.comp item_id
    --, lvl.kit kit_id
    , dh.site_id
    ,  dh.date
    ,  sum((coalesce(dh.quantity,0) + coalesce(depcompdem.quantity,0) + coalesce(kitindirectrepl.quantity,0)) * lvl.child_qty) quantity
    from custom.io_demand_total dh
    left outer join custom.io_demand_kit_rollup depcompdem on depcompdem.item_id=dh.item_id and depcompdem.site_id=dh.site_id and  depcompdem.date=dh.date
    left outer join custom.io_demand_total_repl kitindirectrepl on kitindirectrepl.item_id=dh.item_id and kitindirectrepl.site_id=dh.site_id and  kitindirectrepl.date=dh.date
    join custom.io_kit_bom_superseeded lvl on lvl.kit=dh.item_id and lvl.site_id=dh.site_id
    join custom.io_kit_forecastprocessing_lvl kpl on kpl.site_id=lvl.site_id and kpl.comp=lvl.comp
    where kpl.lvl=$processingLevel
    group by  lvl.comp,  dh.site_id,  dh.date
    having sum((coalesce(dh.quantity,0) + coalesce(depcompdem.quantity,0) + coalesce(kitindirectrepl.quantity,0)) * lvl.child_qty)>0
    """
    
sql_initial_lvl_kit_lt="""
    select kbs.kit kit_id, skit.id site_id, 
    avg(iskit.lead_time ) leg_lead_time
    , max( coalesce (iscomp.lead_time, 0) ) wait_time
    from plan.item ikit
    full outer join plan.site skit on 1=1
    join custom.io_kit_processing_lt_lvl kpl on kpl.kit=ikit.id and kpl.site_id=skit.id
    left outer join custom.io_lead_time iskit on iskit.item_id =ikit.id and iskit.site_id =skit.id 
    left outer join custom.io_kit_bom_superseeded kbs on kbs.kit=kpl.kit and kbs.site_id=kpl.site_id 
    left outer join custom.io_lead_time iscomp on iscomp.item_id =kbs.comp and iscomp.site_id =skit.id
    left outer join plan.item icomp on icomp.id =kbs.comp 
    where kpl.lvl=0
    group by kbs.kit , skit.id 
"""
    
    
sql_insert_dep_kit_lt="""
    insert into custom.io_kit_lt (kit_id, site_id, leg_lead_time, wait_time)
    select kbs.kit kit_id, skit.id site_id, 
    avg(iskit.lead_time ) leg_lead_time
    , max( coalesce (iscomp.lead_time, 0) ) wait_time
        from plan.item ikit
    full outer join plan.site skit on 1=1
    join custom.io_kit_processing_lt_lvl kpl on kpl.kit=ikit.id and kpl.site_id=skit.id
    left outer join custom.io_lead_time iskit on iskit.item_id =ikit.id and iskit.site_id =skit.id 
    left outer join custom.io_kit_bom_superseeded kbs on kbs.kit=kpl.kit and kbs.site_id=kpl.site_id 
    left outer join custom.io_lead_time iscomp on iscomp.item_id =kbs.comp and iscomp.site_id =skit.id
    where kpl.lvl=$processingLevel
    group by kbs.kit , skit.id 
    """

sql_targets_per_sku = """
select sd.item_id, sd.site_id
, json_agg(jsonb_build_object( 'io_tgt_group' , itg.name, 'io_tgt_fill_rate', itg.fill_rate, 'io_tgt_max_budget', coalesce(itg.max_budget,999999999))::jsonb) targets
, max(iog."name" ) opt_group
, string_agg (itg.name, ' ,' order by itg.name) tgt_group
, min(itg.sku_max_fill_rate) sku_max_fill_rate
, max(itg.sku_min_fill_rate) sku_min_fill_rate
, min(itg.sku_max_safety_level) sku_max_sl_qty
, max(itg.sku_min_safety_level) sku_min_sl_qty
, min(itg.sku_min_safety_level_fcst_slices) sku_min_sl_slices
, max(itg.sku_max_safety_level_fcst_slices) sku_max_sl_slices
, bool_or (use_existing_inventory) use_existing_inventory
, min (varcoeff_max) varcoeff_max
from custom.io_item_site_group sd
join custom.io_target_group_skugroup itgs on itgs.group_id =sd.group_id 
join custom.io_target_group itg on itg.id=itgs.target_group_id 
join custom.io_opt_tgt_group iotg on iotg.target_group_id =itg.id 
join custom.io_optimization_group iog on iog.id =iotg.optimization_group_id 
join custom.io_optimization_group_skugroup iogs on iogs.optimization_group_id =iog.id 
join custom.io_item_site_group sdopt on sdopt.group_id =iogs.group_id 
and sdopt.item_id=sd.item_id
and sdopt.site_id=sd.site_id
group by sd.item_id, sd.site_id
"""

sql_target_sku = """
select distinct (sd.levels->'item_id')::int item_id, (sd.levels->'site_id')::int site_id
, itg.name target_group_name
from ketteq.segment_Detail sd
join custom.io_target_group_segment itgs on itgs.segment_id =sd.segment_id 
join custom.io_target_group itg on itg.id=itgs.target_group_id 
join custom.io_opt_tgt_group iotg on iotg.target_group_id =itg.id 
join custom.io_optimization_group iog on iog.id =iotg.optimization_group_id 
join custom.io_optimization_group_segment iogs on iogs.optimization_group_id =iog.id 
join ketteq.segment_detail sdopt on sdopt.segment_id =iogs.segment_id 
and sdopt.levels->'item_id'=sd.levels->'item_id'
and sdopt.levels->'site_id'=sd.levels->'site_id'
"""



# MOVED TO NG
sql_scope_slice="""SELECT
	sco.*,
	sli.monthdate
FROM
	custom.io_scope sco
JOIN (
	SELECT
		cd.id ,
		cd.name,
		cd.date monthdate
	FROM
		plan.calendar_date cd
	INNER JOIN plan.calendar cal ON
		cal.id = cd.calendar_id
	WHERE
		cal.type = 'MONTH'
		AND date<current_date
		AND date>current_date-INTERVAL '2 years'
 ) sli ON
	1 = 1"""


class IOLibrary:
    def __init__(self, dbConnection):
        self.dbConnection = dbConnection
        self.schema_custo="custom"
        self.distribution_threshold=5
        #IOLibrary.dataPreparation(self)
        

    def coalesce(*args, preds=[False, None]):
        for el in args:
            if el not in preds:
                return el
        return None
    
    
    def checkTableExists(self, tablename):

        result=self.dbConnection.execute(DDL("""
            SELECT COUNT(*)
            FROM pg_tables
            WHERE tablename = '{0}'
            """.format(tablename.replace('\'', '\'\''))))
        if result.fetchone()[0] == 1:
            result.close()
            return True
    
        result.close()
        return False

    def executeSQL (self, sql):
        logging.warning("executeSQL - " + sql)

        modified_rows=self.dbConnection.execute(DDL(sql))
        logging.warning("executeSQL (" + str(modified_rows.rowcount) +")"  )
        return(modified_rows)
        
    def dropTable (self, table_name, schema_name):
        logging.warning("dropTable - before dropping - " + table_name)

        drop_sql="""drop table IF EXISTS """ + IOLibrary.coalesce(schema_name, self.schema_custo) + "." + table_name ;
        logging.warning("dropTable - built dropping sql - " + drop_sql)

        self.dbConnection.execute(DDL(drop_sql));
        logging.warning("dropTable - Dropped table - " + table_name)
        
    def createTable (self, table_name, schema_name, table_sql):
        logging.warning("createTable - before dropping - " + table_name)
        IOLibrary.dropTable (self, table_name, schema_name)
        logging.warning("createTable - after dropping - " + table_name)

        create_sql="""create table """ + IOLibrary.coalesce(schema_name, self.schema_custo) + "." + table_name + " as " + table_sql;
        logging.warning("CREATING TABLE - " + table_name.upper() + " - "  + " - as - " + table_sql)

        row_count=self.dbConnection.execute(DDL(create_sql)).rowcount
        logging.warning("CREATED TABLE - " + table_name.upper() + " - " + str(row_count) )

        return(row_count);
        
    def renameTable (self, table_name, schema_name, renamed_table):
        IOLibrary.dropTable (self, renamed_table, schema_name)
        renameTableSQL="""alter table """ + IOLibrary.coalesce(schema_name, self.schema_custo) + "." + table_name + " rename to "  + renamed_table;
        self.dbConnection.execute(DDL(renameTableSQL));
        logging.warning("renamed table - " + table_name + " - into - " + renamed_table)

    
    def generateKitForecastProcessingOrder (self):
        #KIT 2 COMP
        IOLibrary.createTable(self, "io_kit_fcst_lvl", self.schema_custo, sql_topkit)
        
        #MOVED TO NG
        kit_count=IOLibrary.createTable(self, "io_kit_ForecastProcessing_lvl_master", self.schema_custo, "select distinct site_id, kit, comp, kit initial_kit,  0 lvl, initial_level from custom.io_kit_fcst_lvl")
        loopIndex=0
        # here we simply go down recursively from the top kit to components, giving lvls increase
        while kit_count>0:
            kit_count=IOLibrary.createTable(self, "io_kit_fcst_next_lvl", self.schema_custo, sql_kit_next_lvl)
            IOLibrary.renameTable(self, "io_kit_fcst_next_lvl", self.schema_custo, "io_kit_fcst_lvl")
            IOLibrary.executeSQL(self, """insert into custom.io_kit_ForecastProcessing_lvl_master  (site_id, kit, comp, initial_kit, lvl, initial_level) 
                                 select distinct site_id, kit, comp, initial_kit, (select max(lvl)+1 from custom.io_kit_ForecastProcessing_lvl_master) lvl, initial_level from custom.io_kit_fcst_lvl """)
            loopIndex+=1
  
        #only one level of processing but all skus. This goes to the attribute table (components field)
        #kit_count=IOLibrary.createTable(self, "io_component_forecastprocessing_first_level", self.schema_custo, "select site_id, comp, kit  from custom.io_kit_ForecastProcessing_lvl_master  group by site_id, comp, initial_kit , initial_level having max(lvl)=0 ")  
  
        kit_count=IOLibrary.createTable(self, "io_kit_forecastprocessing_lvl", self.schema_custo, "select site_id, comp,  max(lvl) lvl from custom.io_kit_forecastprocessing_lvl_master where initial_level='topkit'  group by site_id, comp  ")
        return (loopIndex)
  
    def generateKitLtProcessingOrder (self):
        #COMPONENTS 2 KITS
        #IOLibrary.createTable(self, "IO_KIT_BOM_SUPERSEEDED", self.schema_custo, sql_superseeded_kit)
        # this returns the component at the bottom most level
        IOLibrary.createTable(self, "io_kit_lt_lvl", self.schema_custo, sql_lowcomp)
        
        kit_count=IOLibrary.createTable(self, "io_kit_processing_lt_lvl_master", self.schema_custo, "select distinct site_id, kit, comp, initial_comp, 0 lvl, initial_level from custom.io_kit_lt_lvl")
        loopIndex=0
        while kit_count>0:
            kit_count=IOLibrary.createTable(self, "io_kit_lt_next_lvl", self.schema_custo, sql_kit_lt_next_lvl)
            IOLibrary.renameTable(self, "io_kit_lt_next_lvl", self.schema_custo, "io_kit_lt_lvl")
            IOLibrary.executeSQL(self, """insert into custom.io_kit_processing_lt_lvl_master  (site_id, kit, comp, initial_comp, lvl, initial_level) 
                                 select distinct site_id, kit, comp, initial_comp,  (select max(lvl)+1 from custom.io_kit_processing_lt_lvl_master) lvl, initial_level 
                                 from custom.io_kit_lt_lvl """)
            loopIndex+=1
  
        #only one level of processing but all skus. This goes to the attribute table (kits field)
        #kit_count=IOLibrary.createTable(self, "io_kit_processing_lt_lvl_first_level", self.schema_custo, "select site_id, kit, initial_comp , 0 lvl, initial_level from custom.io_kit_processing_lt_lvl_master  group by site_id, kit, initial_comp , initial_level having max(lvl)=0 ")

        # all levels starting from the root. This is used to build io_kit_lt.
        kit_count=IOLibrary.createTable(self, "io_kit_processing_lt_lvl", self.schema_custo, "select site_id, kit, max(lvl) lvl from custom.io_kit_processing_lt_lvl_master where initial_level='component' group by site_id, kit  ")
        
        
        
        return (loopIndex)
  
    
    def generateDependantKitWaitTime (self):
        maxLoopIndex=IOLibrary.generateKitLtProcessingOrder (self)
        # this creates the lvl 0 (no wait time) and creates the table
        IOLibrary.createTable(self, "io_kit_lt", self.schema_custo, sql_initial_lvl_kit_lt)
        
        loopIndex=1
        while loopIndex<maxLoopIndex:
            #insert kit dependant demand for the level
            IOLibrary.executeSQL(self, Template(sql_insert_dep_kit_lt).substitute(processingLevel = loopIndex))
            loopIndex+=1
            

    
    def generateDependantKitForecast (self):
        maxLoopIndex=IOLibrary.generateKitForecastProcessingOrder (self)
        IOLibrary.createTable(self, "io_demand_forecast_kit", self.schema_custo, sql_create_dep_kit_forecast_rollup)
        
        loopIndex=1
        while loopIndex<maxLoopIndex:
            #insert kit dependant demand for the level
            IOLibrary.executeSQL(self, Template(sql_insert_dep_kit_forecast_rollup).substitute(processingLevel = loopIndex))
            loopIndex+=1
            
        return maxLoopIndex
    
    def generateDependantKitDemand (self, maxLoopIndex):
        #maxLoopIndex=IOLibrary.generateKitProcessingOrder (self)
        IOLibrary.executeSQL(self, sql_delete_kit_dh_roll)
        
        loopIndex=0
        while loopIndex<maxLoopIndex:
            #insert kit dependant demand for the level
            IOLibrary.executeSQL(self, Template(sql_insert_dep_kit_demand_rollup).substitute(processingLevel = loopIndex))
            loopIndex+=1
            

    
    def generateDependantReplForecastAndDemand (self):
        IOLibrary.createTable(self, "io_repl_hier", self.schema_custo, sql_repl_hier)
        repl_count=IOLibrary.createTable(self, "io_repl_lvl", self.schema_custo, sql_repl_hier_bottom_locs)
        logging.warning("Tables created - " + str(repl_count))
        
        IOLibrary.createTable(self, "io_demand_forecast_repl", self.schema_custo, sql_create_repl_dep)
        #IOLibrary.executeSQL(self, sql_create_repl_dep)
        logging.warning("create - " + sql_create_repl_dep)

        
        #IOLibrary.executeSQL(self, sql_create_repl_dep_dh)
        #logging.warning("create - " + sql_create_repl_dep_dh)

        while repl_count>0:
            #insert kit dependant demand for the level
            IOLibrary.executeSQL(self, sql_insert_repl_dep)
            
            #IOLibrary.executeSQL(self, sql_insert_repl_dep_dh)
            #create the next level
            repl_count=IOLibrary.createTable(self, "io_repl_next_lvl", self.schema_custo, sql_repl_next_lvl)
            IOLibrary.renameTable(self, "io_repl_next_lvl", self.schema_custo, "io_repl_lvl")
        


        
    def forecastAndDemand (self):
        #Merge forecast and adjustments
        #IOLibrary.createTable(self, "io_demand_forecast_temp", self.schema_custo, Template(sql_forecast_total_temp).substitute(scenarioname = self.scenario_name) )
        IOLibrary.createTable(self, "io_demand_forecast", self.schema_custo, sql_forecast_total )

        # removed demand rollup
        # IOLibrary.createTable(self, "io_demand_total", self.schema_custo, Template(sql_demand_total).substitute(scenarioname = self.scenario_name) )
        #Create kit'sEchelon's dependent demand
        IOLibrary.generateDependantReplForecastAndDemand (self)
        #Create kit's dependent demand
        maxLoopIndex=IOLibrary.generateDependantKitForecast (self)

        # no rollup of demand is required
        # IOLibrary.generateDependantKitDemand(self, maxLoopIndex)

        #Regrouping all 
        IOLibrary.createTable(self, "io_total_forecast", self.schema_custo, sql_total_forecast )
        
        # no demand table required
        # IOLibrary.createTable(self, "io_total_demand", self.schema_custo, sql_total_demand )

    
    def dataPreparation (self):
        #general
        #sales_order_type_in=IOLibrary.executeSQL(self, sql_list_order_type)
        
        #IOLibrary.createTable(self, "io_engine_parameter_per_sku", self.schema_custo, Template(sql_engine_parameter_per_sku).substitute(scenarioname = self.scenario_name))
        IOLibrary.createTable(self, "io_scope", self.schema_custo, sql_scope)
        IOLibrary.createTable(self, "io_scope_slice", self.schema_custo, sql_scope_slice)
        IOLibrary.createTable(self, "io_kit_bom_superseeded", self.schema_custo, sql_superseeded_kit)
        IOLibrary.createTable(self, "io_lead_time", self.schema_custo, sql_lead_time)
        #IOLibrary.createTable(self, "io_po_stddev", self.schema_custo, Template(sql_po_history_stddev).substitute(HistoryHorizonDays=history_horizon_days, minPOCount=min_PO_count)  )

        #Forecast
        IOLibrary.forecastAndDemand(self)
        IOLibrary.createTable(self, "io_fcst", self.schema_custo, sql_fcst_over_lt)
        
        #KITTING LEAD TIMES
        IOLibrary.generateDependantKitWaitTime (self)
        
            #the following table is to populate the attribute table
        IOLibrary.createTable(self, "io_component_to_kits", self.schema_custo, sql_component_to_kits)
        IOLibrary.createTable(self, "io_kit_to_components", self.schema_custo, sql_kit_to_components)


        #REPLENISHMENT LEAD TIMES
        IOLibrary.createTable(self, "io_replenished_skus", self.schema_custo, sql_replenished_skus)
        IOLibrary.createTable(self, "io_replenishing_skus", self.schema_custo, sql_replenishing_skus)
        IOLibrary.createTable(self, "io_loc_lt_hier", self.schema_custo, sql_loc_lt_hier)
        
        

        #sql_average_size='select 	so.item_id, so.site_id, greatest(coalesce(ceil(avg(so.request_quantity)), 1),1) avgsize from ketteq_planning.sales_order so join ketteq_planning.sales_order_type sot on sot.id =so.sales_order_type_id  	where sot.name in $in_list   ';
        
        #IOLibrary.createTable(self, "io_avg_size", self.schema_custo, Template(sql_average_size).substitute(in_list= (' and sot.name in ' + sales_order_type_in.fetchone()) if (not sales_order_type_in.fetchone() is None) else " ", HistoryHorizonDays= (' and so.request_due_on > current_date - ' + history_horizon_days), intervalDuration=history_horizon_days) )
        
        IOLibrary.createTable(self, "io_targets_per_sku", self.schema_custo, sql_targets_per_sku)
        IOLibrary.createTable(self, "io_history_stddev", self.schema_custo, Template(sql_history_stddev).substitute(HistoryHorizonDays=history_horizon_days)  )
        

        # FINAL TABLE
        # if the Wilson table was calculated and is existing then set WILSON_EOQ_TABLE
        if IOLibrary.checkTableExists(self, 'kcm_Wilson'):
            IOLibrary.createTable(self, "io_sku_attributes", self.schema_custo, Template(sql_sku_attributes).substitute( KCM_EOQ = ' kcm.order_size, ', WILSON_EOQ_TABLE=' left outer join custom.kcm_Wilson kcm on kcm.item_id=fcst.item_id and kcm.site_id=fcst.site_id and kcm.scenario_id=fcst.scenario_id  ') )
        else:
            IOLibrary.createTable(self, "io_sku_attributes", self.schema_custo, Template(sql_sku_attributes).substitute( KCM_EOQ = '  ', WILSON_EOQ_TABLE='  ') )
        
        #link to attribute table
        #sql_initialize_skus
        IOLibrary.createTable(self, "io_initialize_skus", self.schema_custo, sql_initialize_skus)

        


        