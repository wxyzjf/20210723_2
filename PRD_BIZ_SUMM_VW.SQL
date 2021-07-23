create view prd_biz_summ_vw.vw_bw_as_build_mast
    as select * from mig_adw.BW_SO_HIST
    where  end_date = to_date('2999-12-31','yyyy-mm-dd');

create view prd_biz_summ_vw.VW_BW_BIP_MAST 
    as select * from mig_adw.bw_bip_mast_hist 
        where end_date =to_date('2999-12-31','yyyy-mm-dd');

create view prd_biz_summ_vw.vw_bw_est_build
    as   
    select 
             e.ESTATE_ID as EST_ESTATE_ID
            ,e.BUILDING_TYPE AS EST_BUILDING_TYPE
            ,e.ESTATE_NAME AS EST_ESTATE_NAME
            ,e.LOT_NAME AS EST_LOT_NAME
            ,e.LOT_NUMBER AS EST_LOT_NUMBER
            ,e.UPDATED_BY AS EST_UPDATED_BY
            ,e.UPDATED_DATE AS EST_UPDATED_DATE
            ,e.UPDATED_TIME AS EST_UPDATED_TIME
            ,e.CREATED_BY AS EST_CREATED_BY
            ,e.CREATED_DATE AS EST_CREATED_DATE
            ,e.CREATED_TIME AS EST_CREATED_TIME
            ,e.AREA AS EST_AREA
            ,e.DISTRICT AS EST_DISTRICT
            ,e.EST_BUILD_HOTEL_SHOP_FLG AS EST_BUILD_HOTEL_SHOP_FLG
            ,e.NO_STREET_NAME AS EST_NO_STREET_NAME
            ,e.SALES_MANAGER AS EST_SALES_MANAGER
            ,e.SALES_SUPERVISOR AS EST_SALES_SUPERVISOR
            ,b.BUILDING_NAME
            ,b.ESTATE_NAME
            ,b.BUILDING_TYPE
            ,b.TYPE            
            ,b.AVAILABLE_AS_BUILT_COPPER
            ,b.AVAILABLE_AS_BUILT_FIBER
            ,b.AVAILABLE_BIP_COPPER
            ,b.AVAILABLE_BIP_FIBER
            ,b.BMO_ADDRESS
            ,b.BMO_CONTACT
            ,b.BROKEN_AS_BUILT_COPPER
            ,b.BROKEN_AS_BUILT_FIBER
            ,b.BUILT_BY
            ,b.CAP_PER_FLAT_RATIO_COPPER
            ,b.CAP_PER_FLAT_RATIO_FIBER
            ,b.CAP_PER_FLOOR_RATIO_COPPER
            ,b.CAP_PER_FLOOR_RATIO_FIBER
            ,b.CREATED_BY
            ,b.CREATED_DATE
            ,b.CREATED_TIME
            ,b.ESTATE
            ,b.LDB_ASSIGNMENT
            ,b.LOT_NAME
            ,b.LOT_NUMBER
            ,b.MAINTAINED_BY
            ,b.NO_OF_FLOOR
            ,b.OFFICE_BUILDING_FLG
            ,b.REMARK
            ,b.RENT_AS_BUILT_COPPER
            ,b.RENT_AS_BUILT_FIBER
            ,b.RENT_BIP_COPPER
            ,b.RENT_BIP_FIBER
            ,b.REPLACEMENT_BIP_COPPER
            ,b.REPLACEMENT_BIP_FIBER
            ,b.REQ_SUBMIT_FORM_ACCS_BUILDING
            ,b.RUNNING_NUMBER
            ,b.SPARE_AS_BUILT_COPPER
            ,b.SPARE_AS_BUILT_FIBER
            ,b.TBE
            ,b.V_PLUS_H
            ,b.V
            ,b.WARRANTY_END_DATE
            ,b.UPDATED_BY
            ,b.UPDATED_DATE
            ,b.UPDATED_TIME
            ,b.ESTATE_ID
            ,b.START_DATE
            ,b.END_DATE
            ,b.CREATE_TS
            ,b.REFRESH_TS
            ,b.CREATED_DATETIME_HK
            ,b.UPDATED_DATETIME_HK
        from mig_adw.bw_est_ref e
            ,mig_adw.bw_build_ref b            
        where b.estate_id =  e.estate_id
        and b.end_date = to_date('2999-12-31','yyyy-mm-dd')         
        and e.end_date = to_date('2999-12-31','yyyy-mm-dd');                                 
        
create view prd_biz_summ_vw.vw_bw_so  
as select * from mig_adw.BW_SO_HIST
    where  end_date = to_date('2999-12-31','yyyy-mm-dd');
 
create view prd_biz_summ_vw.vw_bw_so_job
as select * from mig_adw.BW_SO_JOB_HIST
    where  end_date = to_date('2999-12-31','yyyy-mm-dd');
 
create view prd_biz_summ_vw.VW_BW_SO_CONTRACT
as  select 
            m.ref_id
            ,m.building_name
            ,m.blockwiring_id
            ,m.cable_type
            ,m.unit_socket_no
            ,m.floor
            ,m.unit_socket_type
            ,m.socket_term_cd
            ,m.cable_pair
            ,m.connect_to_ldb
            ,m.sub_local
            ,m.sub_ldb_capacity
            ,m.sub_local_box_port
            ,m.local_box
            ,m.ldb_capacity
            ,m.local_box_port
            ,m.mdf_fm_type
            ,m.mdf_fm
            ,m.mdf_vertical_fm_panel
            ,m.mdf_fm_capacity
            ,m.mdf_pair_fm_panel_port
            ,m.tbe
            ,m.original_status
            ,m.status
            ,m.connected_to_bip
            ,m.fno
            ,m.bip
            ,m.bip_vertical
            ,m.bip_cable_port
            ,m.mdf_fm_2
            ,m.mdf_vertical_fm_panel_2
            ,m.mdf_pair_fm_port_2
            ,m.building_id
            ,m.estate_id
            ,m.estate_name
            ,m.building_type
            ,m.start_date
            ,m.end_date
            ,m.create_ts
            ,m.refresh_ts
            ,so.CONTRACT_START_DATE
            ,so.CONTRACT_EXPIRY_DATE
            ,so.CONTRACT_REVIEW_DATE
            ,so.CONTRACT_ACTIVE
            ,so.CONTRACT_LD_CHARGE_DAYS
        from mig_adw.bw_as_build_mast_hist m
        left outer join 
            (Select   h.bwid
                    ,max(h.CONTRACT_START_DATE) keep  (dense_rank first order by active,created_date desc, expiry_date asc,contract_start_date asc,review_date asc,ld_charge_days desc) CONTRACT_START_DATE
                    ,max(h.expiry_date ) keep  (dense_rank first order by active,created_date desc, expiry_date asc,contract_start_date asc,review_date asc,ld_charge_days desc) as CONTRACT_EXPIRY_DATE
                    ,max(h.review_date ) keep  (dense_rank first order by active,created_date desc, expiry_date asc,contract_start_date asc,review_date asc,ld_charge_days desc) as CONTRACT_REVIEW_DATE
                    ,max(h.active ) keep  (dense_rank first order by active,created_date desc, expiry_date asc,contract_start_date asc,review_date asc,ld_charge_days desc) as CONTRACT_ACTIVE
                    ,max(h.ld_charge_days ) keep  (dense_rank first order by active,created_date desc, expiry_date asc,contract_start_date asc,review_date asc,ld_charge_days desc) as CONTRACT_LD_CHARGE_DAYS
            from  mig_adw.bw_so_contract_hist h
            where h.end_date = to_date('2999-12-31','yyyy-mm-dd')              
            group by h.bwid) so
        on m.blockwiring_id = so.bwid
        where m.end_date = to_date('2999-12-31','yyyy-mm-dd');
 
create view prd_biz_summ_vw.VW_BW_AGREE 
    as select * from mig_adw.BW_AGREE_HIST
    where m.end_date = to_date('2999-12-31','yyyy-mm-dd');

create view prd_biz_summ_vw.VW_BW_AGREE_EST 
    as select * from mig_adw.BW_AGREE_EST_HIST h
    where h.end_date = to_date('2999-12-31','yyyy-mm-dd');

create view prd_biz_summ_vw.VW_BW_AGREE_TARIFF 
    as select * from mig_adw.BW_AGREE_TARIFF_HIST h;;

create view prd_biz_summ_vw.VW_BW_LAST_MTH_BILL 
    as select * from mig_adw.BW_LAST_MTH_BILL_DTL h;

create view prd_biz_summ_vw.VW_BW_SUBCON_JOBSUMM 
    as select h.*,s.service_order
      from mig_adw.BW_SUBCON_JOBSUMM_DTL h
      left outer join mig_adw.BW_SO_JOB_HIST s
       on h.job_order = s.job_order
      where 
        s.end_date = to_date('2999-12-31','yyyy-mm-dd');


create view prd_biz_summ_vw.VW_BW_SUBCON_SOSUMM 
    as select * from mig_adw.BW_SUBCON_SOSUMM_DTL h;

create view prd_biz_summ_vw.VW_BW_PTII_QTN 
    as select * from mig_adw.BW_PTII_QTN_HIST h
    where h.end_date = to_date('2999-12-31','yyyy-mm-dd');

create view prd_biz_summ_vw.VW_BW_AS_BUILD_SUMM     
as  select 
            m.ref_id
            ,m.building_name
            ,m.blockwiring_id
            ,m.cable_type
            ,m.unit_socket_no
            ,m.floor
            ,m.unit_socket_type
            ,m.socket_term_cd
            ,m.cable_pair
            ,m.connect_to_ldb
            ,m.sub_local
            ,m.sub_ldb_capacity
            ,m.sub_local_box_port
            ,m.local_box
            ,m.ldb_capacity
            ,m.local_box_port
            ,m.mdf_fm_type
            ,m.mdf_fm
            ,m.mdf_vertical_fm_panel
            ,m.mdf_fm_capacity
            ,m.mdf_pair_fm_panel_port
            ,m.tbe
            ,m.original_status
            ,m.status
            ,m.connected_to_bip
            ,m.fno
            ,m.bip
            ,m.bip_vertical
            ,m.bip_cable_port
            ,m.mdf_fm_2
            ,m.mdf_vertical_fm_panel_2
            ,m.mdf_pair_fm_port_2
            ,m.building_id
            ,m.estate_id
            ,m.estate_name
            ,m.building_type
            ,m.start_date
            ,m.end_date
            ,m.create_ts
            ,m.refresh_ts
            ,so.CONTRACT_START_DATE
            ,so.CONTRACT_EXPIRY_DATE
            ,so.CONTRACT_REVIEW_DATE
            ,so.CONTRACT_ACTIVE
            ,so.CONTRACT_LD_CHARGE_DAYS
            ,t.trx_month
        from (
                select add_months(trunc(sysdate,'mm') ,- (rownum))trx_month
                      ,add_months(trunc(sysdate,'mm') ,- (rownum-1))-1 trx_mthend 
                  from dual connect by rownum< 25
        )t           
        left outer join mig_adw.bw_as_build_mast_hist m
            on t.trx_mthend between m.start_date and m.end_date
        left outer join 
            (Select   h.bwid
                    ,max(h.CONTRACT_START_DATE) keep  (dense_rank first order by active,created_date desc, expiry_date asc,contract_start_date asc,review_date asc,ld_charge_days desc) CONTRACT_START_DATE
                    ,max(h.expiry_date ) keep  (dense_rank first order by active,created_date desc, expiry_date asc,contract_start_date asc,review_date asc,ld_charge_days desc) as CONTRACT_EXPIRY_DATE
                    ,max(h.review_date ) keep  (dense_rank first order by active,created_date desc, expiry_date asc,contract_start_date asc,review_date asc,ld_charge_days desc) as CONTRACT_REVIEW_DATE
                    ,max(h.active ) keep  (dense_rank first order by active,created_date desc, expiry_date asc,contract_start_date asc,review_date asc,ld_charge_days desc) as CONTRACT_ACTIVE
                    ,max(h.ld_charge_days ) keep  (dense_rank first order by active,created_date desc, expiry_date asc,contract_start_date asc,review_date asc,ld_charge_days desc) as CONTRACT_LD_CHARGE_DAYS
            from  mig_adw.bw_so_contract_hist h
            where h.end_date = to_date('2999-12-31','yyyy-mm-dd')              
            group by h.bwid) so
        on m.blockwiring_id = so.bwid;
        
create view VW_BW_BIP_MAST_SUMM
as select 
     h.REF_ID
    ,h.CABLE_TYPE
    ,h.STATUS
    ,h.FNO
    ,h.BW_BIP_MAST_HIST
    ,h.BW_BIP_MAST_HIST_VERTICAL
    ,h.BW_BIP_MAST_HIST_CAPACITY
    ,h.BW_BIP_MAST_HIST_CABLE_PORT
    ,h.MDF_FM_TYPE
    ,h.MDF_FM
    ,h.MDF_VERTICAL_FM_PANEL
    ,h.MDF_FM_CAPACITY
    ,h.MDF_PAIR_FM_PORT
    ,h.TBE
    ,h.BUILDING_ID
    ,h.ESTATE_ID
    ,h.ESTATE_NAME
    ,h.BUILDING_TYPE
    ,h.BLOCKWIRING_ID
    ,h.BUILDING_NAME
    ,t.TRX_MONTH 
   from (
                select add_months(trunc(sysdate,'mm') ,- (rownum)) as trx_month
                      ,add_months(trunc(sysdate,'mm') ,- (rownum-1))-1 as trx_mthend 
                  from dual connect by rownum< 25
   ) t
   left outer join mig_adw.BW_BIP_MAST_HIST h
        on t.trx_mthend between h.start_date and h.end_date;
        
create view VW_BW_PTII_QTN_SUMM
as select
     h.ITEM_CAT
    ,h.PRICE_LIST_ID
    ,h.ITEM
    ,h.UNIT
    ,h.STANDARD_UNIT_RATE
    ,h.SUBCON_QTY
    ,h.SUBCON_AMT
    ,h.SMC_CONFIRMED_QTY
    ,h.SMC_CONFIRMED_AMT
    ,h.FNO_CONFIRMED_QTY
    ,h.FNO_CONFIRMED_AMT
    ,h.SERVICE_ORDER
    ,h.PART_II_QUOTATION
    ,h.BUILDING_ID
    ,h.BUILDING_NAME
    ,h.ESTATE_ID
    ,h.ESTATE_NAME
    ,h.QUOTED_COMPLETED_FLAG
    ,t.trx_month  
    from (
                select add_months(trunc(sysdate,'mm') ,- (rownum)) as trx_month
                      ,add_months(trunc(sysdate,'mm') ,- (rownum-1))-1 as trx_mthend 
                  from dual connect by rownum< 25
   ) t
   left outer join mig_adw.BW_PTII_QTN_HIST h
    on t.trx_mthend between h.start_date and h.end_date;
        