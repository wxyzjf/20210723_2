/opt/etl/prd/etl/APP/ADW/B_NORTON_RPT/bin> cat b_norton_rpt0010.pl
######################################################
#   $Header: /CVSROOT/SmarTone-Vodafone/Code/ETL/APP/ADW/B_BLACK_LIST_CUST/bin/b_black_list_cust0010.pl,v 1.1 2005/12/14 01:03:55 MichaelNg Exp $
#   Purpose:
#
#
######################################################


my $ETLVAR = $ENV{"AUTO_ETLVAR"};require $ETLVAR;

my $MASTER_TABLE = ""; #Please input the final target ADW table name here

sub runSQLPLUS{
    my $rc = open(SQLPLUS, "| sqlplus /\@${etlvar::TDDSN}");
    unless ($rc){
        print "Cound not invoke SQLPLUS commAND\n";
        return -1;
    }

    print SQLPLUS<<ENDOFINPUT;
        ${etlvar::LOGON_TD}
        ${etlvar::SET_MAXERR}
        ${etlvar::SET_ERRLVL_1}
        ${etlvar::SET_ERRLVL_2}

--Please type your SQL statement here

set define on;
define rpt_s_date=trunc(add_months(SYSDATE,-1),'mm');
define rpt_e_date=last_day(add_months(SYSDATE,-1));

--EXECUTE ${etlvar::UTLDB}.ETL_UTILITY.truncate_tbl2(P_schema_tbl_name=>'${etlvar::TMPDB}.B_NORTON_RPT_001');
EXECUTE ${etlvar::UTLDB}.ETL_UTILITY.truncate_tbl2(P_schema_tbl_name=>'MIG_ADW.B_NORTON_RPT_001');


--Define Code_Type
--INSERT /*+ APPEND */ INTO ${etlvar::TMPDB}.B_NORTON_RPT_001
INSERT /*+ APPEND */ INTO MIG_ADW.B_NORTON_RPT_001
(
   CUST_NUM
  ,SUBR_NUM
  ,BILL_SERV_CD
  ,START_DATE
  ,END_DATE
  ,NUM_ID
  ,USER_ID
  ,CAMP_ID
  ,PRODUCT_SKU
  ,SUBSCRIP_START_DATE
  ,SUBSCRIP_CANCEL_DATE
  ,SUBSCRIP_CUR_MON
  ,DEMO_UNIT
  ,CANCEL_UNIT
  ,BILLABLE_UNIT
  ,CREATE_TS
  ,REFRESH_TS
)
select 
   a.ACCOUNT
  ,a.MSISDN
  ,b.BILL_SERV_CD
  ,d.START_DATE
  ,d.END_DATE
  ,row_number() over (order by a.USER_ID) as NUM_ID
  ,a.USER_ID
  ,c.CAMP_ID
  ,c.PRODUCT_SKU
  ,b.BILL_START_DATE
  ,b.BILL_END_DATE
  ,case when trunc(d.SUBR_SW_ON_DATE,'mm') = trunc(add_months(SYSDATE,-1),'mm')
        then d.SUBR_SW_ON_DATE
   else to_date('19000101','YYYYMMDD')
   end as SUBSCRIP_CUR_MON
  ,' ' as DEMO_UNIT
  ,case when trunc(d.SUBR_SW_OFF_DATE,'mm') = trunc(add_months(SYSDATE,-1),'mm')
        then 'Y'
   else 'N'
   end as CANCEL_UNIT
  ,' ' as BILLABLE_UNIT
  ,sysdate
  ,sysdate
from MIG_ADW.NORTON_ACR_MAP a 
left outer join prd_adw.bill_servs b
on a.ACCOUNT = b.CUST_NUM and a.MSISDN = b.SUBR_NUM
   and b.BILL_END_DATE >= &rpt_s_date
   and b.BILL_START_DATE <= &rpt_e_date
left outer join MIG_ADW.NORTON_CAMP_ID_MAP c
on b.BILL_SERV_CD = c.BILL_CODE
   and &rpt_e_date between c.START_DATE and c.END_DATE
left outer join prd_adw.subr_info_hist d
on a.ACCOUNT = d.CUST_NUM and a.MSISDN = d.SUBR_NUM
   and &rpt_e_date between c.START_DATE and c.END_DATE
where c.CAMP_ID is not null;


COMMIT;

ENDOFINPUT

    close(SQLPLUS);
    my $RET_CODE = $? >> 8;
    if ($RET_CODE != 0){
        return 1;
    }else{
        return 0;
    }
}


#We need to have variable input for the program to start
if ($#ARGV < 0){
    print("Syntax : perl <Script Name> <System Name>_<Job Name>_<TXDATE>.dir>\n");
    print("Example: perl b_cust_info0010.pl adw_b_cust_info_20051010.dir\n");
    exit(1);
}


#Call the function we want to run
open(STDERR, ">&STDOUT");

my $pre = etlvar::preProcess($ARGV[0]);
my $rc = etlvar::getTXDate($MASTER_TABLE);
my $ret = runSQLPLUS();
my $post = etlvar::postProcess();

exit($ret);



