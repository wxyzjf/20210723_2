/opt/etl/prd/etl/APP/ADW/B_NORTON_RPT/bin> cat b_norton_rpt0020.pl
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

--EXECUTE ${etlvar::UTLDB}.ETL_UTILITY.truncate_tbl2(P_schema_tbl_name=>'${etlvar::TMPDB}.B_NORTON_RPT_002');
EXECUTE ${etlvar::UTLDB}.ETL_UTILITY.truncate_tbl2(P_schema_tbl_name=>'MIG_ADW.B_NORTON_RPT_002');


--Define Code_Type
--INSERT /*+ APPEND */ INTO ${etlvar::TMPDB}.B_NORTON_RPT_002
INSERT /*+ APPEND */ INTO MIG_ADW.B_NORTON_RPT_002
(
   MONTH
  ,CAMP_ID
  ,PRODUCT_SKU
  ,DESCRIPTION
  ,BILLABLE_UNITS
  ,UNIT_PRICE
  ,SUB_TTL
  ,CREATE_TS
  ,REFRESH_TS
)
select
   trunc(add_months(SYSDATE,-1),'mm') as MONTH
  ,a.CAMP_ID
  ,a.PRODUCT_SKU
  ,' ' as DESCRIPTION
  ,NVL(b.BILLABLE_UNITS,0)
  ,a.UNIT_PRICE
  ,NVL(b.BILLABLE_UNITS * a.UNIT_PRICE,0) as SUB_TTL
  ,sysdate
  ,sysdate
from MIG_ADW.NORTON_CAMP_ID_MAP a 
left outer join (select CAMP_ID,count(CAMP_ID) as BILLABLE_UNITS 
                 from MIG_ADW.B_NORTON_RPT_001 group by CAMP_ID)b
on a.CAMP_ID = b.CAMP_ID;

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



