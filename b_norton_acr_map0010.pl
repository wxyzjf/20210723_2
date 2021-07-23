/opt/etl/prd/etl/APP/ADW/B_NORTON_ACR_MAP/bin> cat b_norton_acr_map0010.pl
######################################################
#   $Header: /CVSROOT/SmarTone-Vodafone/Code/ETL/APP/ADW/B_MKT_REF/bin/b_mkt_ref0020.pl,v 1.1 2005/12/14 01:04:03 MichaelNg Exp $
#   Purpose:
#
#
######################################################


my $ETLVAR = $ENV{"AUTO_ETLVAR"};require $ETLVAR;


#We need to have variable input for the program to start
if ($#ARGV < 0){
    print("Syntax : perl <Script Name> <System Name>_<Job Name>_<TXDATE>.dir>\n");
    print("Example: perl d_cust_info001.pl adw_d_cust_info_20051010.dir\n");
    exit(1);
}

#my $TARGET_DB = "$etlvar::ADWDB";
my $TARGET_DB = "MIG_ADW";

my $TARGET_TABLE = "NORTON_ACR_MAP";

#my $SOURCE_DB = "$etlvar::TMPDB";
my $SOURCE_DB = "MIG_ADW";

my $SOURCE_TABLE = "D_NORTON_ACR_MAP";

my $SCRIPT_TYPE = "$etlvar::HIST_SCRIPT_EXPIRE";



#Call the function we want to run
open(STDERR, ">&STDOUT");

my $pre = etlvar::preProcess($ARGV[0]);
my $rc = etlvar::getTXDate($TARGET_TABLE);
my $ret = etlvar::runGenScript($TARGET_DB,$TARGET_TABLE,$SOURCE_DB,$SOURCE_TABLE,$SCRIPT_TYPE,$etlvar::TXDATE);

if ($ret == 0){
    $ret = etlvar::updateJobTXDate($TARGET_TABLE);
}
my $post = etlvar::postProcess($TARGET_TABLE);

exit($ret);


