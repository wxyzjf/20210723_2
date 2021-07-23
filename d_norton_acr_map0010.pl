/opt/etl/prd/etl/APP/APS/D_NORTON_ACR_MAP/bin> cat d_norton_acr_map0010.pl
######################################################
#   $Header: 
#   Purpose:
#
#
########################################################
my $ETLVAR = $ENV{"AUTO_ETLVAR"};require $ETLVAR;
#We need to have variable input for the program to start
if ($#ARGV < 0){
    print("Syntax : perl <Script Name> <System Name>_<Job Name>_<TXDATE>.dir>\n");
    print("Example: perl d_cust_info001.pl adw_d_cust_info_20051010.dir\n");
    exit(1);
}


my $MASTER_TABLE = "D_NORTON_ACR_MAP";

#my $LOADING_TABLE_DB = "$etlvar::TMPDB"; 
my $LOADING_TABLE_DB = "MIG_ADW";


$etlvar::DS_MLOAD = "N";  

#Call the function we want to run
open(STDERR, ">&STDOUT");

my $pre = etlvar::preProcess($ARGV[0]);
my $ret = etlvar::runDataStageJob($LOADING_TABLE_DB, $MASTER_TABLE);
my $post = etlvar::postProcess($MASTER_TABLE);
 
exit($ret);
