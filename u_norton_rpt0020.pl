/opt/etl/prd/etl/APP/RPT/U_NORTON_RPT/bin> cat u_norton_rpt0020.pl
####################################################
#   $Header: 
#   Purpose: The job is created for VAS commission, 2019/05/31 Zeo
#   This is for Outbound
#
######################################################
use Date::Manip;

my $ETLVAR = $ENV{"AUTO_ETLVAR"};
#my $ETLVAR = "/opt/etl/prd/etl/APP/RPT/U_NORTON_RPT/bin/master_dev.pl";
require $ETLVAR;

my $MASTER_TABLE = ""; #Please input the final target ADW table name here

my $DSSVR,$DSUSR,$DSPWD,$DSPROJECT,$DSJOBNAME,$ulog;
my $TABLEDB,$REPFILENAME,$TDSVR,$TDUSR,$TDPWD,$ERRTXDATE;
my $OUTPUT_FILE_PATH,$ENV,$OUTPUT_FILE_NAME;

sub runSQLPLUS{

  #my $SQLCMD_FILE="${etlvar::AUTO_GEN_TEMP_PATH}billing_summary_sqlcmd.sql";
  my $SQLCMD_FILE="/opt/etl/prd/etl/APP/RPT/U_NORTON_RPT/bin/billing_summary_sqlcmd.sql";
  open SQLCMD, ">" . $SQLCMD_FILE || die "Cannot open file" ;
  print SQLCMD<<ENDOFINPUT;

--  ${etlvar::LOGON_TD}
--  ${etlvar::SET_MAXERR}
--  ${etlvar::SET_ERRLVL_1}
--  ${etlvar::SET_ERRLVL_2}
--  ${etlvar::SPOOLOPT}

set head off
set verify off
--set trimspool on
set trimspool off
set newpage 0
set timing off
set pagesize 0
set lines 400
set termout off
set serveroutput off
set feedback off
set echo off
--set trimout on
set trimout off

--Please type your SQL statement here 

 
--set termout off;
--SET LINE 500;

--define tx_date=to_date('${etlvar::TXDATE}','YYYY-MM-DD');

SPOOL '${OUTPUT_FILE_PATH}/${OUTPUT_FILE_NAME}';
--SPOOL '/opt/etl/prd/etl/APP/RPT/U_NORTON_RPT/bin/billing_summary.csv';

SELECT 'MONTH,CAMP_ID,PRODUCT_SKU,DESCRIPTION,BILLABLE_UNITS,UNIT_PRICE,SUB_TTL,CREATE_TS,REFRESH_TS'
FROM DUAL;

select  
  to_char(MONTH,'YYYYMM')
  ||','||CAMP_ID
  ||','||PRODUCT_SKU
  ||','||DESCRIPTION
  ||','||BILLABLE_UNITS
  ||','||UNIT_PRICE
  ||','||SUB_TTL
from MIG_ADW.B_NORTON_RPT_002;

SPOOL OFF;

COMMIT;
quit
ENDOFINPUT
  close(SQLCMD);
  print("sqlplus /\@${etlvar::TDDSN} \@$SQLCMD_FILE");
  my $ret = system("sqlplus /\@${etlvar::TDDSN} \@$SQLCMD_FILE");
  if ($ret != 0)
  {
    return (1);
  }
}

sub initParam{
   # $FILE_DATE = &UnixDate(DateCalc("${etlvar::TXDATE}", "- 0 days", \$err), "%Y%m%d");
   # $RUN_DATE = &UnixDate(DateCalc("${etlvar::TXDATE}", "- 0 days", \$err), "%Y-%m-%d");
    # PARAMETERS -- For Datastage File Export
    #$DSSVR = "${etlvar::DSSVR}";
    #$DSUSR = "${etlvar::DSUSR}";
    #$DSPWD = "${etlvar::DSPWD}";
    #$DSPROJECT = "${etlvar::DSPROJECT}";
    #$DSJOBNAME = "${etlvar::ETLJOBNAME}";
    #$ulog = "${etlvar::ulog}";

    #$TABLEDB = "${etlvar::TMPDB}";
    #$REPFILENAME = "${etlvar::ETLJOBNAME}";
    #$TDSVR = "${etlvar::TDSVR}";
    #$TDUSR = "${etlvar::TDUSR}";
    #$TDPWD = "${etlvar::TDPWD}";

    # Get ERRTXDATE
    my ($esec,$emin,$ehour,$emday,$emon,$eyear,$ewday,$eyday,$eisdst) = localtime(time());
    $eyear += 1900;
    $emon = sprintf("%02d", $emon+1);
    $emday = sprintf("%02d", $emday);
    $ehour = sprintf("%02d",$ehour);
    $emin = sprintf("%02d",$emin);
    $ERRTXDATE = "${emon}${emday}${ehour}";
    
    #$ENV = $ENV{"ETL_ENV"};
    $ENV = "DEV";

    # Get LASTMONTH
    my ($lsec,$lmin,$lhour,$lmday,$lmon,$lyear,$lwday,$lyday,$lisdst) = localtime(time());
    my $yyyymm, $lyear, $lmon;

    $yyyymm = "${etlvar::TXMONTH}";
    $lyear = substr($yyyymm,0,4);
    $lmon = substr($yyyymm,4,2);
    $lmon -= 1;

    if ($lmon == 0){

            $lmon = 12;
                $lyear -= 1;
                $lmon = sprintf("%02d", $lmon);
        }
        else{

        $lmon = sprintf("%02d", $lmon);
        }

    # ------------------------------------------------------------------#
    #  Please define the parameters for this job below.                 #
    # ------------------------------------------------------------------#

    if ($ENV eq "DEV"){

        ##  DEVELOPMENT  ##
        $OUTPUT_FILE_PATH = "/opt/etl/prd/etl/APP/RPT/U_NORTON_RPT/bin";
        $OUTPUT_FILE_NAME = "billing_summary.csv";
    }
    else
    {
        ##  PRODUCTION  ##
        #$OUTPUT_FILE_PATH = "${etlvar::ETL_TMP_DIR_DS}";
        $OUTPUT_FILE_PATH = ${etlvar::ETL_OUTPUT_DIR}."/".${etlvar::ETLSYS}."/".${etlvar::ETLJOBNAME};
        $OUTPUT_FILE_NAME = "billing_summary.csv";
    }
   
   $FTP_TO_DEST_PATH = "/Norton"; 
   $FTP_TO_HOST = "sftp://10.16.9.166 ";
   $FTP_TO_PORT = "22";
   $FTP_TO_USERNAME = "dwftp2";

}

# EMAIL TO User
sub EmailUser{

    print("\n\n\n#####################################\n");
    print("#  Email user\n");
    print("#####################################\n");

    my $TOLIST, $CCLIST, $BCCLIST, $SUBJECT, $ATTACH;

    if ($ENV eq "DEV")
    {
        #$TOLIST = "woody_kwok".q(@)."smartone.com";
        $TOLIST = "Eloise_Wu".q(@)."smartone.com";
        #$CCLIST = "Kevin_ou".q(@)."smartone.com";
        #$BCCLIST = "";
        $ATTACH = "-a ${OUTPUT_FILE_PATH}/${OUTPUT_FILE_NAME}";
    }
     else
    {
        #$TOLIST = etlvar::getEmailList("TOLIST", ${etlvar::ETLJOBNAME});
        #$CCLIST = etlvar::getEmailList("CCLIST", ${etlvar::ETLJOBNAME});
        #$BCCLIST = etlvar::getEmailList("BCCLIST", ${etlvar::ETLJOBNAME});

        ##$TOLIST = "joe_chan".q(@)."smartone.com";
        ##$CCLIST = " ";
        ##$BCCLIST = " ";

        $TOLIST = "Eloise_Wu".q(@)."smartone.com";
        $ATTACH = "-a ${OUTPUT_FILE_PATH}/${OUTPUT_FILE_NAME}";
    }

    $SUBJECT = "billing summary on ${etlvar::TXDATE}";

    #my $rc = open(EMAIL_EOF, "| /usr/local/bin/mutt -s '${SUBJECT}'  ${CCLIST} ${BCCLIST} ${TOLIST} ${ATTACH}");
    my $rc = open(EMAIL_EOF, "| /usr/local/bin/mutt -s '${SUBJECT}' ${TOLIST} ${ATTACH}");
    unless ($rc){
            print "Cound not invoke mutt command\n";
            return -1;
    }

    print EMAIL_EOF<<ENDOFINPUT;
Please check the report file billing_summary.csv which is generated on ${etlvar::TXDATE}.

ENDOFINPUT
    close(EMAIL_EOF);
    my $RET_CODE = $? >> 8;
    if ($RET_CODE != 0){
        return 1;
    }
}

sub sendFile{
    system("lftp $FTP_TO_HOST -p $FTP_TO_PORT -u $FTP_TO_USERNAME<<FOftp
        set ssl:verify-certificate no
        set ftp:ssl-allow true
        set ftp:ssl-force false
        set ftp:ssl-protect-data true
        set ftp:ssl-protect-list true
        cd $FTP_TO_DEST_PATH
        lcd $OUTPUT_FILE_PATH
        put $OUTPUT_FILE_NAME
        quit
        FOftp
        ");
        my $RET_CODE = $? >> 8;
        if ($RET_CODE != 0){
                return 1;
                print("ret=1");
        }else{
                print("ret=0");
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
initParam();
my $ret = runSQLPLUS();

if ($ret == 0)
{
  if (${etlvar::ENABLE_EMAIL} eq "Y") {
    $ret = EmailUser();
    if ($ret == 0){$ret = sendFile();}
  }
}

my $post = etlvar::postProcess();

exit($ret);




