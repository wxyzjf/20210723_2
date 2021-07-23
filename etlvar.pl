/home/adwbat> cat etlvar.pl
######################################################
#   $Header: /CVSROOT/SmarTone-Vodafone/Code/ETL/etc/etlvar.pl,v 1.55 2008/01/17 03:15:30 KYLee Exp $
#   Purpose: Script contains the global variables
#            and globel functions used for the
#            ETL process
#
#########################################################################################
# History
#
# Date            Who                  Description
# -------------------------------------------------------------------------------------
# 12-AUG-2015     DASH - Henry Wong    Migrate teradata to oracle
#                                      1. collectStatisticByPI
#                                      2. runMultiload
#                                      3. runFastload
#                                      4. runMultiloadWithParam
#                                      5. runFastloadWithParam
#                                      6. checkVariance2
#                                      7. updateValidationLog
#                                      8. removeValidationLog
#                                      9. parseLogFile
#                                      10. preProcess
#                                      11. postProcess 
#                                      12. updateJobTXDate
#                                      13. getTXDate
#                                      14. setTXDate
#                                      15. collectStatistic
#                                      16. runGenScript
#                                      17. connectETL
#                                      18. genFirstDayOfMonth
#                                      19. genSecondDAyOfMonth
#                                      20. genCDRTXDate
#                                      21. genCDRMinTXDate
#                                      22. genCDRMaxTXDate
#                                      23. genETL_BEGIN_CTR_FILE
#                                      24. genEAReport
#                                      25. updateBizTXDate
#                                      26. updateDateAsOfTs
#                                      27. genLast2Month
#                                      28. genLastMonth
#                                      29. gen_rated_cdr_summ_cmlmlm2
#                                      30. gen_rated_cdr_summ_lm3lm4lm5
#                                      31. gen_rated_cdr_summ_lm6lm7lm8
#                                      32. gen_rated_cdr_summ_lm9lm12
#                                      33. genNextStart     
#                                      34. runFastloadWithoutFile
# 04-NOV-2015      DASH - Henry Wong   Add a procedure runTruncateTbl for calling truncate table package 
#                                      that used by datastage procedure in etlvar.pl
# 27-NOV-2015      DASH - Henry Wong   Fix the runGenScript error 
# 17-DEC-2015      DASH - Henry Wong   Add a variable SQLPLUSOPT and set to > /dev/null to suppress large sqlplus output 
#                                      (prevent etlagent crash (note sqlplus option  "SET TERMOUT OFF" cannot be used because it's only 
#                                      applicable for non-interactive mode
# 21-JAN-2016      DASH - Henry Wong   Add runGatherStats procedure for datastage import procedure  
# 12-FEB-2016      DASH - Henry Wong   Modify runGenScript to show number of records insert/update/delete
#                                      using DBMS_OUTPUT.PUT_LINE(sql%rowcount)
# 03-MAR-2016      DASH - Henry Wong    Add alter session force parallel query;
#                                          alter session enable parallel dml;
#                                      In runGenScript to enforce parallel dml
#
# 31-MAR-2016      DASH - Henry Wong   Remove gather statistic under runGenScript  
#                                        gather statistic will be executed automatically
#                                        when using online gather statistic feature
#
# 05-APR-2016      DASH - Henry Wong   Rollback online gather statistic feature to run gather statistics
#                                        because it partition statistic cannot be updated with this feature
#
# 16-APR-2016      DASH - Henry Wong   Add "SET SQLBLANKLINES ON" and "SET SERVEROUTPUT ON" in sqlplus 
#                                       initial parameter to avoid empty line problem.
#
# 09-MAY-2016      DASH - Henry Wong   AUTOGEN_HINT - Set the PARALLEL from 8 to 16
#
# 17-MAY-2016      DASH - Henry Wong   Add alter session to change the default date format for ETL job 
#                                      together with runGenScript
# 22-MAY-2016      DASH - Henry Wong   Change SET HEADING OFF to ON in  SPOOLOPT
#########################################################################################
 

package etlvar;

use DBI;
use Time::Local;


###############################################
#The variables below is read from the 
#environment
###############################################
$ENV = $ENV{"ETL_ENV"};
$ENVIR = $ENV{"ETL_ENV"}; # Have problem using ENV, suspect perl confused with the keyword
$ETLPATH = $ENV{"ETLPATH"};
$TDSVR = $ENV{"TDSVR"};
$DSSVR = $ENV{"DSSVR"};
$DSPROJECT = $ENV{"DSPROJECT"};
# new added for the oracle environment
# to be discussed for the environment variable name 
$TS_FORMAT = "YYYY-MM-DD HH24:MI:SS";

    
###############################################
#Below is the list of the Environment variables
###############################################


##########Database Related Variables##########
$ADWDB = $ENV{"ADWDB"};
$TMPDB = $ENV{"TMPDB"};
$BIZDB = $ENV{"BIZDB"};
$ADWVW = $ENV{"ADWVW"};
$BIZVW = $ENV{"BIZVW"};
$ETLDB = $ENV{"ETLDB"};
$UTLDB = $ENV{"UTLDB"};
$TDDSN = $ENV{"AUTO_DSN"};
$ERRDB = "${ENV}_ERR";
$MIGDB = "MIG_ADW";
$TDUSR = "";
$TDPWD = "";
$LOGON_TD = "";
$BIZVW_PWD ="smartone";


#########ETL Automation Related Variables#########
$ETLSYS = "";
$CONTROL_FILE = "";
$ETL_PROCESS_DIR_CTL = "${ETLPATH}/DATA/process/";
$ETL_PROCESS_DIR = "${ETLPATH}/DATA/process/";
$ETL_RECEIVE_DIR = "${ETLPATH}/DATA/receive/";
$ETL_TMP_DIR = "${ETLPATH}/tmp";
$ETL_TMP_DIR_DS = "${ETLPATH}/tmp";
$ETL_DUP_DIR = "${ETLPATH}/duplicate_records";
$ETL_ETC_DIR = "${ETLPATH}/etc";
$ETL_RESER_WORD_LIST = "${ETL_ETC_DIR}/td_reserved_word_list";  # Contains the list of reserved words to be masked
$ETL_OUTPUT_DIR = "/opt/etl/output";
$ETL_JOB_LIMIT = "${ETL_ETC_DIR}/etl_job_limit"; 
# path for etl06
# $ETL_MUTT = "/usr/bin/mutt";
# path from original server
$ETL_MUTT = "/usr/local/bin/mutt";
$DS_MLOAD = "N";
$BT_TBL_SIZE = 100;
$CS_TABLE_SIZE = 100000;
$SWITCH_TBL_LOCK_RETRY_SEC = 60;
$ERROR_TBL_RETENTION_DAYS = 30;
$AUTO_GEN_SCRIPT_RETENTION_DAYS = 30;


    ####################################################
    #This is only used for development; 
    #$ETL_PROCESS_DIR_CTL = "/opt/etl/dev/etl/DATA/process/";
    #$ETL_PROCESS_DIR = "/opt/etl/dev/etl/DATA/process/";
    #$ETL_TMP_DIR_DS = "/opt/etl/dev/etl/tmp";
    #################################################### 





$DSUSR = "";
$DSPWD = "";
$TOTAL_INS = 0;
$TOTAL_DUP = 0;
$TOTAL_UPD = 0;
$TOTAL_DEL = 0;
$TOTAL_OUT = 0;
$TOTAL_ER1 = 0;
$TOTAL_ER2 = 0;
$TOTAL_ET = 0;
$TOTAL_UV = 0;

##########Path Related Variables##########
$AUTO_GEN_TEMP_PATH = "${ETLPATH}/auto_gen/";

##########Other Variables##########
$LOADTIME = "";
$ENTRYDATE = "";
$ENTRYTIME = "";
$SCRIPTGENTIME = "";
$TXDATE = "";
$TXMONTH = "";
$ERRTXDATE = "";
$TABLEDB = "";
$AUTOGEN_HINT = " PARALLEL(16) ";
@F_D_MONTH;
@RESER_WORD_LIST = ();          # List of reserved words to be checked



##########Fixed Variables##########
$REPLACE_SCRIPT = 1;
$UPSERT_SCRIPT = 2;
$APPEND_SCRIPT = 3;
$HIST_SCRIPT_NON_EXPIRE = 4;
$HIST_SCRIPT_EXPIRE = 5;
$SWITCH_POSTFIX_NEW = "_NEW";
$SWITCH_POSTFIX_OLD = "_OLD";
$SET_MAXERR = "";
# $SET_ERRLVL_1 = "";
$SET_ERRLVL_1 = "whenever sqlerror exit 2;\n";
$SET_ERRLVL_2 = "set serveroutput on;\n set sqlblanklines on;\n set autocommit 1;\n set timing on; \n set echo on; \n set sqlprompt ' '; \n set sqlnumber off; \n set define off; \n ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'; \n ";      #Reserved
$SET_ERRLVL_3 = "set echo on;"; #Only used for Auto_Gen when the reference object does not exist
# SQLPLUS spool file option
$SPOOLOPT = "SET HEADING ON;\n SET LINE 500;\n SET PAGES 0;\n SET HEADING OFF;\n SET SERVEROUTPUT OFF;\n SET FEED OFF;\n SET VERIFY OFF;\n SET TIMING OFF;\n set echo off;\n SET TERM ON;\n SET TRIMSPOOL ON;\n";

# subpress all sqlplus output to prevent etlagent crash
$SQLPLUSOPT = " "; # " > /dev/null ";

# Global enable FTP command in ETL job (Y/N)
$ENABLE_FTP = "Y";

# Global enable Copy command in ETL job (Y/N)
$ENABLE_COPY = "Y";

# Global enable Email command (mutt) in ETL job (Y/N)
$ENABLE_EMAIL = "Y";


$MAXDATE = "2999-12-31";
$MINDATE = "1900-01-01";
$ulog = q(2>&1);



#Get the current loading time
my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());
$year += 1900;
$mon = sprintf("%02d", $mon+1);
$mday = sprintf("%02d", $mday);
$hour = sprintf("%02d",$hour);
$min = sprintf("%02d",$min);
$sec = sprintf("%02d",$sec);
$LOADTIME = "${year}-${mon}-${mday} ${hour}:${min}:${sec}";
$SUMMLOADDATE = "${year}-${mon}-${mday}";
$ENTRYDATE = "${year}${mon}${mday}";
$ENTRYTIME = "${hour}:${min}:${sec}";
$SCRIPTGENTIME = "${year}${mon}${mday}${hour}${min}${sec}";
my $SUMM_CHANGE_DAY = "${mday}";




#Generate the logon string
sub getLogonString
{
    open (LOGONF, "${ETLPATH}/etc/etl_logon");
    my $logon = <LOGONF>;
    close(LOGONF);
    
    $logon =~ s/([\n\.\;])//g;
    $logon =~ s/([^ ]*) *([^ ]*)/$2/;
    my ($user , $passwd) = split(',' , $logon);

    # We decode the password in ICE algorithm
    my $decodepass = `${ETLPATH}/bin/IceCode -d "$passwd" "$user"`;
    
    $TDUSR = $user;
    $TDPWD = $decodepass;
    $LOGON_TD = ".logon $TDSVR/$TDUSR,$TDPWD;";
    
    
    open (LOGONF, "${ETLPATH}/etc/ds_logon");
    my $logon = <LOGONF>;
    close(LOGONF);
    
    $logon =~ s/([\n\.\;])//g;
    $logon =~ s/([^ ]*) *([^ ]*)/$2/;
    my ($duser , $dpasswd) = split(',' , $logon);

    # We decode the password in ICE algorithm
    $decodepass = `${ETLPATH}/bin/IceCode -d "$dpasswd" "$duser"`;
    
    $DSUSR = $duser;
    $DSPWD = $decodepass;
        
}





###############################################
#Functions
###############################################





sub collectStatisticByPI
{
    my ($CS_TABLE_NAME,$CS_DB) = @_;
   
    # Oracle - Temporary by-pass this function 
    return 0;
    
    my $AUTO_GEN_SCRIPT = "";
    
    if ($CS_DB eq ""){
        $CS_DB = $ADWDB;
    }
    
    $AUTO_GEN_SCRIPT = lc "${AUTO_GEN_TEMP_PATH}${CS_DB}_${CS_TABLE_NAME}_${SCRIPTGENTIME}.collectStatisticbyPI";
    
    open DF, ">" . $AUTO_GEN_SCRIPT || die "Can not open " . $AUTO_GEN_SCRIPT;
    
    
    #The following script is used to generated the header of the auto-gen script
    print DF "--This collect statistic scirpt is generated by AUTO_GEN function\n";
    
    
    print DF "$LOGON_TD \n\n";
    print DF "$SET_MAXERR\n";
    print DF "$SET_ERRLVL_1\n";
    print DF "$SET_ERRLVL_3\n\n";
    
    
    
    my $DBH = connectETL();

    unless ( defined($DBH) ) 
    {
        print "ERROR - Unable to connect to TD!\n";
        my $ERRSTR = $DBI::errstr;
        print "$ERRSTR\n";
        return -1;
    }

    my $STH;
   
    
    #Select the related collect statistics statement
        
    my $SQLTEXT = "";
    $SQLTEXT = $SQLTEXT . "WITH RECURSIVE PI_List (databasename, tablename, collist, columnposition, Depth) AS ";
    $SQLTEXT = $SQLTEXT . "( ";
    $SQLTEXT = $SQLTEXT . "SELECT databasename, tablename,cast(columnname as varchar(1000)), ";
    $SQLTEXT = $SQLTEXT . "1,0 FROM dbc.indices where indextype in ('P','Q') and columnposition = 1 ";
    $SQLTEXT = $SQLTEXT . "UNION ALL ";
    $SQLTEXT = $SQLTEXT . "SELECT PI_List.databasename, ";
    $SQLTEXT = $SQLTEXT . "PI_List.tablename, ";
    $SQLTEXT = $SQLTEXT . "PI_List.collist || ',' || trim(aa.columnname), ";
    $SQLTEXT = $SQLTEXT . "aa.columnposition, ";
    $SQLTEXT = $SQLTEXT . "PI_List.Depth + 1 ";
    $SQLTEXT = $SQLTEXT . "FROM PI_List, dbc.indices aa/*Recursive reference*/ ";
    $SQLTEXT = $SQLTEXT . "WHERE ";
    $SQLTEXT = $SQLTEXT . "PI_List.databasename = aa.databasename ";
    $SQLTEXT = $SQLTEXT . "and PI_List.tablename = aa.tablename ";
    $SQLTEXT = $SQLTEXT . "and PI_List.columnposition +1 = aa.columnposition ";
    $SQLTEXT = $SQLTEXT . "and aa.indextype in ('P','Q') ";
    $SQLTEXT = $SQLTEXT . "and PI_LIST.depth < 50 ";
    $SQLTEXT = $SQLTEXT . ") ";
    $SQLTEXT = $SQLTEXT . "SELECT collist, ";
    $SQLTEXT = $SQLTEXT . "CASE WHEN (b.Sum_Currentperm > ${CS_TABLE_SIZE}) THEN 'Y' ELSE 'N' END , ";
    $SQLTEXT = $SQLTEXT . "'I' FROM PI_List a , ";
    $SQLTEXT = $SQLTEXT . "(Select   databasename, tablename, Sum(currentperm) as Sum_Currentperm From dbc.allspace Group     By 1,2) b  ";
    $SQLTEXT = $SQLTEXT . "where  ";
    $SQLTEXT = $SQLTEXT . "a.databasename = '${CS_DB}' ";
    $SQLTEXT = $SQLTEXT . "and a.tablename = '${CS_TABLE_NAME}' ";
    $SQLTEXT = $SQLTEXT . "and a.databasename = b.databasename ";
    $SQLTEXT = $SQLTEXT . "and a.tablename = b.tablename ";
    $SQLTEXT = $SQLTEXT . "qualify rank() over (partition by a.databasename, a.tablename order by a.depth desc) = 1 ";
    
    
    print ("\n$SQLTEXT\n");
    
    $STH = $DBH->prepare($SQLTEXT);
    unless ($STH) 
    {
        return -1;
    }

    $STH->execute();

    my @TABROW;
    my $result_count = 0;

    while ( @TABROW = $STH->fetchrow() ) 
    {
        $result_count = $result_count + 1;
        my $CS_Column = $TABROW[0];
        my $CS_Sample = $TABROW[1];
        my $CS_Type = $TABROW[2];
        
        
        foreach $KEYW (@RESER_WORD_LIST)
        {
                        if (lc($CS_Column) eq $KEYW) {  # Only exact match of reserved word needs to be masked
                                print "Found reserved word in PI [$CS_Column]\n";
                                $CS_Column = q(") . $CS_Column . q(");
                                last;
                        }
                }
        
        if ((uc($TABROW[1]) eq "Y") && (uc($TABROW[2]) eq "I")){
            print DF "COLLECT STATISTICS USING SAMPLE ON ";
            print DF $CS_DB . "." . $CS_TABLE_NAME . " INDEX (" . $CS_Column . ");\n";      
        }
        
        if ((uc($TABROW[1]) eq "N") && (uc($TABROW[2]) eq "I")){
            print DF "COLLECT STATISTICS ON ";
            print DF $CS_DB . "." . $CS_TABLE_NAME . " INDEX (" . $CS_Column . ");\n";      
        }
        
        
    }
    $STH->finish();
    disconnectETL($DBH);

    
    close DF;
    
    
    my $rc;
    
    if ($result_count == 0){
        $rc = 0;
    }else {
        $rc = system("bteq < $AUTO_GEN_SCRIPT $ulog");
    }
    
    if ($rc != 0) 
    { 
        return 1;
    }
    else
    {
        return 0;
    }    
}



sub runDataStageJob
{
    my ($TDDBN, $MSTABLENAME) = @_;
    my $rc;
    my $rc_stats;
    my $FILE_PROCESS_DATE = "";
    
    
    open (DS, "${ETL_PROCESS_DIR_CTL}${CONTROL_FILE}") || die "Can not open " . $CONTROL_FILE;
    @DATA_FILE_LIST = <DS>;
    close(DS);

    foreach $DATAFILE (@DATA_FILE_LIST) {
        my @DFNAME = split(/ /,$DATAFILE);
        $FILE_PROCESS_DATE = substr($DFNAME[0],-8);
    }
    
    $rc = runTruncateTbl($TDDBN,$ETLJOBNAME);   
 
    if (lc $DS_MLOAD eq "y"){
        my $PATTERN = lc $ETLJOBNAME . "_*.dat*";
        $rc = runMultiload($TDDBN, $MSTABLENAME, $PATTERN);
    }
    else 
    {
        $rc = runFastload($TDDBN, $MSTABLENAME);
    }
   
    $rc_stats = runGatherStats($TDDBN,$ETLJOBNAME); 

    
    return ($rc);
}


sub runMultiload
{
    my ($TDDBN, $MSTABLENAME, $PATTERN) = @_;
    my $rc;
    my @DATA_FILE_LIST;
    my $REPFILENAME = $ETLJOBNAME;
    my $DSJOBNAME = $ETLJOBNAME;
    my @ROW_CNT;
    $TABLEDB = $TDDBN;
    
    $ERRTXDATE = "";
    
    # print("\n hw testing in runMultiload start \n"); 
    my $rc = open(SQLPLUS, "| sqlplus /\@${TDDSN}");
    unless ($rc){
        print "Cound not invoke sqlplus command\n";
        return -1;
    }

    print SQLPLUS<<ENDOFINPUT;
        DELETE FROM ${TDDBN}.${ETLJOBNAME};
        COMMIT;
        EXIT;

ENDOFINPUT

    close(SQLPLUS);
    my $RET_CODE = $? >> 8;
    if ($RET_CODE != 0){
        return 1;
    }
    
    
        my ($esec,$emin,$ehour,$emday,$emon,$eyear,$ewday,$eyday,$eisdst) = localtime(time());
        $eyear += 1900;
        $emon = sprintf("%02d", $emon+1);
        $emday = sprintf("%02d", $emday);
        $ehour = sprintf("%02d",$ehour);
        $emin = sprintf("%02d",$emin);
        $ERRTXDATE = "${emon}${emday}${ehour}";
    
    
    
        system("rm ${ETL_TMP_DIR}/${REPFILENAME}.log $ulog");
      

        $rc = 0;
        my $run_count = 1;

        $rc = system("${DS_JOB_PATH}dsjob -run -mode RESET $DSPROJECT $DSJOBNAME $ulog"); 
        sleep(10);    
        # teradata dsjob syntax 
        # $rc = system("${DS_JOB_PATH}dsjob -run -mode NORMAL -param TDDB=$TABLEDB -param TDTABLE=$DSJOBNAME -param REPFILENAME=$REPFILENAME -param FILEPATTERN=${ETL_PROCESS_DIR}$PATTERN -param TDSVR=$TDSVR -param TDUSR=$TDUSR -param TDPWD=$TDPWD -param OUTPUT_FILE_PATH=${ETL_TMP_DIR_DS} -param ERRTXDATE=$ERRTXDATE -param ENV=$ENV -wait -jobstatus $DSPROJECT $DSJOBNAME $ulog");
        # oracle version - remove parameter of ERRTXDATA and append $TABLEDB in TDTABLE parameter
        $rc = system("${DS_JOB_PATH}dsjob -run -mode NORMAL -param TDTABLE=$TABLEDB.$DSJOBNAME -param REPFILENAME=$REPFILENAME -param FILEPATTERN=${ETL_PROCESS_DIR}$PATTERN -param TDSVR=${TDSVR} -param TDUSR= -param TDPWD= -param OUTPUT_FILE_PATH=${ETL_TMP_DIR_DS} -param ENV=$ENV -wait -jobstatus $DSPROJECT $DSJOBNAME $ulog");

        while ( $run_count < 2 && $rc == 768 )
        {   
            $run_count++;
            sleep(900);

            $rc = system("${DS_JOB_PATH}dsjob -run -mode RESET $DSPROJECT $DSJOBNAME $ulog"); 
            sleep(10);     
            # teradata dsjob syntax 
            # $rc = system("${DS_JOB_PATH}dsjob -run -mode NORMAL -param TDDB=$TABLEDB -param TDTABLE=$DSJOBNAME -param REPFILENAME=$REPFILENAME -param FILEPATTERN=${ETL_PROCESS_DIR}$PATTERN -param TDSVR=$TDSVR -param TDUSR=$TDUSR -param TDPWD=$TDPWD -param OUTPUT_FILE_PATH=${ETL_TMP_DIR_DS} -param ERRTXDATE=$ERRTXDATE -param ENV=$ENV -wait -jobstatus $DSPROJECT $DSJOBNAME $ulog");
            # oracle version - remove parameter of ERRTXDATA and append $TABLEDB in TBTABLE parameter
            $rc = system("${DS_JOB_PATH}dsjob -run -mode NORMAL -param TDTABLE=$TABLEDB.$DSJOBNAME -param REPFILENAME=$REPFILENAME -param FILEPATTERN=${ETL_PROCESS_DIR}$PATTERN -param TDSVR=${TDSVR} -param TDUSR= -param TDPWD= -param OUTPUT_FILE_PATH=${ETL_TMP_DIR_DS} -param ENV=$ENV -wait -jobstatus $DSPROJECT $DSJOBNAME $ulog");
        }

                
        if ($rc == 256) 
        {
            print("\n\n\n#####################################\n");
            print("#Teradata Fastload/Multiload Job log\n");
            print("#DataFile: $DFNAME[0]\n");
            print("#####################################\n\n");
            ##`echo "\$ getdslog" | ftp bill02`;
            my $rfile = $REPFILENAME;
            system("mv ${ETL_TMP_DIR}/${rfile}*.txt ${ETL_TMP_DIR}/${rfile}.log $ulog");
            system("more ${ETL_TMP_DIR}/${rfile}.log $ulog");
            print("\n\n");
            parseLogFile("${ETL_TMP_DIR}/${rfile}.log");
        }
        elsif ($rc == 512) 
        {
            
            print("\n\n\n#####################################\n");
            print("#DataStage complete with warnings\n\n");
            print("#Teradata Fastload/Multiload Job log\n");
            print("#DataFile: $DFNAME[0]\n");
            print("#####################################\n\n");
            ##`echo "\$ getdslog" | ftp bill02`;
            my $rfile = $REPFILENAME;
            system("mv ${ETL_TMP_DIR}/${rfile}*.txt ${ETL_TMP_DIR}/${rfile}.log $ulog");
            system("more ${ETL_TMP_DIR}/${rfile}.log $ulog");
            print("\n\n");
            parseLogFile("${ETL_TMP_DIR}/${rfile}.log");
        }
        else
        {
            ##print("Fail: ${DS_JOB_PATH}dsjob -run -mode NORMAL -param TDDB=$TABLEDB -param TDTABLE=$DSJOBNAME -param REPFILENAME=$REPFILENAME -param FILEPATTERN=${ETL_PROCESS_DIR}$PATTERN -param TDSVR=$TDSVR -param TDUSR=$TDUSR -param TDPWD=$TDPWD -param OUTPUT_FILE_PATH=${ETL_TMP_DIR_DS} -param ERRTXDATE=$ERRTXDATE -param ENV=$ENV -wait -jobstatus $DSPROJECT $DSJOBNAME $ulog");
            print("Fail: $DSPROJECT $DSJOBNAME (return code: $rc)\n");
            return (1);   
        }

    
    $rc = updateValidationLog();
    
    if ($rc != 0)
    {
        print ("Update Validation Log fail!\n");
        removeValidationLog();
        return(1);    
    }
    
    $rc = checkVariance();
    
    if ($rc != 0)
    {
        removeValidationLog();
        return(1);    
    }
    
    $rc = setTXDate($MSTABLENAME, $TABLEDB);
    
    if ($rc != 0)
    {
        print ("\n\n\nUpdate transaction date fail!\n");
        removeValidationLog();
        return(1);    
    }
    
    return 0;
}



sub runFastload
{
    my ($TDDBN, $MSTABLENAME) = @_;
    my $rc;
    my @DATA_FILE_LIST;
    my $REPFILENAME = $ETLJOBNAME;
    my $DSJOBNAME = $ETLJOBNAME;
    my @ROW_CNT;
    $TABLEDB = $TDDBN;
    
    $ERRTXDATE = "";
    print ("Current control file: ${ETL_PROCESS_DIR_CTL}${CONTROL_FILE} \n");    
    open (DS, "${ETL_PROCESS_DIR_CTL}${CONTROL_FILE}") || die "Can not open " . $CONTROL_FILE;
    @DATA_FILE_LIST = <DS>;
    close(DS);


#    print("hello: $DATA_FILE_LIST[0]");

    foreach $DATAFILE (@DATA_FILE_LIST) {

        my ($esec,$emin,$ehour,$emday,$emon,$eyear,$ewday,$eyday,$eisdst) = localtime(time());
        $eyear += 1900;
        $emon = sprintf("%02d", $emon+1);
        $emday = sprintf("%02d", $emday);
        $ehour = sprintf("%02d",$ehour);
        $emin = sprintf("%02d",$emin);
        $ERRTXDATE = "${emon}${emday}${ehour}${emin}";
    
#        print ("hw DATAFILE: $DATAFILE"); 
        system("rm ${ETL_TMP_DIR}/${REPFILENAME}.log $ulog");
        my @DFNAME = split(/ /,$DATAFILE);


#         $rc = 0;
#         my $run_count = 0;

#         while ( $run_count < 2 && $rc != 256 && $rc != 512 )
#         {   
#             $run_count++;
#             if ($run_count == 2) 
#             { sleep(900); }

            $rc = system("${DS_JOB_PATH}dsjob -run -mode RESET $DSPROJECT $DSJOBNAME $ulog");     
            sleep(10);  
            # teradata - modify the param when using oracle wallet 
            # $rc = system("${DS_JOB_PATH}dsjob -run -mode NORMAL -param TDDB=$TABLEDB -param TDTABLE=$DSJOBNAME -param REPFILENAME=$REPFILENAME -param SRCFILENAME=${ETL_PROCESS_DIR}$DFNAME[0] -param TDSVR=$TDSVR -param TDUSR=$TDUSR -param TDPWD=$TDPWD -param OUTPUT_FILE_PATH=${ETL_TMP_DIR_DS} -param ERRTXDATE=$ERRTXDATE -param ENV=$ENV -wait -jobstatus $DSPROJECT $DSJOBNAME $ulog");
            # oracle version - remove the parameter of TDDB, TDSRV, TDUSR, TDPWD, ERRTXDATE and add ETL06UATDBUser parameter 
            $DFNAME[0]=~s/\n//g;
            print (" Call dsjob with the following parameters: \n");
            print (" -param TDTABLE=$TABLEDB.$DSJOBNAME \n");
            print (" -param REPFILENAME=$REPFILENAME \n");
            print (" -param SRCFILENAME=${ETL_PROCESS_DIR}$DFNAME[0] \n");
            print (" -param TDSVR=${TDSVR} \n");
            print (" -param TDUSR= \n");
            print (" -param TDPWD= \n");
            print (" -param OUTPUT_FILE_PATH=${ETL_TMP_DIR_DS} \n");
            print (" -param ENV=$ENV \n");

       

            $rc = system("${DS_JOB_PATH}dsjob -run -mode NORMAL -param TDTABLE=$TABLEDB.$DSJOBNAME -param REPFILENAME=$REPFILENAME -param SRCFILENAME=${ETL_PROCESS_DIR}$DFNAME[0] -param TDSVR=${TDSVR} -param TDUSR= -param TDPWD= -param OUTPUT_FILE_PATH=${ETL_TMP_DIR_DS} -param ENV=$ENV -wait -jobstatus $DSPROJECT $DSJOBNAME $ulog");
#         }

        # print("\n this is a dummy line after dsjob...\n";


        if ($rc == 256) 
        {
            print("\n\n\n#####################################\n");
            print("#Teradata Fastload/Multiload Job log\n");
            print("#DataFile: $DFNAME[0]\n");
            print("#####################################\n\n");
            ##`echo "\$ getdslog" | ftp bill02`;
            my $rfile = $REPFILENAME;
            system("mv ${ETL_TMP_DIR}/${rfile}*.txt ${ETL_TMP_DIR}/${rfile}.log $ulog");
            system("more ${ETL_TMP_DIR}/${rfile}.log $ulog");
            print("\n\n");
            parseLogFile("${ETL_TMP_DIR}/${rfile}.log");
        }
        elsif ($rc == 512) 
        {
            
            print("\n\n\n#####################################\n");
            print("#DataStage complete with warnings\n\n");
            print("#Teradata Fastload/Multiload Job log\n");
            print("#DataFile: $DFNAME[0]\n");
            print("#####################################\n\n");
            ##`echo "\$ getdslog" | ftp bill02`;
            my $rfile = $REPFILENAME;
            system("mv ${ETL_TMP_DIR}/${rfile}*.txt ${ETL_TMP_DIR}/${rfile}.log $ulog");
            system("more ${ETL_TMP_DIR}/${rfile}.log $ulog");
            print("\n\n");
            parseLogFile("${ETL_TMP_DIR}/${rfile}.log");
        }
        else
        {
            ##print("Fail: ${DS_JOB_PATH}dsjob -run -mode NORMAL -param TDDB=$TABLEDB -param REPFILENAME=$REPFILENAME -param SRCFILENAME=${ETL_PROCESS_DIR}$DFNAME[0] -param TDSVR=$TDSVR -param TDUSR=$TDUSR -param TDPWD=$TDPWD -param OUTPUT_FILE_PATH=${ETL_TMP_DIR} -param ERRTXDATE=$ERRTXDATE -param ENV=$ENV -wait -jobstatus $DSPROJECT $DSJOBNAME $ulog");
            print("Fail: $DSPROJECT $DSJOBNAME (return code: $rc)\n");
            return (1);   
        }
    }
    
    
    
    if ($TOTAL_DUP != 0)
    {   
        system("${ETL_ETC_DIR}/check_dup.ksh $ETLJOBNAME $CONTROL_FILE $ETL_PROCESS_DIR $ETL_DUP_DIR");
        print("${ETL_ETC_DIR}/check_dup.ksh $ETLJOBNAME $CONTROL_FILE $ETL_PROCESS_DIR $ETL_DUP_DIR\n");
        print("There are duplicated rows in the data file\n");
        return(1);   
    }    
    
    
    $rc = updateValidationLog();
    
    if ($rc != 0)
    {
        print ("Update Validation Log fail!\n");
        removeValidationLog();
        return(1);    
    }
    
    $rc = checkVariance();
    
    if ($rc != 0)
    {
        removeValidationLog();
        return(1);    
    }
    
    $rc = setTXDate($MSTABLENAME, $TABLEDB);
    
    if ($rc != 0)
    {
        print ("\n\n\nUpdate transaction date fail!\n");
        removeValidationLog();
        return(1);    
    }
    
    return 0;
}



########################

#
# Run DataStage Job with extra parameters
# $EXTRAPARAM: "-param PARAM_1_NAME=PARAM_1_VALUE -param PARAM_2_NAME=PARAM_2_VALUE ..."
#
sub runDataStageJobWithParam
{
    my ($TDDBN, $MSTABLENAME, $EXTRAPARAM) = @_;
    my $rc;
    my $rc_stats;
    my $FILE_PROCESS_DATE = "";
    
    
    open (DS, "${ETL_PROCESS_DIR_CTL}${CONTROL_FILE}") || die "Can not open " . $CONTROL_FILE;
    @DATA_FILE_LIST = <DS>;
    close(DS);

    foreach $DATAFILE (@DATA_FILE_LIST) {
        my @DFNAME = split(/ /,$DATAFILE);
        $FILE_PROCESS_DATE = substr($DFNAME[0],-8);
    
    }
    
    $rc = runTruncateTbl($TDDBN,$ETLJOBNAME);    
    
    if (lc $DS_MLOAD eq "y"){
        my $PATTERN = lc $ETLJOBNAME . "_*.dat*";
        $rc = runMultiloadWithParam($TDDBN, $MSTABLENAME, $PATTERN, $EXTRAPARAM);
    }
    else 
    {
        $rc = runFastloadWithParam($TDDBN, $MSTABLENAME, $EXTRAPARAM);
    }
    
    $rc_stats = runGatherStats($TDDBN,$ETLJOBNAME);
    
    return ($rc);
}




sub runMultiloadWithParam
{
    my ($TDDBN, $MSTABLENAME, $PATTERN, $EXTRAPARAM) = @_;
    my $rc;
    my @DATA_FILE_LIST;
    my $REPFILENAME = $ETLJOBNAME;
    my $DSJOBNAME = $ETLJOBNAME;
    my @ROW_CNT;
    $TABLEDB = $TDDBN;
    
    $ERRTXDATE = "";
    
    my $rc = open(SQLPLUS, "| sqlplus /\@${TDDSN}");
    unless ($rc){
        print "Cound not invoke sqlplus command\n";
        return -1;
    }

    print SQLPLUS<<ENDOFINPUT;
    DELETE FROM ${TDDBN}.${ETLJOBNAME};
    COMMIT;
    EXIT;

ENDOFINPUT

    close(SQLPLUS);
    my $RET_CODE = $? >> 8;
    if ($RET_CODE != 0){
        return 1;
    }
    
        my ($esec,$emin,$ehour,$emday,$emon,$eyear,$ewday,$eyday,$eisdst) = localtime(time());
        $eyear += 1900;
        $emon = sprintf("%02d", $emon+1);
        $emday = sprintf("%02d", $emday);
        $ehour = sprintf("%02d",$ehour);
        $emin = sprintf("%02d",$emin);
        $ERRTXDATE = "${emon}${emday}${ehour}";
    
        system("rm ${ETL_TMP_DIR}/${REPFILENAME}.log $ulog");
      
    
        $rc = 0;
        my $run_count = 1;

        $rc = system("${DS_JOB_PATH}dsjob -run -mode RESET $DSPROJECT $DSJOBNAME $ulog"); 
        sleep(10);     
        # teradata syntax 
        # $rc = system("${DS_JOB_PATH}dsjob -run -mode NORMAL -param TDDB=$TABLEDB -param TDTABLE=$DSJOBNAME -param REPFILENAME=$REPFILENAME -param FILEPATTERN=${ETL_PROCESS_DIR}$PATTERN -param TDSVR=$TDSVR -param TDUSR=$TDUSR -param TDPWD=$TDPWD -param OUTPUT_FILE_PATH=${ETL_TMP_DIR_DS} -param ERRTXDATE=$ERRTXDATE -param ENV=$ENV $EXTRAPARAM -wait -jobstatus $DSPROJECT $DSJOBNAME $ulog");
        # oracle version - remove ERRTXDATA and append $TABLEDB into TDTABLE
        print("${DS_JOB_PATH}dsjob -run -mode NORMAL -param TDTABLE=$TABLEDB.$DSJOBNAME -param REPFILENAME=$REPFILENAME -param FILEPATTERN=${ETL_PROCESS_DIR}$PATTERN -param TDSVR=${TDSVR} -param TDUSR= -param TDPWD= -param OUTPUT_FILE_PATH=${ETL_TMP_DIR_DS} -param ERRTXDATE=$ERRTXDATE -param ENV=$ENV $EXTRAPARAM -wait -jobstatus $DSPROJECT $DSJOBNAME $ulog");
        $rc = system("${DS_JOB_PATH}dsjob -run -mode NORMAL -param TDTABLE=$TABLEDB.$DSJOBNAME -param REPFILENAME=$REPFILENAME -param FILEPATTERN=${ETL_PROCESS_DIR}$PATTERN -param TDSVR=${TDSVR} -param TDUSR= -param TDPWD= -param OUTPUT_FILE_PATH=${ETL_TMP_DIR_DS} -param ERRTXDATE=$ERRTXDATE -param ENV=$ENV $EXTRAPARAM -wait -jobstatus $DSPROJECT $DSJOBNAME $ulog");

        while ( $run_count < 2 && $rc == 768 )
        {   
            $run_count++;
            sleep(900);

            $rc = system("${DS_JOB_PATH}dsjob -run -mode RESET $DSPROJECT $DSJOBNAME $ulog"); 
            sleep(10);     
            # teradata version
            # $rc = system("${DS_JOB_PATH}dsjob -run -mode NORMAL -param TDDB=$TABLEDB -param TDTABLE=$DSJOBNAME -param REPFILENAME=$REPFILENAME -param FILEPATTERN=${ETL_PROCESS_DIR}$PATTERN -param TDSVR=$TDSVR -param TDUSR=$TDUSR -param TDPWD=$TDPWD -param OUTPUT_FILE_PATH=${ETL_TMP_DIR_DS} -param ERRTXDATE=$ERRTXDATE -param ENV=$ENV $EXTRAPARAM -wait -jobstatus $DSPROJECT $DSJOBNAME $ulog");
            # oracle version - - remove ERRTXDATA and append $TABLEDB into TDTABLE
            $rc = system("${DS_JOB_PATH}dsjob -run -mode NORMAL -param TDTABLE=$TABLEDB.$DSJOBNAME -param REPFILENAME=$REPFILENAME -param FILEPATTERN=${ETL_PROCESS_DIR}$PATTERN -param TDSVR=$TDSVR -param TDUSR= -param TDPWD= -param OUTPUT_FILE_PATH=${ETL_TMP_DIR_DS} -param ENV=$ENV $EXTRAPARAM -wait -jobstatus $DSPROJECT $DSJOBNAME $ulog");
        }

                
        if ($rc == 256) 
        {
            print("\n\n\n#####################################\n");
            print("#Teradata Fastload/Multiload Job log\n");
            print("#DataFile: $DFNAME[0]\n");
            print("#####################################\n\n");
            ##`echo "\$ getdslog" | ftp bill02`;
            my $rfile = $REPFILENAME;
            system("mv ${ETL_TMP_DIR}/${rfile}*.txt ${ETL_TMP_DIR}/${rfile}.log $ulog");
            system("more ${ETL_TMP_DIR}/${rfile}.log $ulog");
            print("\n\n");
            parseLogFile("${ETL_TMP_DIR}/${rfile}.log");
        }
        elsif ($rc == 512) 
        {
            
            print("\n\n\n#####################################\n");
            print("#DataStage complete with warnings\n\n");
            print("#Teradata Fastload/Multiload Job log\n");
            print("#DataFile: $DFNAME[0]\n");
            print("#####################################\n\n");
            ##`echo "\$ getdslog" | ftp bill02`;
            my $rfile = $REPFILENAME;
            system("mv ${ETL_TMP_DIR}/${rfile}*.txt ${ETL_TMP_DIR}/${rfile}.log $ulog");
            system("more ${ETL_TMP_DIR}/${rfile}.log $ulog");
            print("\n\n");
            parseLogFile("${ETL_TMP_DIR}/${rfile}.log");
        }
        else
        {
            ##print("Fail: ${DS_JOB_PATH}dsjob -run -mode NORMAL -param TDDB=$TABLEDB -param TDTABLE=$DSJOBNAME -param REPFILENAME=$REPFILENAME -param FILEPATTERN=${ETL_PROCESS_DIR}$PATTERN -param TDSVR=$TDSVR -param TDUSR=$TDUSR -param TDPWD=$TDPWD -param OUTPUT_FILE_PATH=${ETL_TMP_DIR_DS} -param ERRTXDATE=$ERRTXDATE -param ENV=$ENV -wait -jobstatus $DSPROJECT $DSJOBNAME $ulog");
            print("Fail: $DSPROJECT $DSJOBNAME (return code: $rc)\n");
            return (1);   
        }

    
    $rc = updateValidationLog();
    
    if ($rc != 0)
    {
        print ("Update Validation Log fail!\n");
        removeValidationLog();
        return(1);    
    }
    
    $rc = checkVariance();
    
    if ($rc != 0)
    {
        removeValidationLog();
        return(1);    
    }
    
    $rc = setTXDate($MSTABLENAME, $TABLEDB);
    
    if ($rc != 0)
    {
        print ("\n\n\nUpdate transaction date fail!\n");
        removeValidationLog();
        return(1);    
    }
    
    return 0;
}


sub runFastloadWithParam
{
    my ($TDDBN, $MSTABLENAME, $EXTRAPARAM) = @_;
    my $rc;
    my @DATA_FILE_LIST;
    my $REPFILENAME = $ETLJOBNAME;
    my $DSJOBNAME = $ETLJOBNAME;
    my @ROW_CNT;
    $TABLEDB = $TDDBN;
    
    
    $ERRTXDATE = "";
    
    open (DS, "${ETL_PROCESS_DIR_CTL}${CONTROL_FILE}") || die "Can not open " . $CONTROL_FILE;
    @DATA_FILE_LIST = <DS>;
    close(DS);

    foreach $DATAFILE (@DATA_FILE_LIST) {
    
    
        my ($esec,$emin,$ehour,$emday,$emon,$eyear,$ewday,$eyday,$eisdst) = localtime(time());
        $eyear += 1900;
        $emon = sprintf("%02d", $emon+1);
        $emday = sprintf("%02d", $emday);
        $ehour = sprintf("%02d",$ehour);
        $emin = sprintf("%02d",$emin);
        $ERRTXDATE = "${emon}${emday}${ehour}${emin}";
    
    
    
        system("rm ${ETL_TMP_DIR}/${REPFILENAME}.log $ulog");
        my @DFNAME = split(/ /,$DATAFILE);
    

#         $rc = 0;
#         my $run_count = 0;

#         while ( $run_count < 2 && $rc != 256 && $rc != 512 )
#         {   
#             $run_count++;
#             if ($run_count == 2) 
#             { sleep(900); }

            $rc = system("${DS_JOB_PATH}dsjob -run -mode RESET $DSPROJECT $DSJOBNAME $ulog");      
            sleep(10);  
            # Teradata - old syntax for running dsjob
            # $rc = system("${DS_JOB_PATH}dsjob -run -mode NORMAL -param TDDB=$TABLEDB -param TDTABLE=$DSJOBNAME -param REPFILENAME=$REPFILENAME -param SRCFILENAME=${ETL_PROCESS_DIR}$DFNAME[0] -param TDSVR=$TDSVR -param TDUSR=$TDUSR -param TDPWD=$TDPWD -param OUTPUT_FILE_PATH=${ETL_TMP_DIR_DS} -param ERRTXDATE=$ERRTXDATE -param ENV=$ENV $EXTRAPARAM -wait -jobstatus $DSPROJECT $DSJOBNAME $ulog");
            # oracle version - remove the parameter of TDDB, TDSRV, TDUSR, TDPWD, ERRTXDATE and add ETL06UATDBUser parameter 
            # for debug use 
            print("${DS_JOB_PATH}dsjob -run -mode NORMAL -param TDTABLE=$TABLEDB.$DSJOBNAME -param REPFILENAME=$REPFILENAME -param SRCFILENAME=${ETL_PROCESS_DIR}$DFNAME[0] -param TDSVR=$TDSVR -param TDUSR= -param TDPWD= -param OUTPUT_FILE_PATH=${ETL_TMP_DIR_DS} -param ENV=$ENV $EXTRAPARAM -wait -jobstatus $DSPROJECT $DSJOBNAME $ulog");
            $rc = system("${DS_JOB_PATH}dsjob -run -mode NORMAL -param TDTABLE=$TABLEDB.$DSJOBNAME -param REPFILENAME=$REPFILENAME -param SRCFILENAME=${ETL_PROCESS_DIR}$DFNAME[0] -param TDSVR=$TDSVR -param TDUSR= -param TDPWD= -param OUTPUT_FILE_PATH=${ETL_TMP_DIR_DS} -param ENV=$ENV $EXTRAPARAM -wait -jobstatus $DSPROJECT $DSJOBNAME $ulog");
#         }


        if ($rc == 256) 
        {
            print("\n\n\n#####################################\n");
            print("#Teradata Fastload/Multiload Job log\n");
            print("#DataFile: $DFNAME[0]\n");
            print("#####################################\n\n");
            ##`echo "\$ getdslog" | ftp bill02`;
            my $rfile = $REPFILENAME;
            system("mv ${ETL_TMP_DIR}/${rfile}*.txt ${ETL_TMP_DIR}/${rfile}.log $ulog");
            system("more ${ETL_TMP_DIR}/${rfile}.log $ulog");
            print("\n\n");
            parseLogFile("${ETL_TMP_DIR}/${rfile}.log");
        }
        elsif ($rc == 512) 
        {
            
            print("\n\n\n#####################################\n");
            print("#DataStage complete with warnings\n\n");
            print("#Teradata Fastload/Multiload Job log\n");
            print("#DataFile: $DFNAME[0]\n");
            print("#####################################\n\n");
            ##`echo "\$ getdslog" | ftp bill02`;
            my $rfile = $REPFILENAME;
            system("mv ${ETL_TMP_DIR}/${rfile}*.txt ${ETL_TMP_DIR}/${rfile}.log $ulog");
            system("more ${ETL_TMP_DIR}/${rfile}.log $ulog");
            print("\n\n");
            parseLogFile("${ETL_TMP_DIR}/${rfile}.log");
        }
        else
        {
            ##print("Fail: ${DS_JOB_PATH}dsjob -run -mode NORMAL -param TDDB=$TABLEDB -param REPFILENAME=$REPFILENAME -param SRCFILENAME=${ETL_PROCESS_DIR}$DFNAME[0] -param TDSVR=$TDSVR -param TDUSR=$TDUSR -param TDPWD=$TDPWD -param OUTPUT_FILE_PATH=${ETL_TMP_DIR} -param ERRTXDATE=$ERRTXDATE -param ENV=$ENV -wait -jobstatus $DSPROJECT $DSJOBNAME $ulog");
            print("Fail: $DSPROJECT $DSJOBNAME (return code: $rc)\n");
            return (1);   
        }
    }
    
    
    
    if ($TOTAL_DUP != 0)
    {   
        system("${ETL_ETC_DIR}/check_dup.ksh $ETLJOBNAME $CONTROL_FILE $ETL_PROCESS_DIR $ETL_DUP_DIR");
        print("${ETL_ETC_DIR}/check_dup.ksh $ETLJOBNAME $CONTROL_FILE $ETL_PROCESS_DIR $ETL_DUP_DIR\n");
        print("There are duplicated rows in the data file\n");
        return(1);   
    }    
    
    
    $rc = updateValidationLog();
    
    if ($rc != 0)
    {
        print ("Update Validation Log fail!\n");
        removeValidationLog();
        return(1);    
    }
    
    $rc = checkVariance();
    
    if ($rc != 0)
    {
        removeValidationLog();
        return(1);    
    }
    
    $rc = setTXDate($MSTABLENAME, $TABLEDB);
    
    if ($rc != 0)
    {
        print ("\n\n\nUpdate transaction date fail!\n");
        removeValidationLog();
        return(1);    
    }
    
    return 0;
}


########################

sub checkVariance
{
    my $DBH = connectETL();
    
    unless ( defined($DBH) ) 
    {
        print "ERROR - Unable to connect to TD!\n";
        my $ERRSTR = $DBI::errstr;
        print "$ERRSTR\n";
        return -1;
    }
    my $STH;
    
    my $SQLTEXT = "SELECT DV_Check_Table_Name ,DV_Check_Field_Name ,DV_Aggr_Func ,Check_Type , Percentage_Fixed_Val, Min_Margin, Max_Margin, Past_Variance_Use_Cnt, Margin_Percentage FROM ${ETLDB}.ETL_FILE_VALIDATION_RULE WHERE Job_Name = '$ETLJOBNAME' AND Rule_Activate_Flg = 'Y' AND Check_Type IN ('DV','RR','OC')";
        

    $STH = $DBH->prepare($SQLTEXT);
    unless ($STH) 
    {
        return -1;
    }

    $STH->execute();

    my @TABROW;

    while ( @TABROW = $STH->fetchrow() ) 
    {

        
        my $rcd = checkVariance2($TABROW[0],$TABROW[1],$TABROW[2],$TABROW[3],$TABROW[4],$TABROW[5],$TABROW[6],$TABROW[7],$TABROW[8]);
        if ($rcd != 0){
            print ("     .....Fail!!\n");
            return(-1);
        }
        print("     .....OK!!\n");
 
    }
    $STH->finish();
    
    disconnectETL($DBH);

    return (0);
}





sub checkVariance2
{
    my ($cTABLENAME ,$cFIELDNAME, $cARRGFUNC, $cCHECKTYPE, $cPFV, $cMINMG, $cMAXMG, $cPVUC, $cMP) = @_;
    my $rcc = -1;
    my $SQLTEXT = "";
    
    if (lc $cPFV eq "f")
    {
        print ("Checking: $cCHECKTYPE               Percentage Or Fixed:$cPFV");

        $SQLTEXT = "SELECT Job_Name " . 
                   " FROM ${ETLDB}.ETL_FILE_VALIDATION_LOG " .
                   " WHERE Job_Name = '$ETLJOBNAME' " .
                   " AND Check_Type = '$cCHECKTYPE' " .
               #    " AND Check_Ts = CAST('$LOADTIME' AS TIMESTAMP(0)) " .
               #    Date format for LOADTIME is YYYY-MM-DD HH24:MI:SS
                   " AND Check_Ts = TO_TIMESTAMP('$LOADTIME', 'YYYY-MM-DD HH24:MI:SS') " .
                   " AND Result_Val <= $cMAXMG AND Result_Val >= $cMINMG";
    }
    elsif (lc $cPFV eq "p")
    {
        print ("Checking: $cCHECKTYPE               Percentage Or Fixed:$cPFV");
        # $SQLTEXT = "SELECT a.Job_Name " .
        #           " FROM ${ETLDB}.ETL_FILE_VALIDATION_LOG a INNER JOIN " .
        #           " ( SELECT AVG(Result_Val) AS Avg_Result_Val " . 
        #           " FROM (SELECT *  " . 
        #           " FROM ${ETLDB}.ETL_FILE_VALIDATION_LOG " .
        #           " WHERE Job_Name = '$ETLJOBNAME' " .
        #           " AND Check_Type = '$cCHECKTYPE' QUALIFY RANK() OVER ( PARTITION BY Job_Name ,Check_Type ORDER BY Check_Ts DESC ) <= ($cPVUC + 1)) c " .
        #          " WHERE c.Check_Ts <> TO_TIMESTAMP('$LOADTIME', 'YYYY-MM-DD HH24:MI:SS') ) b ON 1 = 1 " . 
        #           " WHERE a.Job_Name = '$ETLJOBNAME' " .
        #           " AND a.Check_Type = '$cCHECKTYPE' " .
        #           " AND a.Check_Ts = TO_TIMESTAMP('$LOADTIME', 'YYYY-MM-DD HH24:MI:SS') " .
        #           " AND ABS(a.Result_Val) <= ABS(b.Avg_Result_Val) * (1.00 + (CAST($cMP AS decimal(4,2))/100.00)) " .
        #           " AND ABS(a.Result_Val) >= ABS(b.Avg_Result_Val) * (1.00 - (CAST($cMP AS decimal(4,2))/100.00))";
        $SQLTEXT = "SELECT a.Job_Name FROM ${ETLDB}.ETL_FILE_VALIDATION_LOG a INNER JOIN(SELECT AVG(Result_Val) AS Avg_Result_Val FROM(SELECT * from(select *, RANK() OVER(PARTITION BY Job_Name,Check_Type ORDER BY Check_Ts DESC) SWV_Qualify FROM ${ETLDB}.ETL_FILE_VALIDATION_LOG WHERE Job_Name = '$ETLJOBNAME' AND Check_Type = '$cCHECKTYPE') where SWV_Qualify =(v_cPVUC+1)) c WHERE c.Check_Ts <> CAST('$LOADTIME' AS TIMESTAMP(0))) b ON 1 = 1 WHERE a.Job_Name = '$ETLJOBNAME' AND a.Check_Type = '$cCHECKTYPE' AND a.Check_Ts = CAST('$LOADTIME' AS TIMESTAMP(0)) AND ABS(a.Result_Val) <= ABS(b.Avg_Result_Val)*(1.00+(CAST(v_cMP AS NUMBER(4,2))/100.00)) AND ABS(a.Result_Val) >= ABS(b.Avg_Result_Val)*(1.00 -(CAST(v_cMP AS NUMBER(4,2))/100.00))";
        
        
    }
    else
    {
        print ("Unknown type of checking\n");    
    }
    
    my $CETL = connectETL();
    # print ("\n");
    # print ("hw sqltext: $SQLTEXT");
    print ("\n");
  
 
    unless ( defined($CETL) ) 
    {
        print "ERROR - Unable to connect to TD!\n";
        my $ERRSTR = $DBI::errstr;
        print "$ERRSTR\n";
        return -1;
    }
    my $CTH;

    $CTH = $CETL->prepare($SQLTEXT);
    unless ($CTH) 
    {
        my $ERRSTR = $DBI::errstr;
        print "$ERRSTR\n";
        return -1;
    }

    $CTH->execute();

    my @TROW;

    while ( @TROW = $CTH->fetchrow() ) 
    {
        $rcc = 0;
    }
    $CTH->finish();
    
    
    disconnectETL($CETL);
    return($rcc);    
}





sub updateValidationLog
{
    my $DBH = connectETL();

    unless ( defined($DBH) ) 
    {
        print "ERROR - Unable to connect to TD!\n";
        my $ERRSTR = $DBI::errstr;
        print "$ERRSTR\n";
        return -1;
    }
    my $STH;
    
    $V_LOG_SCRIPT = lc "${AUTO_GEN_TEMP_PATH}${ETLJOBNAME}.validation";
    
    open VL, ">" . $V_LOG_SCRIPT || die "Can not open " . $V_LOG_SCRIPT;
    
    # for teradata only 
    # print VL "$LOGON_TD \n\n";
    # print VL "$SET_MAXERR\n";
    # print VL "$SET_ERRLVL_1\n";
    # print VL "$SET_ERRLVL_3\n\n";
    
    # hw set echo on
    # print VL "spool '/tmp/hw_20150904_${ETLJOBNAME}_sqlplus.log';\n"; 
    print VL "set echo on;\n\n";
    my $SQLTEXT = "SELECT DV_Check_Table_Name ,DV_Check_Field_Name ,DV_Aggr_Func FROM ${ETLDB}.ETL_FILE_VALIDATION_RULE WHERE Job_Name = '$ETLJOBNAME' AND Check_Type = 'DV'";
        

    $STH = $DBH->prepare($SQLTEXT);
    unless ($STH) 
    {
        return -1;
    }

    $STH->execute();

    my @TABROW;

    while ( @TABROW = $STH->fetchrow() ) 
    {
        print VL "INSERT INTO ${ENV}_ETL.ETL_FILE_VALIDATION_LOG\n";
        print VL "(\n";
        print VL "    Job_Name\n";
        print VL "    ,Check_Type\n";
        print VL "    ,Check_Ts\n";
        print VL "    ,Table_Name_List\n";
        print VL "    ,Percentage_Fixed_Val\n";
        print VL "    ,Result_Val\n";
        print VL ")\n";
        print VL "SELECT\n";
        print VL "    '$ETLJOBNAME'\n";
        print VL "    ,'DV'\n";
        print VL "    , TO_TIMESTAMP('$LOADTIME','YYYY-MM-DD HH24:MI:SS')\n";
        print VL "    ,''\n";
        print VL "    ,'F'\n";
        print VL "    ,$TABROW[2] ( $TABROW[1] ) \n";
        print VL "FROM\n";
        print VL "    ${TABLEDB}.$TABROW[0];\n\n";
    }
    $STH->finish();
    
       
    print VL "INSERT INTO ${ENV}_ETL.ETL_FILE_VALIDATION_LOG\n";
    print VL "(\n";
    print VL "    Job_Name\n";
    print VL "    ,Check_Type\n";
    print VL "    ,Check_Ts\n";
    print VL "    ,Table_Name_List\n";
    print VL "    ,Percentage_Fixed_Val\n";
    print VL "    ,Result_Val\n";
    print VL ")\n";
    print VL "VALUES\n";
    print VL "(\n";
    print VL "    '$ETLJOBNAME'\n";
    print VL "    ,'RR'\n";
    print VL "    ,TO_TIMESTAMP('$LOADTIME', 'YYYY-MM-DD HH24:MI:SS')\n";
    print VL "    ,''\n";
    print VL "    ,'F'\n";
    print VL "    ,$TOTAL_ER1 + $TOTAL_ER2 + $TOTAL_ET + $TOTAL_UV + $TOTAL_DUP  \n";
    print VL ");\n\n";

    print VL "INSERT INTO ${ENV}_ETL.ETL_FILE_VALIDATION_LOG\n";
    print VL "(\n";
    print VL "    Job_Name\n";
    print VL "    ,Check_Type\n";
    print VL "    ,Check_Ts\n";
    print VL "    ,Table_Name_List\n";
    print VL "    ,Percentage_Fixed_Val\n";
    print VL "    ,Result_Val\n";
    print VL ")\n";
    print VL "VALUES\n";
    print VL "(\n";
    print VL "    '$ETLJOBNAME'\n";
    print VL "    ,'OC'\n";
    print VL "    ,TO_TIMESTAMP('$LOADTIME', 'YYYY-MM-DD HH24:MI:SS')\n";
    print VL "    ,''\n";
    print VL "    ,'F'\n";
    print VL "    ,$TOTAL_INS + $TOTAL_UPD\n";
    print VL ");\n";
   
    print VL "commit;\n";
    # print VL "spool off;\n";
    print VL "exit;\n";
 
    close (VL);    
    # hw check the script file 
    # $rc = system("cp ${V_LOG_SCRIPT} /tmp/hw_20150904_${ETLJOBNAME}.backup ");
    print ("\n");
    print ("*** calling sqlplus to create record in ETL_FILE_VALIDATION_LOG ***");
    print ("\n");

    $rc = system("sqlplus /\@${TDDSN} \@${V_LOG_SCRIPT} ");
    
    disconnectETL($DBH);
    return($rc);
    
    
}




#Below function is used to remove the new entries in the 
#ETL_FILE_VALIDATION_LOG table when the check variance fail
sub removeValidationLog
{
    my $rc = open(SQLPLUS, "| sqlplus /\@${TDDSN}");
    unless ($rc){
        print "Cound not invoke sqlplus command\n";
        return -1;
    }

    print SQLPLUS<<ENDOFINPUT;
DELETE FROM 
    ${ENV}_ETL.ETL_FILE_VALIDATION_LOG 
WHERE
    Check_Ts = TO_TIMESTAMP('$LOADTIME', 'YYYY-MM-DD HH24:MI:SS')
AND Job_Name = '$ETLJOBNAME';
COMMIT;
EXIT;
ENDOFINPUT

    close(SQLPLUS);
    my $RET_CODE = $? >> 8;
    if ($RET_CODE != 0){
        return 1;
    }else{
        return 0;
    }
}



sub parseLogFile
{
    my ($LOGFILE) = @_;
    my ($NUM_INS, $NUM_DUP, $NUM_UPD, $NUM_DEL, $NUM_OUT);
    my ($NUM_ER1, $NUM_ER2, $NUM_ET, $NUM_UV);
    my $TYPE = "";

    $NUM_INS = 0; $NUM_DUP = 0; $NUM_UPD = 0; $NUM_DEL = 0, $NUM_OUT = 0;
    $NUM_ER1 = 0; $NUM_ER2 = 0; $NUM_ET  = 0; $NUM_UV  = 0;
 
    unless ( open(MRESULT, "$LOGFILE") ) {
        return ($TYPE,$NUM_INS,$NUM_DUP,$NUM_UPD,$NUM_DEL,$NUM_OUT,$NUM_ET,$NUM_UV,$NUM_ER1,$NUM_ER2);
    }

    my $LINE;
    my $TYPEFLAG = 0;

    while ( $LINE = <MRESULT> ) {
        if ( $TYPEFLAG == 0 ) {
            my $BUF = $LINE;
            $BUF =~ tr/[a-z]/[A-Z]/;

            if ( index($BUF, "UTILITY") > 0)
            {
                $TYPE = (split(' ', $BUF))[1];
                $TYPEFLAG = 1;
                next;
            }
        }

        if ( $TYPE eq "FASTLOAD" ) {
            if ( index($LINE, "Total Inserts Applied") > 0 ) {
                $NUM_INS = (split(' ', $LINE))[4];
            }
            elsif ( index($LINE, "Total Duplicate Rows") > 0 ) {
                $NUM_DUP = (split(' ', $LINE))[4];
            }
            elsif ( index($LINE, "Total Error Table 1") > 0 ) {
                $NUM_ER1 = (split(' ', $LINE))[5];
            }
            elsif ( index($LINE, "Total Error Table 2") > 0 ) {
                $NUM_ER2 = (split(' ', $LINE))[5];
            }
        }
        elsif ( $TYPE eq "MULTILOAD" ) {
            if ( substr($LINE, 0, 16) eq "        Inserts:" ) {
                $NUM_INS = (split(' ', $LINE))[1];
            } 
            elsif ( substr($LINE, 0, 16) eq "        Updates:" ) {
                $NUM_UPD = (split(' ', $LINE))[1];
            }
            elsif ( substr($LINE, 0, 16) eq "        Deletes:" ) {
                $NUM_DEL = (split(' ', $LINE))[1];
            } 
            elsif ( substr($LINE, 0, 37) eq "     Number of Rows  Error Table Name" ) {
                my $BUF1 = <MRESULT>;
                my $ETLINE = <MRESULT>;
                $NUM_ET = (split(' ', $ETLINE))[0];
                my $UVLINE = <MRESULT>;
                $NUM_UV = (split(' ', $UVLINE))[0];
            }
        }
    }
    close(MRESULT);


    $TOTAL_INS = $TOTAL_INS + $NUM_INS;
    $TOTAL_DUP = $TOTAL_DUP + $NUM_DUP;
    $TOTAL_UPD = $TOTAL_UPD + $NUM_UPD;
    $TOTAL_DEL = $TOTAL_DEL + $NUM_DEL;
    $TOTAL_OUT = $TOTAL_OUT + $NUM_OUT;
    $TOTAL_ET = $TOTAL_ET + $NUM_ET;
    $TOTAL_UV = $TOTAL_UV + $NUM_UV;
    $TOTAL_ER1 = $TOTAL_ER1 + $NUM_ER1;
    $TOTAL_ER2 = $TOTAL_ER2 + $NUM_ER2;
    
}





sub preProcess
{
    $CONTROL_FILE = pop(@_);

    #Get the name of the table
    $ETLSYS = substr(${CONTROL_FILE},0,3);
    $ETLJOBNAME = substr(${CONTROL_FILE},4,length(${CONTROL_FILE})-17);
    $TXDATE = substr(${CONTROL_FILE},length(${CONTROL_FILE})-12,8);
    $TXMONTH = substr($TXDATE,0,6);
    $TXDATE = substr($TXDATE,0,4)."-".substr($TXDATE,4,2)."-".substr($TXDATE,6,2);
    
    # Remark for oracle platform
    # getLogonString();
    
    
}


sub postProcess
{
    $MASTABLE = pop(@_);
}




#This function is used to update the image date in the ETL_TABLE_DATE table
sub updateJobTXDate
{
    my $UTABLENAME = pop(@_);
    
    if (length($UTABLENAME) < 2) { 
        $UTABLENAME = substr($ETLJOBNAME,2);
    }    
    
    my $rc = open(SQLPLUS, "| sqlplus /\@${TDDSN}");
    unless ($rc){
        print "Cound not invoke sqlplus command\n";
        return -1;
    }

    print SQLPLUS<<ENDOFINPUT;

    Update ${ETLDB}.ETL_TABLE_DATE  etd
    SET ETD.As_Of_Ts = (SELECT to_timestamp(EJT.Tx_Ts + cast(ETD.AS_OF_DATE_OFFSET as number)) FROM ${ETLDB}.ETL_JOB_TXDATE  EJT WHERE EJT.Table_Name = '${UTABLENAME}')
    ,   ETD.Refresh_Ts = to_TIMESTAMP('${LOADTIME}', 'YYYY-MM-DD HH24:MI:SS')
    WHERE  ETD.Table_Name = '$UTABLENAME'
    AND exists (SELECT 'X'
                FROM ${ETLDB}.ETL_JOB_TXDATE  EJT1
                WHERE ETD.Table_Name = EJT1.Table_Name
                AND  (ETD.AS_OF_TS IS NULL OR ETD.As_Of_Ts < TO_TIMESTAMP(EJT1.Tx_Ts + cast(ETD.AS_OF_DATE_OFFSET as number)))
               );
    commit;
    exit; 
ENDOFINPUT

    close(SQLPLUS);
    my $RET_CODE = $? >> 8;
    if ($RET_CODE != 0){
        return 1;
    }else{
        return 0;
    }
}







#The function below should be called before running a transformation job
sub getTXDate
{
    my $TX_TABLE_NAME = pop(@_);
    
    # $SQLTEXT = "SELECT CAST( CAST((CAST(a.Tx_Ts AS DATE FORMAT 'YYYYMMDD') + b.As_Of_Date_Offset) AS DATE FORMAT 'YYYY-MM-DD') AS CHAR(10)) FROM ${ETLDB}.ETL_JOB_TXDATE a INNER JOIN ${ETLDB}.ETL_TABLE_DATE b ON a.Table_Name = b.Table_Name WHERE a.Table_Name = '$TX_TABLE_NAME'";
    $SQLTEXT = "SELECT TO_CHAR(TO_DATE(TO_CHAR(a.Tx_Ts,'YYYYMMDD'),'YYYYMMDD') + b.As_Of_Date_Offset, 'YYYY-MM-DD') FROM ${ETLDB}.ETL_JOB_TXDATE a INNER JOIN ${ETLDB}.ETL_TABLE_DATE b ON a.Table_Name = b.Table_Name WHERE a.Table_Name = '${TX_TABLE_NAME}'";
    
    my $CETL = connectETL();
    
    unless ( defined($CETL) ) 
    {
        print "ERROR - Unable to connect to TD!\n";
        my $ERRSTR = $DBI::errstr;
        print "$ERRSTR\n";
        return -1;
    }
    my $CTH;

    $CTH = $CETL->prepare($SQLTEXT);
    unless ($CTH) 
    {
        return -1;
    }

    $CTH->execute();

    my @TROW;

    while ( @TROW = $CTH->fetchrow() ) 
    {
       $TXDATE = $TROW[0];
       $TXMONTH = substr($TXDATE,0,4).substr($TXDATE,5,2);
    }
    
    $CTH->finish();
    
    disconnectETL($CETL);
}



#The function below is only called in the runDataStageJob to set the intermediate transaction date used for transformation job
sub setTXDate
{
    my ($TX_TABLE_NAME, $LDDB) = @_;
    my $AS_COLUMN_NAME = "n/a";
    my $AS_TABLE_NAME = "n/a";
    my $SQLTEXT = "";
    
    
    $SQLTEXT = "SELECT As_Of_Date_Determine_Column, As_Of_Date_Determine_Table FROM ${ETLDB}.ETL_TABLE_DATE WHERE Table_Name = '$TX_TABLE_NAME' AND As_Of_Date_Determine_Meth = 'T'";
    
    my $CETL = connectETL();
    
    unless ( defined($CETL) ) 
    {
        print "ERROR - Unable to connect to TD!\n";
        my $ERRSTR = $DBI::errstr;
        print "$ERRSTR\n";
        return -1;
    }
    my $CTH;

    $CTH = $CETL->prepare($SQLTEXT);
    unless ($CTH) 
    {
        return -1;
    }

    $CTH->execute();



    my @TROW;

    while ( @TROW = $CTH->fetchrow() ) 
    {
        $AS_COLUMN_NAME = $TROW[0];
        $AS_TABLE_NAME = $TROW[1];
    }
   
    
    
    if ($AS_COLUMN_NAME ne "n/a")
    {
        # $SQLTEXT = "SELECT CAST(CAST(MAX($AS_COLUMN_NAME) AS TIMESTAMP(0)) AS CHAR(19)) FROM ${LDDB}.${AS_TABLE_NAME}"; 
        $SQLTEXT = "SELECT TO_CHAR(MAX($AS_COLUMN_NAME),'YYYY-MM-DD HH24:MI:SS') FROM ${LDDB}.${AS_TABLE_NAME}"; 
    }
    else
    {
        # $SQLTEXT = "SELECT CAST(Last_File_As_Of_Ts AS CHAR(19)) FROM ${ETLDB}.ETL_SRC_FILE WHERE Job_Name = '$ETLJOBNAME'";  
        $SQLTEXT = "SELECT TO_CHAR(Last_File_As_Of_Ts, 'YYYY-MM-DD HH24:MI:SS') FROM ${ETLDB}.ETL_SRC_FILE WHERE Job_Name = '$ETLJOBNAME'";  
    }


    my $AS_DATE = "${TXDATE} ${ENTRYTIME}";

    print "AS_DATE value is : $AS_DATE \n";    
    
    $CTH = $CETL->prepare($SQLTEXT);
    unless ($CTH) 
    {
        return -1;
    }

    print "To be executed: $SQLTEXT\n";

    $CTH->execute();

    my @TROW;

    while ( @TROW = $CTH->fetchrow() ) 
    {
        if ($TROW[0] ne "")
        { 
            $AS_DATE = $TROW[0];
        }
    }
   
    # $SQLTEXT = "UPDATE ${ETLDB}.ETL_JOB_TXDATE SET Tx_Ts = CAST('$AS_DATE' AS TIMESTAMP(0)) WHERE (Table_Name = '$TX_TABLE_NAME') ELSE INSERT INTO ${ETLDB}.ETL_JOB_TXDATE ('$TX_TABLE_NAME',CAST('$AS_DATE' AS TIMESTAMP(0)))";

    $SQLTEXT = " MERGE INTO ${ETLDB}.ETL_JOB_TXDATE EJT USING DUAL ON (EJT.TABLE_NAME = '${TX_TABLE_NAME}' )" .
               " WHEN NOT MATCHED THEN INSERT (TABLE_NAME,TX_TS) VALUES ('${TX_TABLE_NAME}',TO_TIMESTAMP('${AS_DATE}','YYYY-MM-DD HH24:MI:SS'))" .
               " WHEN MATCHED THEN UPDATE SET EJT.TX_TS = TO_TIMESTAMP('${AS_DATE}','YYYY-MM-DD HH24:MI:SS') ";
    
    $CTH = $CETL->prepare($SQLTEXT);
    unless ($CTH) 
    {
        return -1;
    }

    print "To be executed: $SQLTEXT\n";

    my $rx = $CTH->execute();

    print "Execute Result: $rx\n";

    #if ($rx <= 0)
    #{
    #    return -1;
    #}
    
    $CTH->finish();
    
    disconnectETL($CETL);
    
    return 0;
    
}


sub collectStatistic
{
    my ($CS_TABLE_NAME,$CS_DB) = @_;
    # Oracle - Temporary by-pass this function
    return 0;
    
    my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());
    
    my $AUTO_GEN_SCRIPT = "";
    
    if ($CS_DB eq ""){
        $CS_DB = $ADWDB;
    }
    
    $AUTO_GEN_SCRIPT = lc "${AUTO_GEN_TEMP_PATH}${CS_DB}_${CS_TABLE_NAME}_${SCRIPTGENTIME}.collectStatistic";
    
    open DF, ">" . $AUTO_GEN_SCRIPT || die "Can not open " . $AUTO_GEN_SCRIPT;
    
    
    #The following script is used to generated the header of the auto-gen script
    print DF "--This collect statistic scirpt is generated by AUTO_GEN function\n";
    
    
    print DF "$LOGON_TD \n\n";
    print DF "$SET_MAXERR\n";
    print DF "$SET_ERRLVL_1\n";
    print DF "$SET_ERRLVL_3\n\n";
    
    
    
    my $DBH = connectETL();

    unless ( defined($DBH) ) 
    {
        print "ERROR - Unable to connect to TD!\n";
        my $ERRSTR = $DBI::errstr;
        print "$ERRSTR\n";
        return -1;
    }

    my $STH;
   
    
    #Select the related collect statistics statement
        
    my $SQLTEXT = "SELECT ";
    $SQLTEXT = $SQLTEXT . "Collect_Stat_Columns ";
    $SQLTEXT = $SQLTEXT . ",CASE WHEN (b.Sum_Currentperm > ${CS_TABLE_SIZE}) AND Use_Sample_Flg = '' THEN 'Y' WHEN Use_Sample_Flg = '' THEN 'N' ELSE Use_Sample_Flg END ";
    $SQLTEXT = $SQLTEXT . ",Collect_Type ";
    $SQLTEXT = $SQLTEXT . "FROM $ETLDB.ETL_COLLECT_STAT_LIST a , ( ";
    $SQLTEXT = $SQLTEXT . "Select   databasename, tablename, Sum(currentperm) as Sum_Currentperm ";
    $SQLTEXT = $SQLTEXT . "From       dbc.allspace ";
    $SQLTEXT = $SQLTEXT . "Group     By 1,2 ";
    $SQLTEXT = $SQLTEXT . ") b ";
    $SQLTEXT = $SQLTEXT . "WHERE ";
    $SQLTEXT = $SQLTEXT . "a.collect_stat_table = b.tablename ";
    $SQLTEXT = $SQLTEXT . "and a.collect_stat_db = b.databasename ";
    $SQLTEXT = $SQLTEXT . "and Collect_Stat_Db = '${CS_DB}' ";
    $SQLTEXT = $SQLTEXT . "AND Collect_Stat_Table = '${CS_TABLE_NAME}' ";
    $SQLTEXT = $SQLTEXT . "AND Enable = 'Y' ";
    $SQLTEXT = $SQLTEXT . "AND (INDEX(Frequency,'${wday}') > 0 OR Frequency = 'D') ";
    
    print ("\n$SQLTEXT\n");
    
    $STH = $DBH->prepare($SQLTEXT);
    unless ($STH) 
    {
        return -1;
    }

    $STH->execute();

    my @TABROW;
    my $result_count = 0;

    while ( @TABROW = $STH->fetchrow() ) 
    {
        $result_count = $result_count + 1;
        my $CS_Column = $TABROW[0];
        my $CS_Sample = $TABROW[1];
        my $CS_Type = $TABROW[2];
        
        
        if ((uc($TABROW[1]) eq "Y") && (uc($TABROW[2]) eq "I")){
            print DF "COLLECT STATISTICS USING SAMPLE ON ";
            print DF $CS_DB . "." . $CS_TABLE_NAME . " INDEX (" . $CS_Column . ");\n";      
        }
        
        if ((uc($TABROW[1]) eq "Y") && (uc($TABROW[2]) eq "C")){
            print DF "COLLECT STATISTICS USING SAMPLE ON ";
            print DF $CS_DB . "." . $CS_TABLE_NAME . " COLUMN (" . $CS_Column . ");\n";      
        }
        
        if ((uc($TABROW[1]) eq "N") && (uc($TABROW[2]) eq "I")){
            print DF "COLLECT STATISTICS ON ";
            print DF $CS_DB . "." . $CS_TABLE_NAME . " INDEX (" . $CS_Column . ");\n";      
        }
        
        if ((uc($TABROW[1]) eq "N") && (uc($TABROW[2]) eq "C")){
            print DF "COLLECT STATISTICS ON ";
            print DF $CS_DB . "." . $CS_TABLE_NAME . " COLUMN (" . $CS_Column . ");\n";      
        }
        
        
        
    }
    $STH->finish();
    disconnectETL($DBH);
    
    
    
    close DF;
    
    
    my $rc;
    
    if ($result_count == 0){
        $rc = 0;
    }else {
        $rc = system("bteq < $AUTO_GEN_SCRIPT $ulog");
    }
    
    if ($rc != 0) 
    { 
        return 1;
    }
    else
    {
        return 0;
    }    
}




#This function is used to generate the upsert/append/replace/history approach scripts
sub runGenScript
{
    my ($TARGETDBNAME, $TARGETTABLENAME, $SOURCEDBNAME, $SOURCETABLENAME, $GENTYPE, $PROCESSDATE) = @_;
    
    my $AUTO_GEN_SCRIPT = "";


    #First we design the path and file name for the auto-gen script
    if ($GENTYPE == $REPLACE_SCRIPT)
    {
        $AUTO_GEN_SCRIPT = lc "${AUTO_GEN_TEMP_PATH}${TARGETDBNAME}_${TARGETTABLENAME}_${SCRIPTGENTIME}.replace";
    }
    elsif ($GENTYPE == $UPSERT_SCRIPT)
    {
        $AUTO_GEN_SCRIPT = lc "${AUTO_GEN_TEMP_PATH}${TARGETDBNAME}_${TARGETTABLENAME}_${SCRIPTGENTIME}.upsert";      
    }   
    elsif ($GENTYPE == $HIST_SCRIPT_EXPIRE)
    {
        $AUTO_GEN_SCRIPT = lc "${AUTO_GEN_TEMP_PATH}${TARGETDBNAME}_${TARGETTABLENAME}_${SCRIPTGENTIME}.exp.history";
    }   
    elsif ($GENTYPE == $HIST_SCRIPT_NON_EXPIRE)
    {
        $AUTO_GEN_SCRIPT = lc "${AUTO_GEN_TEMP_PATH}${TARGETDBNAME}_${TARGETTABLENAME}_${SCRIPTGENTIME}.non.history";
    }
    elsif ($GENTYPE == $APPEND_SCRIPT)
    {
        $AUTO_GEN_SCRIPT = lc "${AUTO_GEN_TEMP_PATH}${TARGETDBNAME}_${TARGETTABLENAME}_${SCRIPTGENTIME}.append";
    }else
    {
        print "GENTYPE: $GENTYPE not exist";
        return (-1);
    }

    open DF, ">" . $AUTO_GEN_SCRIPT || die "Can not open " . $AUTO_GEN_SCRIPT;
    
    
    #The following script is used to generated the header of the auto-gen script
    print DF "--This scirpt is generated by AUTO_GEN function\n";
    
    if ($GENTYPE == $REPLACE_SCRIPT)
    {
        print DF "--Type of Script: Replace\n\n";
    }
    elsif ($GENTYPE == $UPSERT_SCRIPT)
    {
        print DF "--Type of Script: Upsert\n\n";        
    }   
    elsif ($GENTYPE == $HIST_SCRIPT_NON_EXPIRE)
    {
        print DF "--Type of Script: History (Non expire if records missing in the source table)\n\n";        
    }   
    elsif ($GENTYPE == $HIST_SCRIPT_EXPIRE)
    {
        print DF "--Type of Script: History (Expire if records missing in the source table)\n\n";        
    }   
    elsif ($GENTYPE == $APPEND_SCRIPT)
    {
        print DF "--Type of Script: Append\n\n";        
    }else{
        print "GENTYPE: $GENTYPE not exist";
        return (-1);
    }
    # Add set echo on in the auto_gen sql file 
    print DF "set echo on;\n\n"; 

    my $DBH = connectETL();

    unless ( defined($DBH) ) 
    {
        print "ERROR - Unable to connect to TD!\n";
        my $ERRSTR = $DBI::errstr;
        print "$ERRSTR\n";
        return -1;
    }

    my $no_keyf = 0;

    unless ( open(KEYF, "$ETL_RESER_WORD_LIST") ) { 
                print "Unable to open $ETL_RESER_WORD_LIST\n";
        $no_keyf = 1;
    }

    if (! $no_keyf) {
        @RESER_WORD_LIST = <KEYF>;
        close(KEYF);
                for (my $n = 0; $n <= $#RESER_WORD_LIST ; $n++) {
            chomp($RESER_WORD_LIST[$n]);
#            $RESER_WORD_LIST[$n] = "\\<$RESER_WORD_LIST[$n]\\>";
        }
    }

    my $STH;
    my @COLUMNLIST;
    my @PKLIST;
    my $DATECOLUMN = "N/A";
    
    #Generate the column list
    # Teradata - Generate column list 
    # my $SQLTEXT = "SELECT ColumnName FROM DBC.COLUMNS WHERE DatabaseName = '$TARGETDBNAME' AND TableName = '$TARGETTABLENAME' ORDER BY ColumnId";

    # Oracle - Generate column List      
    # TARGETDBNAME >>> SCHEMA OWNER NAME
    # TARGETTABLENAME >>> TABLE NAME
    
    my $SQLTEXT = "SELECT column_name FROM all_tab_columns WHERE owner = '${TARGETDBNAME}' AND table_name = '${TARGETTABLENAME}' ORDER BY column_id ASC";
    print ("Generate column list: ${SQLTEXT}\n");

    $STH = $DBH->prepare($SQLTEXT);
    unless ($STH) 
    {
        return -1;
    }

    $STH->execute();

    my @TABROW;

    while ( @TABROW = $STH->fetchrow() ) 
    {
        my $col = trim($TABROW[0]);
        foreach $KEYW (@RESER_WORD_LIST)
        {
                        if (lc($col) eq $KEYW) {        # Only exact match of reserved word needs to be masked
                                print "Found reserved word [$col]\n";
                                $col = q(") . $col . q(");
                                last;
                        }
                }
        push  @COLUMNLIST, $col;
    }
    $STH->finish();


    #Generate the primary key list
    if (($GENTYPE == $HIST_SCRIPT_EXPIRE) or ($GENTYPE == $UPSERT_SCRIPT) or ($GENTYPE == $HIST_SCRIPT_NON_EXPIRE))
    {
        print("Get the PK list\n");
        
        # Teradata - generate primary key list for target table 
        # $SQLTEXT = "SELECT PK_Column FROM $ETLDB.ETL_AUTO_GEN_PK WHERE DB_Name = '$TARGETDBNAME' AND Table_Name = '$TARGETTABLENAME'";
        # print ("SELECT PK_Column FROM $ETLDB.ETL_AUTO_GEN_PK WHERE DB_Name = '$TARGETDBNAME' AND Table_Name = '$TARGETTABLENAME'\n");
        #                        
        # Oracle - generate primary key list for target table  
        # ETLDB >> SCHEAM_NAME for ETL_AUTO_GEN_PK
        # TARGETDBNAME >> SCHEMA_NAME FOR Target table 
        
        $SQLTEXT = "SELECT PK_Column FROM $ETLDB.ETL_AUTO_GEN_PK WHERE DB_Name = '$TARGETDBNAME' AND Table_Name = '$TARGETTABLENAME'";
        print ("SELECT PK_Column FROM $ETLDB.ETL_AUTO_GEN_PK WHERE DB_Name = '$TARGETDBNAME' AND Table_Name = '$TARGETTABLENAME'\n");        
        
        $STH = $DBH->prepare($SQLTEXT);
        unless ($STH) 
        {
            return -1;
        }

        $STH->execute();

        @TABROW = -1;

        while ( @TABROW = $STH->fetchrow() ) 
        {
            push @PKLIST, trim($TABROW[0]);
        }
        $STH->finish();
    }
    
    
    if ($GENTYPE == $APPEND_SCRIPT)
    {
        $SQLTEXT = "SELECT CAST(Last_Append_Ts AS CHAR(20)) FROM $ETLDB.ETL_TABLE_DATE WHERE Table_Name = '$TARGETTABLENAME' AND Last_Append_Ts IS NOT NULL";

        # Teradata - get last_append_ts for GENTYPE = APPEND
        # $SQLTEXT = "SELECT CAST(Last_Append_Ts AS CHAR(20)) FROM $ETLDB.ETL_TABLE_DATE WHERE Table_Name = '$TARGETTABLENAME' AND Last_Append_Ts IS NOT NULL";
        #                 
        # Oracle   - get last_append_ts for GENTYPE = APPEND 
        $SQLTEXT = "SELECT TO_CHAR(Last_Append_Ts ,'YYYY-MM-DD HH24:MI:SS') FROM ${ETLDB}.ETL_TABLE_DATE WHERE Table_Name = '$TARGETTABLENAME' AND Last_Append_Ts IS NOT NULL";
        
        $STH = $DBH->prepare($SQLTEXT);
        unless ($STH)
        {
            return -1;
        }

        $STH->execute();

        @TABROW = -1;

        while ( @TABROW = $STH->fetchrow() )
        {
            $DATECOLUMN =  trim($TABROW[0]);
        }
        $STH->finish();

    }
    
    my $COLUMNSIZE = @COLUMNLIST;
    my $PKSIZE = @PKLIST;
    
    
    # Teradata - Connect DB command  
    # print DF "$LOGON_TD \n\n";
    # print DF "$SET_MAXERR\n";
    # print DF "$SET_ERRLVL_1\n";
    # print DF "$SET_ERRLVL_3\n\n";
    
    # Oracle - Append DB connection command in the dynamic script 
    #
    # declare 
    #   x_count number;
    #   x_pkey_exp   exception;
    #   x_start_date_exp exception;
    #   x_start_end_date_exp exception; 
    # begin 

    # print DF " whenever sqlerror exit 2; \n";
    print DF " set autocommit 1; \n";
    print DF " SET SERVEROUTPUT ON; \n";
    print DF " SET SQLPROMPT ' '; \n";
    print DF " SET SQLNUMBER OFF; \n";
    print DF " whenever sqlerror exit 2;\n ";
    print DF " alter session force parallel query; \n ";
    print DF " alter session enable parallel dml; \n ";
    print DF " ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'; \n ";
    print DF " \n";
    print DF "declare\n";
    print DF "  x_count number;\n";
    print DF "  x_pkey_exp   exception;\n";
    print DF "  x_start_date_exp   exception;\n";
    print DF "  x_start_end_date_exp   exception;\n";
    print DF "  x_count_debug  number; \n ";
    print DF "begin\n";
    
    # Oracle - Rename table will be proceed at the end of script 
    # if ($GENTYPE != $APPEND_SCRIPT)
    # {
    #    print DF "RENAME TABLE \n";
    #    print DF "    ${TARGETDBNAME}.${TARGETTABLENAME}${SWITCH_POSTFIX_OLD}\n";
    #    print DF "AS\n";
    #    print DF "    ${TARGETDBNAME}.${TARGETTABLENAME};\n\n";
    
    #    print DF "CREATE TABLE \n";
    #    print DF "    ${TARGETDBNAME}.${TARGETTABLENAME}${SWITCH_POSTFIX_NEW}\n";
    #    print DF "AS\n";
    #    print DF "    ${TARGETDBNAME}.${TARGETTABLENAME}\n";
    #    print DF "WITH NO DATA;\n\n";
    # }
    
    
    if ($GENTYPE == $REPLACE_SCRIPT)
    {   
        # Teradata - 
        # print DF "DELETE FROM ${TARGETDBNAME}.${TARGETTABLENAME}${SWITCH_POSTFIX_NEW} ALL;\n\n";
        print DF " ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL2(P_schema_tbl_name=>'${TARGETDBNAME}.${TARGETTABLENAME}${SWITCH_POSTFIX_NEW}'); \n\n";
        print DF "\n\n";

    }
    if ($GENTYPE == $UPSERT_SCRIPT)
    {
        # Teradata - 
        # print DF "DELETE FROM ${TARGETDBNAME}.${TARGETTABLENAME}${SWITCH_POSTFIX_NEW} ALL;\n\n";
        print DF " ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL2(P_schema_tbl_name=>'${TARGETDBNAME}.${TARGETTABLENAME}${SWITCH_POSTFIX_NEW}'); \n\n";
        print DF "\n\n";
    }   
    if ($GENTYPE == $HIST_SCRIPT_NON_EXPIRE)
    {
        # Teradata - 
        # print DF "DELETE FROM ${TARGETDBNAME}.${TARGETTABLENAME}${SWITCH_POSTFIX_NEW} ALL;\n\n";
        print DF " ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL2(P_schema_tbl_name=>'${TARGETDBNAME}.${TARGETTABLENAME}${SWITCH_POSTFIX_NEW}'); \n\n" ;
        print DF "\n\n";
    }   
    if ($GENTYPE == $HIST_SCRIPT_EXPIRE)
    {
        # Teradata - 
        # print DF "DELETE FROM ${TARGETDBNAME}.${TARGETTABLENAME}${SWITCH_POSTFIX_NEW} ALL;\n\n";
        print DF " ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL2(P_schema_tbl_name=>'${TARGETDBNAME}.${TARGETTABLENAME}${SWITCH_POSTFIX_NEW}'); \n\n";
        print DF "\n\n";
    }
    if ($GENTYPE == $APPEND_SCRIPT)
    {                
        if ($DATECOLUMN ne "N/A")
        {
            # Teradata - 
            # print DF "DELETE FROM ${TARGETDBNAME}.${TARGETTABLENAME} WHERE Refresh_Ts = CAST('${DATECOLUMN}' AS TIMESTAMP(0)) ;\n\n";
            # incorrect logic
            # print DF " ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL2(P_schema_tbl_name=>'${TARGETDBNAME}.${TARGETTABLENAME}${SWITCH_POSTFIX_NEW}'); \n\n";
            print DF " DELETE FROM ${TARGETDBNAME}.${TARGETTABLENAME} WHERE Refresh_Ts = TO_TIMESTAMP('${DATECOLUMN}', 'YYYY-MM-DD HH24:MI:SS') ;";
            print DF " \n";
            print DF " DBMS_OUTPUT.PUT_LINE(sql%rowcount || ' rows deleted');\n";
            print DF " DBMS_OUTPUT.PUT_LINE('Elapsed: current_time ' || to_char(sysdate, 'YYYY-MM-DD HH24:MI:SS') );\n";

        }
    }
    
    if ($GENTYPE == $REPLACE_SCRIPT)
    {
        #special logic for replace script generation
        
        print DF "INSERT /*+ APPEND ${AUTOGEN_HINT}  */ INTO ${TARGETDBNAME}.${TARGETTABLENAME}${SWITCH_POSTFIX_NEW}(\n";
        print DF "    $COLUMNLIST[0]\n";
        
        foreach $CN (@COLUMNLIST)
        {
            if ($CN ne $COLUMNLIST[0])
            { 
                print DF "    ,$CN\n";
            }
        }
        
        print DF ")\n";
        print DF "SELECT\n";
        print DF "    $COLUMNLIST[0]\n";
        
        foreach $CN (@COLUMNLIST)
        {
            if ($CN ne $COLUMNLIST[0])
            {  
                if (lc $CN eq "create_ts")
                {
                    # Teradata - using cast command 
                    # print DF "    ,CAST('$LOADTIME' AS TIMESTAMP(0))\n";
                    # Oracle - replace cast command with to_timestamp
                    print DF "     ,TO_TIMESTAMP('${LOADTIME}', 'YYYY-MM-DD HH24:MI:SS')\n";
                }elsif (lc $CN eq "refresh_ts")
                {
                    # Teradata - using cast command
                    # print DF "    ,CAST('$LOADTIME' AS TIMESTAMP(0))\n";
                    # Oracle - replace cast command with to_timestamp
                    print DF "     ,TO_TIMESTAMP('${LOADTIME}', 'YYYY-MM-DD HH24:MI:SS')\n";
                }else{
                    print DF "    ,$CN\n";
                }
            }
        }
        
        print DF "FROM\n";
        print DF "    ${SOURCEDBNAME}.${SOURCETABLENAME}\n";
        print DF ";\n";

        # HW - Output Number of rows 
        print DF "  DBMS_OUTPUT.PUT_LINE(sql%rowcount || ' rows created');\n";
        print DF "  DBMS_OUTPUT.PUT_LINE('Elapsed: current_time ' || to_char(sysdate, 'YYYY-MM-DD HH24:MI:SS') );\n";
        # HW - add commit after insert statement 
        print DF "commit;\n";
        
    }
    elsif ($GENTYPE == $UPSERT_SCRIPT)
    {
        #special logic for upsert script generation
        
        
        
        #Special script to check duplicate records
        # Oracle - Add the parent qeury to get duplicate PK count into x_count
        print DF "SELECT COUNT(1)\n";
        print DF "INTO x_count\n";
        print DF "FROM\n";
        print DF "(\n";

        print DF "SELECT \n";
        print DF "    $PKLIST[0]\n";
        
        foreach $CN (@PKLIST)
        {
            if ($CN ne $PKLIST[0])
            {
                print DF ",    $CN\n";
            }
        }         
        
        print DF "FROM\n";
        print DF "    ${SOURCEDBNAME}.${SOURCETABLENAME}\n";
        print DF "GROUP BY \n";
        print DF "    $PKLIST[0]\n";
        
        foreach $CN (@PKLIST)
        {
            if ($CN ne $PKLIST[0])
            {
                print DF ",    $CN\n";
            }
        }         


        # Teradata - change for pending parent query 
        # print DF "HAVING COUNT(*) > 1;\n";
        print DF "HAVING COUNT(*) > 1);\n";
        print DF "IF x_count > 0 THEN\n";
        print DF "  RAISE x_pkey_exp;\n";
        print DF "END IF;\n";
    
        # Teradata - remark due to incompatible syntax in oracle 
        # print DF ".IF ACTIVITYCOUNT > 0 THEN .QUIT 100000\n\n\n";
        
        
        # Insert statement 1 
        print DF "INSERT /*+ APPEND ${AUTOGEN_HINT} */ INTO ${TARGETDBNAME}.${TARGETTABLENAME}${SWITCH_POSTFIX_NEW}(\n";
        print DF "    $COLUMNLIST[0]\n";
        
        foreach $CN (@COLUMNLIST)
        {
            if ($CN ne $COLUMNLIST[0])
            { 
                print DF "    ,$CN\n";
            }
        }
        
        print DF ")\n";
        print DF "SELECT /*+ use_hash(src, tgt) */ \n";
        print DF "    src.$COLUMNLIST[0]\n";
        
        foreach $CN (@COLUMNLIST)
        {
            if ($CN ne $COLUMNLIST[0])
            {    
                if (lc $CN eq "create_ts")
                {
                    # Teradata - using cast command
                    # print DF "    ,COALESCE(tgt.Create_Ts,CAST('$LOADTIME' AS TIMESTAMP(0)))\n";
                    # Oracle - replace cast command with to_timestamp
                    print DF "    ,NVL(tgt.Create_Ts,TO_TIMESTAMP('$LOADTIME', 'YYYY-MM-DD HH24:MI:SS'))\n";

                }elsif (lc $CN eq "refresh_ts")
                {
                    # Teradata - using cast command
                    # print DF "    ,CAST('$LOADTIME' AS TIMESTAMP(0))\n";
                    # Oracle - replace cast command with to_timestamp
                    print DF "    ,TO_TIMESTAMP('$LOADTIME', 'YYYY-MM-DD HH24:MI:SS')\n";
                    
                }else
                {
                    print DF "    ,src.$CN\n";
                }
            }
        }
        print DF "FROM \n";
        print DF "    ${SOURCEDBNAME}.${SOURCETABLENAME} src\n";
        print DF "LEFT OUTER JOIN\n";
        print DF "    ${TARGETDBNAME}.${TARGETTABLENAME} tgt\n";
        print DF "ON\n";
        print DF "    src.$PKLIST[0] = tgt.$PKLIST[0]\n";
        
        foreach $CN (@PKLIST)
        {
            if ($CN ne $PKLIST[0])
            {
                print DF "    AND src.$CN = tgt.$CN\n";
            }
        }
        print DF ";\n";        

        # HW - Output Number of rows
        print DF "  DBMS_OUTPUT.PUT_LINE(sql%rowcount || ' rows created');\n";
        print DF "  DBMS_OUTPUT.PUT_LINE('Elapsed: current_time ' || to_char(sysdate, 'YYYY-MM-DD HH24:MI:SS') );\n";

        # HW - add commit after insert statement
        print DF "commit;\n";

        # Insert statement 2 - No change 
  
        print DF " INSERT /*+ APPEND ${AUTOGEN_HINT} */  INTO ${TARGETDBNAME}.${TARGETTABLENAME}${SWITCH_POSTFIX_NEW}(\n";
        print DF "    $COLUMNLIST[0]\n";

        foreach $CN (@COLUMNLIST)
        {
            if ($CN ne $COLUMNLIST[0])
            { 
                print DF "    ,$CN\n";
            }
        }
        
        print DF ")\n";
        print DF "SELECT /*+ use_hash(src, tgt) */  \n";
        print DF "    src.$COLUMNLIST[0]\n";

        foreach $CN (@COLUMNLIST)
        {
            if ($CN ne $COLUMNLIST[0])
            { 
                print DF "    ,src.$CN\n";
            }
        }
        
        print DF "FROM\n"; 
        print DF "    ${TARGETDBNAME}.${TARGETTABLENAME} src\n";
        print DF "LEFT OUTER JOIN\n";
        print DF "    ${SOURCEDBNAME}.${SOURCETABLENAME} tgt\n";
        print DF "ON\n";
        print DF "    src.$PKLIST[0] = tgt.$PKLIST[0]\n";
        
        foreach $CN (@PKLIST)
        {
            if ($CN ne $PKLIST[0])
            {
                print DF "    AND src.$CN = tgt.$CN\n";
            }
        }               
        
        print DF "WHERE\n";
        print DF "    tgt.$PKLIST[0] IS NULL\n";
        
        foreach $CN (@PKLIST)
        {
            if ($CN ne $PKLIST[0])
            {
                print DF "    AND tgt.$CN IS NULL\n";
            }
        }
        
        print DF ";\n";

        # HW - Output Number of rows
        print DF "  DBMS_OUTPUT.PUT_LINE(sql%rowcount || ' rows created');\n";
        print DF "  DBMS_OUTPUT.PUT_LINE('Elapsed: current_time ' || to_char(sysdate, 'YYYY-MM-DD HH24:MI:SS') );\n";

        # HW - add commit after insert statement
        print DF "commit;\n";
 
    }   
    elsif ($GENTYPE == $HIST_SCRIPT_EXPIRE){
        # Teradata - Generate Insert statement for GENTYPE = HIST_SCRIPT_EXPIRE

        #special logic for history script generation (record will expire if missing in the source table)
        
        #Special script to check duplicate records
        # Oracle - Add the parent qeury to get duplicate PK count into x_count
        print DF "SELECT COUNT(1)\n";
        print DF "INTO x_count\n";
        print DF "FROM\n";
        print DF "(\n";

        print DF "SELECT \n";
        print DF "    $PKLIST[0]\n";
        
        foreach $CN (@PKLIST)
        {
            if ($CN ne $PKLIST[0])
            {
                print DF ",    $CN\n";
            }
        }         
        
        print DF "FROM\n";
        print DF "    ${SOURCEDBNAME}.${SOURCETABLENAME}\n";
        print DF "GROUP BY \n";
        print DF "    $PKLIST[0]\n";
        
        foreach $CN (@PKLIST)
        {
            if ($CN ne $PKLIST[0])
            {
                print DF ",    $CN\n";
            }
        }         
        
        # Teradata - change for pending parent query
        # print DF "HAVING COUNT(*) > 1;\n";
        print DF "HAVING COUNT(*) > 1);\n";
        print DF "IF x_count > 0 THEN\n";
        print DF "  RAISE x_pkey_exp;\n";
        print DF "END IF;\n";
        
        # Teradata - Remark due to incompatible to oracle         
        # print DF ".IF ACTIVITYCOUNT > 0 THEN .QUIT 1000000\n\n\n";


        #Special logic to check start_date
        # Teradata - Add select > into for oracle syntax 
        # print DF "SELECT\n";
        # print DF "   *\n";

        print DF "SELECT count(1)\n";
        print DF "INTO x_count\n";
        print DF "FROM \n";
        print DF "    (SELECT\n";
        print DF "        CASE WHEN ( MAX(START_DATE) + 1 > TO_DATE('${PROCESSDATE}','YYYY-MM-DD')) THEN 'N' ELSE 'Y' END AS STATUS\n";
        print DF "     FROM\n";
        print DF "         ${TARGETDBNAME}.${TARGETTABLENAME}\n";
        print DF "    ) a\n";
        print DF "WHERE\n";
        print DF "    a.STATUS = 'N';\n";
        # Teradata - Change to raise x_start_date_exp exception
        # print DF ".IF ACTIVITYCOUNT > 0 THEN .QUIT 1000000\n\n\n";
        print DF "IF x_count > 0 THEN\n";
        print DF "  RAISE x_start_date_exp;\n";
        print DF "END IF;\n";        
         

        # HW - add commit after insert statement
        print DF "commit;\n";
 
        # Insert statement 1
        print DF "INSERT /*+ APPEND ${AUTOGEN_HINT} */ INTO ${TARGETDBNAME}.${TARGETTABLENAME}${SWITCH_POSTFIX_NEW}(\n";
        print DF "    $COLUMNLIST[0]\n";
        
        foreach $CN (@COLUMNLIST)
        {
            if ($CN ne $COLUMNLIST[0])
            { 
                print DF "    ,$CN\n";
            }
        }
        
        print DF ")\n";
        print DF "SELECT /*+ use_hash(src, tgt) */ \n";
        print DF "    src.$COLUMNLIST[0]\n";
        
        foreach $CN (@COLUMNLIST)
        {
            if ($CN ne $COLUMNLIST[0])
            {                
                if (lc $CN eq "create_ts")
                {
                    # Teradata - using cast command 
                    # print DF "    ,COALESCE(tgt.create_ts,CAST('$LOADTIME' AS TIMESTAMP(0)))\n";
                    # Oracle - replace cast command with to_timestamp 
                    print DF "    ,NVL(tgt.create_ts,TO_TIMESTAMP('$LOADTIME', 'YYYY-MM-DD HH24:MI:SS'))\n";                
                }elsif (lc $CN eq "refresh_ts")
                {
                    # Teradata - using cast command
                    # print DF "    ,CAST('$LOADTIME' AS TIMESTAMP(0))\n";
                    # Oracle - replace cast command with to_timestamp
                    print DF "    ,TO_TIMESTAMP('$LOADTIME', 'YYYY-MM-DD HH24:MI:SS')\n";
                }elsif (lc $CN eq "start_date")
                {
                    # Teradata - using cast command
                    # print DF "    ,COALESCE(tgt.Start_Date,CAST('$PROCESSDATE' AS DATE FORMAT 'YYYY-MM-DD'))\n";
                    # Oracle - replace cast command with to_date
                    print DF "    ,NVL(tgt.Start_Date,TO_DATE('$PROCESSDATE', 'YYYY-MM-DD'))\n";
                }elsif (lc $CN eq "end_date")
                {
                    # Teradata - using cast command
                    # print DF "    ,CAST('$MAXDATE' AS DATE FORMAT 'YYYY-MM-DD')\n";
                    # Oracle - replace cast command with to_date 
                    print DF "    ,TO_DATE('$MAXDATE', 'YYYY-MM-DD')\n";
                }else{
                    print DF "    ,src.$CN\n";
                }
            }
        }
        print DF "FROM \n";
        print DF "    ${SOURCEDBNAME}.${SOURCETABLENAME} src\n";
        print DF "LEFT OUTER JOIN\n";
        print DF "    ${TARGETDBNAME}.${TARGETTABLENAME} tgt\n";
        print DF "ON\n";
        
        
        print DF "    src.$COLUMNLIST[0] = tgt.$COLUMNLIST[0]\n";
        
        foreach $CN (@COLUMNLIST)
        {
            if ($CN ne $COLUMNLIST[0])
            { 
                if ((lc $CN ne "create_ts") && (lc $CN ne "refresh_ts") && (lc $CN ne "start_date") && (lc $CN ne "end_date"))
                {
                    print DF "AND src.$CN = tgt.$CN\n";
                }
            }
        }
        # Teradata - using cast command  
        # print DF "AND tgt.end_date = CAST('$MAXDATE' AS DATE FORMAT 'YYYY-MM-DD')\n";
        # Oracle - replace cast command with to_date
        print DF "AND tgt.end_date = TO_DATE('$MAXDATE', 'YYYY-MM-DD')\n";        
        print DF ";\n";

        # HW - Output Number of rows
        print DF "  DBMS_OUTPUT.PUT_LINE(sql%rowcount || ' rows created');\n";
        print DF "  DBMS_OUTPUT.PUT_LINE('Elapsed: current_time ' || to_char(sysdate, 'YYYY-MM-DD HH24:MI:SS') );\n";

        # HW - add commit after insert statement
        print DF "commit;\n";
                                         
        # Insert statement 2        
        print DF "INSERT /*+ APPEND  ${AUTOGEN_HINT} */ INTO ${TARGETDBNAME}.${TARGETTABLENAME}${SWITCH_POSTFIX_NEW}(\n";
        print DF "    $COLUMNLIST[0]\n";
        
        foreach $CN (@COLUMNLIST)
        {
            if ($CN ne $COLUMNLIST[0])
            { 
                print DF "    ,$CN\n";
            }
        }
        
        print DF ")\n";
        print DF "SELECT /*+ use_hash(src, tgt) */ \n";
        print DF "    src.$COLUMNLIST[0]\n";
        
        foreach $CN (@COLUMNLIST)
        {
            if ($CN ne $COLUMNLIST[0])
            { 
                if (lc $CN eq "end_date")
                {
                    # Teradata - use cast command 
                    # print DF "    ,CAST('$PROCESSDATE' AS DATE FORMAT 'YYYY-MM-DD') - 1\n";
                    # Oracle - replace cast command with TO_DATE
                    print DF "    ,TO_DATE('$PROCESSDATE', 'YYYY-MM-DD') - 1\n";
                }else
                {
                    print DF "    ,src.$CN\n";
                }
            }
        }
        
        print DF "FROM\n";
        print DF "    ${TARGETDBNAME}.${TARGETTABLENAME} src\n";
        print DF "LEFT OUTER JOIN\n";
        print DF "    ${SOURCEDBNAME}.${SOURCETABLENAME} tgt\n";
        print DF "ON\n";
        
        #print DF "    src.$PKLIST[0] = tgt.$PKLIST[0]\n";
        #
        #foreach $CN (@PKLIST)
        #{
        #    if ($CN ne $PKLIST[0])
        #    {
        #        print DF "    AND src.$CN = tgt.$CN\n";
        #    }
        #}               
        
        print DF "    src.$COLUMNLIST[0] = tgt.$COLUMNLIST[0]\n";
        
        foreach $CN (@COLUMNLIST)
        {
            if ($CN ne $COLUMNLIST[0])
            { 
                if ((lc $CN ne "create_ts") && (lc $CN ne "refresh_ts") && (lc $CN ne "start_date") && (lc $CN ne "end_date"))
                {
                    print DF "AND src.$CN = tgt.$CN\n";
                }
            }
        }
        
        
        print DF "WHERE\n";
        # Teradata - Using cast command
        # print DF "    src.End_Date = CAST('$MAXDATE' AS DATE FORMAT 'YYYY-MM-DD')\n";
        # Oracle - Replace cast with to_date 
        print DF "    src.End_Date = TO_DATE('$MAXDATE', 'YYYY-MM-DD')\n";
        print DF "AND tgt.$PKLIST[0] IS NULL\n";
        
        foreach $CN (@PKLIST)
        {
            if ($CN ne $PKLIST[0])
            {
                print DF "    AND tgt.$CN IS NULL\n";
            }
        }
        print DF ";\n";

        # HW - Output Number of rows
        print DF "  DBMS_OUTPUT.PUT_LINE(sql%rowcount || ' rows created');\n";
        print DF "  DBMS_OUTPUT.PUT_LINE('Elapsed: current_time ' || to_char(sysdate, 'YYYY-MM-DD HH24:MI:SS') );\n";

        # HW - add commit after insert statement
        print DF "commit;\n";

        
        # Insert statement 3 
        print DF " INSERT /*+ APPEND  ${AUTOGEN_HINT} */ INTO ${TARGETDBNAME}.${TARGETTABLENAME}${SWITCH_POSTFIX_NEW}(\n";
        print DF "    $COLUMNLIST[0]\n";
        
        foreach $CN (@COLUMNLIST)
        {
            if ($CN ne $COLUMNLIST[0])
            { 
                print DF "    ,$CN\n";
            }
        }
        
        print DF ")\n";
        print DF "SELECT\n";
        print DF "    $COLUMNLIST[0]\n";
        
        foreach $CN (@COLUMNLIST)
        {
            if ($CN ne $COLUMNLIST[0])
            { 
                print DF "    ,$CN\n";
            }
        }
        
        print DF "FROM\n";
        print DF "    ${TARGETDBNAME}.${TARGETTABLENAME}\n";
        print DF "WHERE\n";
        # Teradata - Using cast command 
        # print DF "    End_Date <> CAST('$MAXDATE' AS DATE FORMAT 'YYYY-MM-DD');";
        # Oracle - Replace cast to to_date
        print DF "    End_Date <> TO_DATE('$MAXDATE', 'YYYY-MM-DD');\n";
        # HW - Output Number of rows
        print DF "  DBMS_OUTPUT.PUT_LINE(sql%rowcount || ' rows created');\n";
        print DF "  DBMS_OUTPUT.PUT_LINE('Elapsed: current_time ' || to_char(sysdate, 'YYYY-MM-DD HH24:MI:SS') );\n";

        print DF " commit;\n";

        #Special logic to check end_date
        print DF "\n\nSELECT count(1) \n";
        print DF "    into x_count\n";
        print DF "FROM \n";
        print DF "    ${TARGETDBNAME}.${TARGETTABLENAME}${SWITCH_POSTFIX_NEW}\n";
        print DF "WHERE\n";
        print DF "    End_Date < Start_Date;\n";
        # Teradat - Replaced by exception 
        #        print DF ".IF ACTIVITYCOUNT > 0 THEN .QUIT 1000000  \n\n\n";
        print DF "IF x_count > 0 THEN\n";
        print DF "  RAISE x_start_end_date_exp;\n";
        print DF "END IF;\n";



        #Special logic to check end_date
        # Teradata - select without into syntax 
        # print DF "\n\nSELECT\n";
        # print DF "    *\n";
        # Oracle - Using select into 
        print DF "\n\nSELECT count(1)\n";
        print DF "    into x_count\n";
        print DF "FROM \n";
        print DF "    ${TARGETDBNAME}.${TARGETTABLENAME}${SWITCH_POSTFIX_NEW}\n";
        print DF "WHERE\n";
        print DF "    End_Date < Start_Date;\n";
        # Teradata - Replaced by exception 
        # print DF ".IF ACTIVITYCOUNT > 0 THEN .QUIT 1000000 \n\n\n";
        print DF "IF x_count > 0 THEN\n";
        print DF "  RAISE x_start_end_date_exp;\n";
        print DF "END IF;\n";
        
           
    }
    elsif ($GENTYPE == $HIST_SCRIPT_NON_EXPIRE){
        # Teradata - Generate Insert statement for GENTYPE = HIST_SCRIPT_NON_EXPIRE
        #special logic for history script generation (record will expire if missing in the source table)
        
        #Special script to check duplicate records

        # Oracle - Add the parent qeury to get duplicate PK count into x_count
        print DF "SELECT COUNT(1)\n";
        print DF "INTO x_count\n";
        print DF "FROM\n";
        print DF "(\n";
        print DF "SELECT \n";
        print DF "    $PKLIST[0]\n";

        
        foreach $CN (@PKLIST)
        {
            if ($CN ne $PKLIST[0])
            {
                print DF ",    $CN\n";
            }
        }         
        
        print DF "FROM\n";
        print DF "    ${SOURCEDBNAME}.${SOURCETABLENAME}\n";
        print DF "GROUP BY \n";
        print DF "    $PKLIST[0]\n";
        
        foreach $CN (@PKLIST)
        {
            if ($CN ne $PKLIST[0])
            {
                print DF ",    $CN\n";
            }
        }         
        # Teradata - change for pending parent query
        # print DF "HAVING COUNT(*) > 1;\n";
        # print DF ".IF ACTIVITYCOUNT > 0 THEN .QUIT 1000000 \n\n\n";
        print DF "HAVING COUNT(*) > 1);\n";
        print DF "IF x_count > 0 THEN\n";
        print DF "  RAISE x_pkey_exp;\n";
        print DF "END IF;\n";  
        

        #Special logic to check start_date
        # Teradata - select without into 
        # print DF "SELECT\n";
        # print DF "    *\n";
        print DF "SELECT COUNT(1)\n";
        print DF "INTO x_count\n";
        print DF "FROM \n";
        print DF "    (SELECT\n";
        print DF "        CASE WHEN ( MAX(START_DATE) + 1 > to_date('$PROCESSDATE','YYYY-MM-DD') ) THEN 'N' ELSE 'Y' END AS STATUS\n";
        print DF "     FROM\n";
        print DF "         ${TARGETDBNAME}.${TARGETTABLENAME}\n";
        print DF "    ) a\n";
        print DF "WHERE\n";
        print DF "    a.STATUS = 'N';\n";
        # Teradata - Remark the following line due to incompatible in oracle 
        # print DF ".IF ACTIVITYCOUNT > 0 THEN .QUIT 1000000 \n\n\n";
        print DF "IF x_count > 0 THEN\n";
        print DF "  RAISE x_start_date_exp;\n";
        print DF "END IF;\n"; 


        # HW - add commit after insert statement
        print DF "commit;\n";
        

        # Insert statement 1 
        print DF "INSERT /*+ APPEND  ${AUTOGEN_HINT} */ INTO ${TARGETDBNAME}.${TARGETTABLENAME}${SWITCH_POSTFIX_NEW}(\n";
        print DF "    $COLUMNLIST[0]\n";
        
        foreach $CN (@COLUMNLIST)
        {
            if ($CN ne $COLUMNLIST[0])
            {
                print DF "    ,$CN\n";
            }
        }
        
        print DF ")\n";
        print DF "SELECT /*+ use_hash(src, tgt) */  \n";
        print DF "    src.$COLUMNLIST[0]\n";
        
        foreach $CN (@COLUMNLIST)
        {
            if ($CN ne $COLUMNLIST[0])
            {    
                if (lc $CN eq "create_ts")
                {
                    # Teradata - Using cast command 
                    # print DF "    ,COALESCE(tgt.create_ts,CAST('$LOADTIME' AS TIMESTAMP(0)))\n";
                    # Oracle - Replace cast with to_timestamp
                    print DF "    ,NVL(tgt.create_ts,TO_TIMESTAMP('$LOADTIME', 'YYYY-MM-DD HH24:MI:SS'))\n";
                }elsif (lc $CN eq "refresh_ts")
                {
                    # Teradata - Using cast command 
                    # print DF "    ,CAST('$LOADTIME' AS TIMESTAMP(0))\n";
                    # Oracle - Replace cast with to_timestamp
                    print DF "    ,TO_TIMESTAMP('$LOADTIME', 'YYYY-MM-DD HH24:MI:SS')\n";
                }elsif (lc $CN eq "start_date")
                {
                    # Teradata - using cast command 
                    # print DF "    ,COALESCE(tgt.Start_Date,CAST('$PROCESSDATE' AS DATE FORMAT 'YYYY-MM-DD'))\n";
                    # Oracle - replace cast by to_date 
                    print DF "    ,NVL(tgt.Start_Date,TO_DATE('$PROCESSDATE', 'YYYY-MM-DD'))\n";
                }elsif (lc $CN eq "end_date")
                {
                    # Teradata - using cast command 
                    # print DF "    ,CAST('$MAXDATE' AS DATE FORMAT 'YYYY-MM-DD')\n";
                    # Oracle - replace cast by to_date
                    print DF "    ,TO_DATE('$MAXDATE', 'YYYY-MM-DD')\n";
                }else
                {
                    print DF "    ,src.$CN\n";
                }
            }
        }
        print DF "FROM \n";
        print DF "    ${SOURCEDBNAME}.${SOURCETABLENAME} src\n";
        print DF "LEFT OUTER JOIN\n";
        print DF "    ${TARGETDBNAME}.${TARGETTABLENAME} tgt\n";
        print DF "ON\n";
        
        
        print DF "    src.$COLUMNLIST[0] = tgt.$COLUMNLIST[0]\n";
        
        foreach $CN (@COLUMNLIST)
        {
            if ($CN ne $COLUMNLIST[0])
            { 
                if ((lc $CN ne "create_ts") && (lc $CN ne "refresh_ts") && (lc $CN ne "start_date") && (lc $CN ne "end_date"))
                {
                    print DF "AND src.$CN = tgt.$CN\n";
                }
            }
        }
        
        # Teradata - Using cast command  
        # print DF "AND tgt.end_date = CAST('$MAXDATE' AS DATE FORMAT 'YYYY-MM-DD')\n";
        # Oracle - Replace cast by TO_DATE 
        print DF "AND tgt.end_date = TO_DATE('$MAXDATE', 'YYYY-MM-DD')\n";
        print DF ";\n";

        # HW - Output Number of rows
        print DF "  DBMS_OUTPUT.PUT_LINE(sql%rowcount || ' rows created');\n";
        print DF "  DBMS_OUTPUT.PUT_LINE('Elapsed: current_time ' || to_char(sysdate, 'YYYY-MM-DD HH24:MI:SS') );\n";

        # HW - add commit after insert statement
        print DF "commit;\n";
                                  
        # insert statement 2 
        print DF " INSERT /*+ APPEND  ${AUTOGEN_HINT} */ INTO ${TARGETDBNAME}.${TARGETTABLENAME}${SWITCH_POSTFIX_NEW}(\n";
        print DF "    $COLUMNLIST[0]\n";
        
        foreach $CN (@COLUMNLIST)
        {
            if ($CN ne $COLUMNLIST[0])
            { 
                print DF "    ,$CN\n";
            }
        }
        
        print DF ")\n";
        print DF "SELECT /*+ use_hash(src, tgt) */ \n";
        print DF "    src.$COLUMNLIST[0]\n";
        
        foreach $CN (@COLUMNLIST)
        {
            if ($CN ne $COLUMNLIST[0])
            { 
                print DF "    ,src.$CN\n";
            }
        }
        
        print DF "FROM\n";
        print DF "    ${TARGETDBNAME}.${TARGETTABLENAME} src\n";
        print DF "LEFT OUTER JOIN\n";
        print DF "    ${SOURCEDBNAME}.${SOURCETABLENAME} tgt\n";
        print DF "ON\n";
        print DF "    src.$PKLIST[0] = tgt.$PKLIST[0]\n";
        
        foreach $CN (@PKLIST)
        {
            if ($CN ne $PKLIST[0])
            {
                print DF "    AND src.$CN = tgt.$CN\n";
            }
        }               
        
        print DF "WHERE\n";
        # Teradata - using cast command 
        # print DF "    src.End_Date = CAST('$MAXDATE' AS DATE FORMAT 'YYYY-MM-DD')\n";
        # Oracle - Replace cast by to_date
        print DF "    src.End_Date = TO_DATE('$MAXDATE', 'YYYY-MM-DD')\n";

        print DF "AND tgt.$PKLIST[0] IS NULL\n";
        
        foreach $CN (@PKLIST)
        {
            if ($CN ne $PKLIST[0])
            {
                print DF "    AND tgt.$CN IS NULL\n";
            }
        }
        print DF ";\n";
        # HW - Output Number of rows
        print DF "  DBMS_OUTPUT.PUT_LINE(sql%rowcount || ' rows created');\n";
        print DF "  DBMS_OUTPUT.PUT_LINE('Elapsed: current_time ' || to_char(sysdate, 'YYYY-MM-DD HH24:MI:SS') );\n";

        # HW - add commit after insert statement
        print DF "commit;\n";
 
        
        # Insert statement 3 
        print DF " INSERT /*+ APPEND  ${AUTOGEN_HINT} */ INTO ${TARGETDBNAME}.${TARGETTABLENAME}${SWITCH_POSTFIX_NEW}(\n";
        print DF "    $COLUMNLIST[0]\n";
        
        foreach $CN (@COLUMNLIST)
        {
            if ($CN ne $COLUMNLIST[0])
            { 
                print DF "    ,$CN\n";
            }
        }
        
        print DF ")\n";
        print DF "SELECT /*+ use_hash(src, tgt) */  \n";
        print DF "    src.$COLUMNLIST[0]\n";
        
        foreach $CN (@COLUMNLIST)
        {
            if ($CN ne $COLUMNLIST[0])
            {
                
                if (lc $CN eq "refresh_ts")
                {
                    # Teradata - Using cast command
                    # print DF "    ,CAST('$LOADTIME' AS TIMESTAMP(0))\n";
                    # Oracle - Replace cast by to_timestamp
                    print DF "    ,TO_TIMESTAMP('$LOADTIME', 'YYYY-MM-DD HH24:MI:SS')\n";
                }elsif (lc $CN eq "end_date")
                {
                    # Teradata - Using cast command 
                    # print DF "    ,CAST('$PROCESSDATE' AS DATE FORMAT 'YYYY-MM-DD') - 1\n";
                    # Oracle - Replace cast by to_date 
                    print DF "    ,TO_DATE('$PROCESSDATE', 'YYYY-MM-DD') - 1\n";
                }else
                {
                    print DF "    ,src.$CN\n";
                }
            }
        }
        print DF "FROM \n";
        print DF "    ${TARGETDBNAME}.${TARGETTABLENAME} src\n";
        print DF "LEFT OUTER JOIN\n";
        print DF "    ${SOURCEDBNAME}.${SOURCETABLENAME} tgt\n";
        print DF "ON\n";
        print DF "    src.$PKLIST[0] = tgt.$PKLIST[0]\n";
        
        foreach $CN (@PKLIST)
        {
            if ($CN ne $PKLIST[0])
            {
                print DF "    AND src.$CN = tgt.$CN\n";
            }
        }
        # Teradata - Using cast command      
        # print DF "WHERE src.end_date = CAST('$MAXDATE' AS DATE FORMAT 'YYYY-MM-DD')\n";     
        # Oracle - Replace cast by to_date 
        print DF "WHERE src.end_date = TO_DATE('$MAXDATE', 'YYYY-MM-DD')\n";   
        
        if (@PKLIST < (@COLUMNLIST - 4))
        {
        
            print DF "AND (\n";
        
            my $NOTPK = 1;
            my $GENCOUNT = 1;
        
            foreach $CN (@COLUMNLIST)
            {
                $NOTPK = 1;
                foreach $PN (@PKLIST)
                {
                    if (lc $CN eq lc $PN)
                    {
                        $NOTPK = 0;
                    }
                }    
            
            
                if ($NOTPK == 1)
                { 
                    if ((lc $CN ne "create_ts") && (lc $CN ne "refresh_ts") && (lc $CN ne "start_date") && (lc $CN ne "end_date"))
                    {
                        if ($GENCOUNT == 1)
                        {
                            print DF "    src.$CN <> tgt.$CN\n";
                        }else
                        {
                            print DF "    OR src.$CN <> tgt.$CN\n";
                        }
                        $GENCOUNT = $GENCOUNT + 1;
                    }
                }
            }
            print DF ")\n";
        
          
        }
        
        print DF ";\n";
        # HW - Output Number of rows
        print DF "  DBMS_OUTPUT.PUT_LINE(sql%rowcount || ' rows created');\n";
        print DF "  DBMS_OUTPUT.PUT_LINE('Elapsed: current_time ' || to_char(sysdate, 'YYYY-MM-DD HH24:MI:SS') );\n";

        # HW - add commit after insert statement
        print DF "commit;\n";

        # Insert statement 4 
        print DF " INSERT /*+ APPEND  ${AUTOGEN_HINT} */ INTO ${TARGETDBNAME}.${TARGETTABLENAME}${SWITCH_POSTFIX_NEW}(\n";
        print DF "    $COLUMNLIST[0]\n";
        
        foreach $CN (@COLUMNLIST)
        {
            if ($CN ne $COLUMNLIST[0])
            { 
                print DF "    ,$CN\n";
            }
        }
        
        print DF ")\n";
        print DF "SELECT\n";
        print DF "    $COLUMNLIST[0]\n";
        
        foreach $CN (@COLUMNLIST)
        {
            if ($CN ne $COLUMNLIST[0])
            { 
                print DF "    ,$CN\n";
            }
        }
        
        print DF "FROM\n";
        print DF "    ${TARGETDBNAME}.${TARGETTABLENAME}\n";
        print DF "WHERE\n";
        # Teradata - Using cast command 
        # print DF "    End_Date <> CAST('$MAXDATE' AS DATE FORMAT 'YYYY-MM-DD');";
        # Oracle - Replace cast by to_date 
        print DF "    End_Date <> TO_DATE('$MAXDATE', 'YYYY-MM-DD');\n";

        # HW - Output Number of rows
        print DF "  DBMS_OUTPUT.PUT_LINE(sql%rowcount || ' rows created');\n";
        print DF "  DBMS_OUTPUT.PUT_LINE('Elapsed: current_time ' || to_char(sysdate, 'YYYY-MM-DD HH24:MI:SS') );\n";

        # HW - add commit after insert statement
        print DF "commit;\n";

        #Special logic to check end_date
        # Teradata using select without into 
        # print DF "\n\nSELECT\n";
        # print DF "    *\n";
        print DF "\n\nSELECT COUNT(1)\n";
        print DF "INTO  x_count\n";
        print DF "FROM \n";
        print DF "    ${TARGETDBNAME}.${TARGETTABLENAME}${SWITCH_POSTFIX_NEW}\n";
        print DF "WHERE\n";
        print DF "    End_Date < Start_Date;\n";
        # Teradata - remark due to incompatible sytnax in oracle 
        # print DF ".IF ACTIVITYCOUNT > 0 THEN .QUIT 1000000 \n\n\n";
        print DF "IF x_count > 0 THEN\n";
        print DF "  RAISE x_start_end_date_exp;\n";
        print DF "END IF;\n";             

    }
    elsif ($GENTYPE == $APPEND_SCRIPT)
    {
        # Teradata - Generate Insert statement for GENTYPE = APPEND

        #special logic for append script generation
        # Teradata - Using cast command 
        # print DF "UPDATE ${ETLDB}.ETL_TABLE_DATE SET Last_Append_Ts = CAST('$LOADTIME' AS TIMESTAMP(0)) WHERE Table_Name = '${TARGETTABLENAME}';\n\n";
        # Oracle - replace cast with to_timestamp
        print DF "UPDATE ${ETLDB}.ETL_TABLE_DATE SET Last_Append_Ts = TO_TIMESTAMP('$LOADTIME', 'YYYY-MM-DD HH24:MI:SS') WHERE Table_Name = '${TARGETTABLENAME}';\n\n";
        
        # HW - Output Number of rows
        print DF "  DBMS_OUTPUT.PUT_LINE(sql%rowcount || ' rows updated');\n";
        print DF "  DBMS_OUTPUT.PUT_LINE('Elapsed: current_time ' || to_char(sysdate, 'YYYY-MM-DD HH24:MI:SS') );\n";

        # HW - add commit after insert statement
        print DF "commit;\n";


        # HW - debuging purpose, check record count in temporary table         
        print DF " SELECT COUNT(1) INTO x_count_debug from ${SOURCEDBNAME}.${SOURCETABLENAME}; \n";
        print DF " \n";
        print DF " DBMS_OUTPUT.PUT_LINE('No. of recs in ${SOURCEDBNAME}.${SOURCETABLENAME}:' || x_count_debug); \n";

        print DF "INSERT /*+ APPEND  ${AUTOGEN_HINT} */ INTO ${TARGETDBNAME}.${TARGETTABLENAME}(\n";
        print DF "    $COLUMNLIST[0]\n";
        
        foreach $CN (@COLUMNLIST)
        {
            if ($CN ne $COLUMNLIST[0])
            { 
                print DF "    ,$CN\n";
            }
        }
        
        print DF ")\n";
        print DF "SELECT\n";
        print DF "    $COLUMNLIST[0]\n";
        
        foreach $CN (@COLUMNLIST)
        {
            if ($CN ne $COLUMNLIST[0])
            {
                if (lc $CN eq "create_ts")
                {
                    # Teradata - Use cast command 
                    # print DF "    ,CAST('$LOADTIME' AS TIMESTAMP(0))\n";
                    # Oracle - Replace cast by to_timestamp 
                    print DF "    ,TO_TIMESTAMP('$LOADTIME', 'YYYY-MM-DD HH24:MI:SS')\n";
                }elsif (lc $CN eq "refresh_ts"){
                    # Teradata - Use cast command
                    # print DF "    ,CAST('$LOADTIME' AS TIMESTAMP(0))\n";
                    # Oracle - Replace cast by to_timestamp
                    print DF "    ,TO_TIMESTAMP('$LOADTIME', 'YYYY-MM-DD HH24:MI:SS')\n"; 
                }else{
                    print DF "    ,$CN\n";
                }
            }
        }
        
        print DF "FROM\n";
        print DF "    ${SOURCEDBNAME}.${SOURCETABLENAME}\n";
        print DF ";\n";

        # HW - Output Number of rows
        print DF "  DBMS_OUTPUT.PUT_LINE(sql%rowcount || ' rows created');\n";
        print DF "  DBMS_OUTPUT.PUT_LINE('Elapsed: current_time ' || to_char(sysdate, 'YYYY-MM-DD HH24:MI:SS') );\n";

        # HW - add commit after insert statement
        print DF "commit;\n";
 

        print DF "UPDATE ${ETLDB}.ETL_TABLE_DATE SET Last_Append_Ts = NULL WHERE Table_Name = '${TARGETTABLENAME}';\n\n";

        # HW - Output Number of rows
        print DF "  DBMS_OUTPUT.PUT_LINE(sql%rowcount || ' rows updated');\n";
        print DF "  DBMS_OUTPUT.PUT_LINE('Elapsed: current_time ' || to_char(sysdate, 'YYYY-MM-DD HH24:MI:SS') );\n";

        # HW - add commit after insert statement
        print DF "commit;\n";
        

    }
    else
    {
        print "GENTYPE: $GENTYPE not exist";
        return (-1);
    } 
 
    # Teradata - Used for switch table logic, remark in oracle first  
    # my $NSQLTEXT = "SELECT CASE WHEN SUM(Currentperm) < ${BT_TBL_SIZE} THEN 'Y' ELSE 'N' END FROM dbc.Allspace WHERE Databasename = '${TARGETDBNAME}' AND Tablename = '${TARGETTABLENAME}'";
             
                 
    # my $NSTH = $DBH->prepare($NSQLTEXT);
    # unless ($NSTH) 
    # {
    #    return -1;
    # }
    
    # $NSTH->execute();

    # my @TABROW;

    # @TABROW = $NSTH->fetchrow();
    # $NSTH->finish();
    # End for switch table check 
                                                             
    disconnectETL($DBH);    
    
    
    ##The below script is used to generate the switch table logic
    # Teradata - Rebuild table for all case except append
    # if (($GENTYPE != $APPEND_SCRIPT) && ($TABROW[0] eq 'N'))
    # {
    #    print DF "\n\n--Ready to switch the table\n\n";
    #    
    #    print DF "DROP TABLE ${TARGETDBNAME}.${TARGETTABLENAME}${SWITCH_POSTFIX_OLD};\n\n";
    #    
    #    print DF "RENAME TABLE\n";
    #    print DF "    ${TARGETDBNAME}.${TARGETTABLENAME}\n";
    #    print DF "AS\n";
    #    print DF "    ${TARGETDBNAME}.${TARGETTABLENAME}${SWITCH_POSTFIX_OLD};\n\n";
    #        
    #    print DF "RENAME TABLE\n";
    #    print DF "    ${TARGETDBNAME}.${TARGETTABLENAME}${SWITCH_POSTFIX_NEW}\n";
    #    print DF "AS\n";
    #    print DF "    ${TARGETDBNAME}.${TARGETTABLENAME};\n\n";
    #       
    # }
    
    
    # if (($GENTYPE != $APPEND_SCRIPT) && ($TABROW[0] eq 'Y'))
    # {
    #    print DF "\n\n--Ready to use BT ET\n\n";
    #    
    #    print DF "CREATE MULTISET TABLE \n";
    #    print DF "    ${TARGETDBNAME}.${TARGETTABLENAME}${SWITCH_POSTFIX_OLD}\n"; 
    #    print DF "AS \n";
    #    print DF "    ${TARGETDBNAME}.${TARGETTABLENAME} WITH NO DATA;\n\n";
    #    
    #    print DF "DEL FROM ${TARGETDBNAME}.${TARGETTABLENAME}${SWITCH_POSTFIX_OLD} ALL;\n\n";
    #    
    #    print DF "INSERT INTO ${TARGETDBNAME}.${TARGETTABLENAME}${SWITCH_POSTFIX_OLD}\n";
    #    print DF "SELECT * FROM ${TARGETDBNAME}.${TARGETTABLENAME};\n\n";
    #    
    #    
    #    print DF "BT;\n\n";
    #    
    #    
    #    print DF "DEL FROM ${TARGETDBNAME}.${TARGETTABLENAME} ALL;\n\n";
    #    
    #    print DF "INSERT INTO ${TARGETDBNAME}.${TARGETTABLENAME}\n";
    #    print DF "SELECT * FROM ${TARGETDBNAME}.${TARGETTABLENAME}${SWITCH_POSTFIX_NEW};\n\n";
    #    
    #    print DF "ET;\n\n";
    #    
    #    print DF "DROP TABLE ${TARGETDBNAME}.${TARGETTABLENAME}${SWITCH_POSTFIX_NEW};\n\n";
    #    
    #       
    # }
   
    if ($GENTYPE != $APPEND_SCRIPT)
    {
        # oracle - call a prcedure to rename tables as follow logic:
        # 1. remove all data from _OLD table
        # 2. rename _OLD table to _TMP
        # 3. rename original table to _OLD
        # 4. rename _NEW table to original table
        # 5. rename _TMP table to _NEW table
        print DF "\n";
        print DF "  ${UTLDB}.ETL_UTILITY.SWAP_TABLE('${TARGETDBNAME}','${TARGETTABLENAME}');\n";

    }

    # 2016-03-05 HW Disable force parallel DML and move gather stats outside the SQL block
    # print DF " alter session disable parallel query; \n ";
    # print DF " alter session disable parallel dml; \n ";

    # if gentype = hist_script_non_expire, run gather stat for the target table 
    # if ($GENTYPE == $HIST_SCRIPT_NON_EXPIRE)
    # {
    #     print DF "\n";
    #    print DF "  ${UTLDB}.ETL_UTILITY.GATHER_TABLE_STATS(P_SCHEMA=>'${TARGETDBNAME}' , P_TABLE =>'${TARGETTABLENAME}');\n ";
    #    print DF "  DBMS_OUTPUT.PUT_LINE('Gather Stats Time: current_time ' || to_char(sysdate, 'YYYY-MM-DD HH24:MI:SS') );\n ";
    # }
   
    # if ($GENTYPE == $HIST_SCRIPT_EXPIRE)
    # {
    #    print DF "\n";
    #    print DF "  ${UTLDB}.ETL_UTILITY.GATHER_TABLE_STATS(P_SCHEMA=>'${TARGETDBNAME}' , P_TABLE =>'${TARGETTABLENAME}');\n ";
    #    print DF "  DBMS_OUTPUT.PUT_LINE('Gather Stats Time: current_time ' || to_char(sysdate, 'YYYY-MM-DD HH24:MI:SS') );\n ";
    # }



    # Oracle - Append the ending commands at the end of SQL script file 
    print DF "exception\n";
    print DF "  when x_pkey_exp then\n";
    print DF "    DBMS_OUTPUT.PUT_LINE('Duplicate Primary Key records for ${SOURCEDBNAME}.${SOURCETABLENAME}');\n";
    print DF "    raise; \n";
    print DF "  when x_start_date_exp then\n";
    print DF "    DBMS_OUTPUT.PUT_LINE('Future date exists for ${TARGETDBNAME}.${TARGETTABLENAME}');\n";
    print DF "    raise; \n";
    print DF "  when x_start_end_date_exp then\n";
    print DF "    DBMS_OUTPUT.PUT_LINE('End date is earlier than start date for ${TARGETDBNAME}.${TARGETTABLENAME}${SWITCH_POSTFIX_NEW}');\n";
    print DF "    raise; \n";
    print DF "  when others then\n";
    print DF "    DBMS_OUTPUT.PUT_LINE('Unexpected error found!!  Please review the sql error message for detail.');\n";
    print DF "    DBMS_OUTPUT.PUT_LINE(sqlerrm);\n";
    print DF "    raise; \n";
    print DF "end;\n";    
    print DF "/ \n";

    # 2016-03-05 HW Disable force parallel DML and move gather stats out of SQL block
    print DF " alter session disable parallel query; \n ";
    print DF " alter session disable parallel dml;   \n "; 

    # 2016-03-31 HW remove gather statistics due and proceed by online gather statistics feature 
    # 2016-04-05 HW rollback the logic due to online gather statistics feature cannot be used by partition tbale 
    #
    if ($GENTYPE == $HIST_SCRIPT_NON_EXPIRE)
    {
       print DF "\n";
       print DF " execute ${UTLDB}.ETL_UTILITY.GATHER_TABLE_STATS(P_SCHEMA=>'${TARGETDBNAME}' , P_TABLE =>'${TARGETTABLENAME}');\n ";
    }

    if ($GENTYPE == $HIST_SCRIPT_EXPIRE)
    {
       print DF "\n";
        print DF " execute ${UTLDB}.ETL_UTILITY.GATHER_TABLE_STATS(P_SCHEMA=>'${TARGETDBNAME}' , P_TABLE =>'${TARGETTABLENAME}');\n ";
    }

    close DF;
    
    # Teradata - using bteq
    # $rc = system("bteq < $AUTO_GEN_SCRIPT $ulog");
    # Oracle - using sqlplus 
    $rc = system("sqlplus /\@${TDDSN} \@${AUTO_GEN_SCRIPT}");
    
    
    
    if ($rc == 0)
    {
        if ((${wday} == 0) or ($GENTYPE != $APPEND_SCRIPT))
        {
            $rc = collectStatisticByPI(${TARGETTABLENAME},${TARGETDBNAME});
        }
    }
    
    
    if ($rc == 0)
    {
        $rc = collectStatistic(${TARGETTABLENAME},${TARGETDBNAME});
    }
    
    
    if ($rc != 0) 
    { 
        return 1;
    }
    else
    {
        return 0;
    }
    
}



#This is a function used to connect the Teradata database
sub connectETL
{
    DBI->trace(0);
    my $DBH = DBI->connect("dbi:Oracle:$TDDSN","","", { AutoCommit => 1 } ) ;
    unless ( defined($DBH) ) { return undef; }
    return $DBH;
}

#This is a function used to disconnect the Teradata database
sub disconnectETL
{
    my ($DBH) = @_;
    unless ( $DBH->disconnect() ) { return -1; }
    return 0;
}


#This is a function used to trim the space of a string
sub trim($)
{
    my $string = shift;
    $string =~ s/^\s+//;
    $string =~ s/\s+$//;
    return $string;
}






sub genFirstDayOfMonth
{
    
    my $D_MONTH = pop(@_);
    
    my $CETL = connectETL();
    
    unless ( defined($CETL) ) 
    {
        print "ERROR - Unable to connect to TD!\n";
        my $ERRSTR = $DBI::errstr;
        print "$ERRSTR\n";
        return -1;
    }
    
    my $NEXTSEQ = 0;
    
    
    while ( $NEXTSEQ < 13){
        my $SQLTEXT = "SELECT TO_CHAR(ADD_MONTHS(trunc(TO_DATE('$D_MONTH','YYYY-MM-DD'),'MM'),-${NEXTSEQ}),'YYYY-MM-DD') from dual";
        # print "${SQLTEXT}";
        # print "\n";
        # my $SQLTEXT = "SELECT";
        # $SQLTEXT = $SQLTEXT . " SUBSTR(CAST(ADD_MONTHS(CAST('$D_MONTH' AS DATE FORMAT 'YYYY-MM-DD'),- ${NEXTSEQ}) AS CHAR(10)),1,4) || '-' ||";
        # $SQLTEXT = $SQLTEXT . " SUBSTR(CAST(ADD_MONTHS(CAST('$D_MONTH' AS DATE FORMAT 'YYYY-MM-DD'),- ${NEXTSEQ}) AS CHAR(10)),6,2) || '-01'";
    
        my $CTH;

        $CTH = $CETL->prepare($SQLTEXT);
        unless ($CTH) 
        {
            return -1;
        }

        $CTH->execute();
        my @TROW;
        @TROW = $CTH->fetchrow() ;
        $F_D_MONTH[$NEXTSEQ] = $TROW[0];
        $NEXTSEQ = $NEXTSEQ + 1;
        $CTH->finish();
    }
    disconnectETL($CETL);
}




sub genSecondDayOfMonth
{
    
    my $D_MONTH = pop(@_);
    
    my $CETL = connectETL();
    
    unless ( defined($CETL) ) 
    {
        print "ERROR - Unable to connect to TD!\n";
        my $ERRSTR = $DBI::errstr;
        print "$ERRSTR\n";
        return -1;
    }
    
    my $NEXTSEQ = 0;
    
    
    while ( $NEXTSEQ < 13){

        my $SQLTEXT = "SELECT SUBSTR(CAST(ADD_MONTHS(TO_DATE('$D_MONTH','YYYY-MM-DD'),-${NEXTSEQ}) AS CHAR(10)),1,4) || '-' || SUBSTR(CAST(ADD_MONTHS(TO_DATE('$D_MONTH','YYYY-MM-DD'),-${NEXTSEQ}) AS CHAR(10)),6,2) || '-02' from dual";
    
        # my $SQLTEXT = "SELECT";
        # $SQLTEXT = $SQLTEXT . " SUBSTR(CAST(ADD_MONTHS(CAST('$D_MONTH' AS DATE FORMAT 'YYYY-MM-DD'),- ${NEXTSEQ}) AS CHAR(10)),1,4) || '-' ||";
        # $SQLTEXT = $SQLTEXT . " SUBSTR(CAST(ADD_MONTHS(CAST('$D_MONTH' AS DATE FORMAT 'YYYY-MM-DD'),- ${NEXTSEQ}) AS CHAR(10)),6,2) || '-02'";
   
        my $CTH;
        $CTH = $CETL->prepare($SQLTEXT);
        unless ($CTH) 
        {
            return -1;
        }
        $CTH->execute();
        my @TROW;
        @TROW = $CTH->fetchrow() ;
        $S_D_MONTH[$NEXTSEQ] = $TROW[0];
        $NEXTSEQ = $NEXTSEQ + 1;
        $CTH->finish();
    }
    
    disconnectETL($CETL);
    
}


sub genCDRTXDate
{
    my $CDRTABLE = pop(@_);
    my $CETL = connectETL();
    
    unless ( defined($CETL) ) 
    {
        print "ERROR - Unable to connect to TD!\n";
        my $ERRSTR = $DBI::errstr;
        print "$ERRSTR\n";
        return -1;
    }
    
    
    # my $SQLTEXT = "SELECT CAST(Call_Start_Date as char(10)) FROM ${CDRTABLE} GROUP BY 1 ORDER BY 1 ASC";
    my $SQLTEXT = "SELECT TO_CHAR(Call_Start_Date,'YYYY-MM-DD') FROM ${CDRTABLE} GROUP BY TO_CHAR(Call_Start_Date,'YYYY-MM-DD') ORDER BY 1 ASC ";
    my $CTH;

    $CTH = $CETL->prepare($SQLTEXT);
    unless ($CTH) 
    {
        print ("\n $SQLTEXT \n");
        return -1;
    }

    $CTH->execute();

    my $LINE = 1;
    my @TROW;

    my $TX_STR = "";
    
    while ( @TROW = $CTH->fetchrow() ) 
    {
        if ($LINE == 1) 
        {
            $TX_STR = "'$TROW[0]'";  
            $LINE = 2;
        }else
        {
            $TX_STR = $TX_STR . ",'$TROW[0]'";  
        }
    }
    
    
    $CTH->finish();
    disconnectETL($CETL);
    
    return($TX_STR);
    
}


sub genCDRMinTXDate
{
    my ($FIELD_START_DATE,$CDRTABLE) = @_;
    my $CETL = connectETL();
    
    unless ( defined($CETL) ) 
    {
        print "ERROR - Unable to connect to TD!\n";
        my $ERRSTR = $DBI::errstr;
        print "$ERRSTR\n";
        return -1;
    }
    
    
    # my $SQLTEXT = "SELECT MIN(CAST(${FIELD_START_DATE}  as char(10))) FROM ${CDRTABLE}";
    my $SQLTEXT = "SELECT MIN(TO_CHAR(${FIELD_START_DATE},'YYYY-MM-DD')) FROM ${CDRTABLE}";
    my $CTH;

    $CTH = $CETL->prepare($SQLTEXT);
    unless ($CTH) 
    {
        print ("\n $SQLTEXT \n");
        return -1;
    }

    $CTH->execute();

    my @TROW;
    my $TX_STR = "";
    
    @TROW = $CTH->fetchrow();
    
    $TX_STR = "'$TROW[0]'";
    
    $CTH->finish();
    disconnectETL($CETL);
    
    return($TX_STR);
    
}



sub genCDRMaxTXDate
{
    my ($FIELD_START_DATE,$CDRTABLE) = @_;
    my $CETL = connectETL();
    
    unless ( defined($CETL) ) 
    {
        print "ERROR - Unable to connect to TD!\n";
        my $ERRSTR = $DBI::errstr;
        print "$ERRSTR\n";
        return -1;
    }
    
    
    my $SQLTEXT = "SELECT MAX(TO_CHAR(${FIELD_START_DATE},'YYYY-MM-DD')) FROM ${CDRTABLE}";
    my $CTH;

    $CTH = $CETL->prepare($SQLTEXT);
    unless ($CTH) 
    {
        print ("\n $SQLTEXT \n");
        return -1;
    }

    $CTH->execute();

    my @TROW;
    my $TX_STR = "";
    
    @TROW = $CTH->fetchrow();
    
    $TX_STR = "'$TROW[0]'";
    
    $CTH->finish();
    disconnectETL($CETL);
    
    return($TX_STR);
    
}

sub genETL_BEGIN_CTR_FILE
{
    my $ETL_TXDATE = pop(@_);
    my $CETL = connectETL();
    
    unless ( defined($CETL) ) 
    {
        print "ERROR - Unable to connect to TD!\n";
        my $ERRSTR = $DBI::errstr;
        print "$ERRSTR\n";
        return -1;
    }
    
    # my $SQLTEXT = "SELECT CAST(CAST(CAST('$ETL_TXDATE' AS DATE FORMAT 'YYYY-MM-DD') + 1 AS DATE FORMAT 'YYYYMMDD') AS CHAR(8))";
    my $SQLTEXT = "SELECT TO_CHAR((TO_DATE('$ETL_TXDATE','YYYY-MM-DD')+1),'YYYYMMDD') FROM DUAL";
    my $CTH;

    $CTH = $CETL->prepare($SQLTEXT);
    unless ($CTH) 
    {
        print ("\n SQL FAILED: $SQLTEXT \n");
        return -1;
    }

    $CTH->execute();

    my @TROW;
    my $TX_STR = "";
    
    @TROW = $CTH->fetchrow();
    
    my $GEN_FILE_TX_STR = "$TROW[0]";
    
    
    $CTH->finish();
    disconnectETL($CETL);
    
    $ETL_RECEIVE_DIR = "${ETL_RECEIVE_DIR}dir.m_etl_start${GEN_FILE_TX_STR}";
    open DF, ">" . $ETL_RECEIVE_DIR || die "Can not open " . $ETL_GEN_SCRIPT;
    close DF;
    
}

sub genCheckingReport
{
    my $CHECKSUM_DIR = "/opt/etl/prd/etl/tmp/EA_CHECK.log";
    
    open CHECKSUM, ">" . $CHECKSUM_DIR || die "Can not open " . $CHECKSUM_DIR;
    my $eaCmd = q(mailx -s "Checking Result" );
    
    $eaCmd = $eaCmd . q( " stephen_lee@smartone.com" <<EOFMAIL) . "\n";
    print CHECKSUM "$eaCmd";
    print CHECKSUM "ETL Automation Report - For SIT \n\n";
    print CHECKSUM "Today: $LOADTIME\n\n\n";
    
    print CHECKSUM "Checksum Result - $LOADTIME\n";
    print CHECKSUM "Rule_ID        Check_Table                   TD_Val              ORC_Val             Status    Remark      \n";
    print CHECKSUM "===================================================================================================================\n";
    
    
    my $DBH = connectETL();
    
    unless ( defined($DBH) ) 
    {
        print "ERROR - Unable to connect to TD!\n";
        my $ERRSTR = $DBI::errstr;
        print "$ERRSTR\n";
        return -1;
    }
    my $STH;
    
    my $SQL = "";
    $SQL = $SQL . "SELECT ";
    $SQL = $SQL . "CAST(Rule_Id AS CHAR(15)) ";
    $SQL = $SQL . ",CAST(Check_Table AS CHAR(30)) ";
    $SQL = $SQL . ",CAST(CAST(TD_Val AS DECIMAL(18,0) FORMAT 'z(17)9') AS CHAR(20)) ";
    $SQL = $SQL . ",CAST(CAST(ORC_Val AS DECIMAL(18,0) FORMAT 'z(17)9') AS CHAR(20)) ";
    $SQL = $SQL . ",CAST(Status AS CHAR(10)) ";
    $SQL = $SQL . ",CAST(Remark AS CHAR(100)) ";
    $SQL = $SQL . "FROM ${ENV}_ETL.VW_CHECKSUM_RESULT ";
    $SQL = $SQL . "ORDER BY 5,1 ";
        

    $STH = $DBH->prepare($SQL);
    unless ($STH) 
    {
        return -1;
    }

    $STH->execute();

    my @TABROW;

    while ( @TABROW = $STH->fetchrow() ) 
    {

        print CHECKSUM "$TABROW[0]";
        print CHECKSUM "$TABROW[1]";
        print CHECKSUM "$TABROW[2]";
        print CHECKSUM "$TABROW[3]";
        print CHECKSUM "$TABROW[4]";
        print CHECKSUM "$TABROW[5]";
        print CHECKSUM "\n"; 
    }
    $STH->finish();
    
    disconnectETL($DBH);
    
    print CHECKSUM "\n\n";

    print CHECKSUM "EOFMAIL";

    close (CHECKSUM);
    
    
    system("chmod +x $CHECKSUM_DIR");
    system("$CHECKSUM_DIR");
    
    
}


sub genEAReport
{

    my $REPORT_DIR = "/opt/etl/prd/etl/tmp/EA_REPORT.log";
    open EAREPORT, ">" . $REPORT_DIR || die "Can not open " . $REPORT_DIR;
 
 
    my $eaCmd = q(mailx -s "EA JOB Status" );
    $eaCmd = $eaCmd . q( " stephen_lee@smartone.com" <<EOFMAIL) . "\n";
 
    print EAREPORT "$eaCmd";
    
    print EAREPORT "ETL Automation Report - For SIT \n\n";
    print EAREPORT "Today: $LOADTIME\n\n\n";
    
    print EAREPORT "Failed Job\n";
    print EAREPORT "ETL_System     Etl_Job                       EA_Status      EA_TxDate      EA_Last_Process_Ts      Owner      \n";
    print EAREPORT "===================================================================================================================\n";
    
    
    my $DBH = connectETL();
    
    unless ( defined($DBH) ) 
    {
        print "ERROR - Unable to connect to TD!\n";
        my $ERRSTR = $DBI::errstr;
        print "$ERRSTR\n";
        return -1;
    }
    my $STH;
    
    my $SQL = "";
    # $SQL = $SQL . " SELECT ";
    # $SQL = $SQL . " CAST(ea.ETL_System AS CHAR(15)) "; 
    # $SQL = $SQL . " ,CAST(ea.Etl_Job AS CHAR(30)) ";
    # $SQL = $SQL . " ,CAST(ea.Last_Jobstatus AS CHAR(15)) AS EA_Status ";
    # $SQL = $SQL . " ,CAST(ea.Last_Txdate AS CHAR(15)) AS EA_TxDate ";
    # $SQL = $SQL . " ,CAST(ea.Last_starttime AS CHAR(25)) AS EA_Last_Process_Ts ";
    # $SQL = $SQL . " ,CAST(ea.Owner AS CHAR(30)) ";
    # $SQL = $SQL . " ,ea.Pull_Start_Time_List ";
    # $SQL = $SQL . " ,CAST(ea.Last_Process_Ts AS CHAR(25)) AS PreProcess_Last_Process_Ts ";
    # $SQL = $SQL . " ,ea.Last_Process_Stat AS PreProcess_Status ";
    # $SQL = $SQL . " ,ea.Filename_Mask ";
    # $SQL = $SQL . " FROM  ";
    # $SQL = $SQL . " ( ";
    # $SQL = $SQL . "  ";
    # $SQL = $SQL . " SELECT  ";
    # $SQL = $SQL . " CASE WHEN d.Etl_System IS NOT NULL THEN d.Etl_System  ";
    # $SQL = $SQL . " WHEN e.Dependency_System IS NOT NULL THEN e.Dependency_System ";
    # $SQL = $SQL . " ELSE a.Etl_System END as ETL_System ";
    # $SQL = $SQL . " ,a.Etl_Job, a.Last_Jobstatus, a.Last_Txdate, a.Last_starttime, COALESCE(b.Owner,f.Owner,'') as Owner,  c.Pull_Start_Time_List ";
    # $SQL = $SQL . " ,c.Last_Process_Ts, c.Last_Process_Stat, c.Filename_Mask ";
    # $SQL = $SQL . " FROM ${ENV}_ETL.ETL_JOB a ";
    # $SQL = $SQL . " LEFT OUTER JOIN ";
    # $SQL = $SQL . " ${ENV}_ETL.SIT_OWNER_LIST b ";
    # $SQL = $SQL . " ON ";
    # $SQL = $SQL . " a.Etl_Job = b.Table_Name ";
    # $SQL = $SQL . " LEFT OUTER JOIN ";
    # $SQL = $SQL . " ${ENV}_ETL.SIT_OWNER_LIST f ";
    # $SQL = $SQL . " ON ";
    # $SQL = $SQL . " a.Etl_Job = 'B_' || trim(f.Table_Name) ";
    # $SQL = $SQL . " LEFT OUTER JOIN ";
    # $SQL = $SQL . " ${ENV}_ETL.ETL_SRC_FILE c ";
    # $SQL = $SQL . " ON  ";
    # $SQL = $SQL . " a.Etl_Job = c.Job_Name ";
    # $SQL = $SQL . " LEFT OUTER JOIN ";
    # $SQL = $SQL . " ${ENV}_ETL.ETL_JOB_STREAM d ";
    # $SQL = $SQL . " ON ";
    # $SQL = $SQL . " a.Etl_Job = d.Stream_Job ";
    # $SQL = $SQL . " LEFT OUTER JOIN ";
    # $SQL = $SQL . " ${ENV}_ETL.ETL_JOB_DEPENDENCY e ";
    # $SQL = $SQL . " ON ";
    # $SQL = $SQL . " a.Etl_Job = e.ETL_Job ";
    # $SQL = $SQL . " WHERE  ";
    # $SQL = $SQL . " (a.last_jobstatus = 'Failed') ";
    # $SQL = $SQL . " AND a.Etl_System NOT IN ('ORM','MPC','ORF','SIT','SUB','USR','BUV','DWS') ";
    # $SQL = $SQL . " ) ea ";
    # $SQL = $SQL . " WHERE ";
    # $SQL = $SQL . " ETL_System <> 'ADW' ";
    # $SQL = $SQL . " AND ETL_Job NOT LIKE 'B_%' ";
    # $SQL = $SQL . " QUALIFY ROW_NUMBER() OVER ( ";
    # $SQL = $SQL . " PARTITION BY  ";
    # $SQL = $SQL . " ea.ETL_Job ";
    # $SQL = $SQL . " ORDER BY  ";
    # $SQL = $SQL . " ea.ETL_Job ";
    # $SQL = $SQL . " ) = 1 ";
    # $SQL = $SQL . " ORDER BY 4,3,1,2; ";

    $SQL = $SQL . " SELECT CAST(ea.ETL_System AS CHAR(15)) AS ETL_SYSTEM, ";
    $SQL = $SQL . " CAST(ea.Etl_Job AS         CHAR(30)) AS ETL_JOB , ";
    $SQL = $SQL . " CAST(ea.Last_Jobstatus AS  CHAR(15)) AS EA_Status , ";
    $SQL = $SQL . " CAST(ea.Last_Txdate AS     CHAR(15)) AS EA_TxDate , ";
    $SQL = $SQL . " CAST(ea.Last_starttime AS  CHAR(25)) AS EA_Last_Process_Ts , ";
    $SQL = $SQL . " CAST(ea.Owner AS           CHAR(30)) AS OWNER , ";
    $SQL = $SQL . " ea.Pull_Start_Time_List              AS PULL_START_TIME_LIST, ";
    $SQL = $SQL . " CAST(ea.Last_Process_Ts AS CHAR(25)) AS PreProcess_Last_Process_Ts , ";
    $SQL = $SQL . " ea.Last_Process_Stat                 AS PreProcess_Status , ";
    $SQL = $SQL . " ea.Filename_Mask                     AS FILENAME_MASK  ";
    $SQL = $SQL . " from( select ea.ETL_System  AS ETL_SYSTEM, ";
    $SQL = $SQL . " ea.Etl_Job  AS ETL_JOB , ";
    $SQL = $SQL . " ea.Last_Jobstatus  AS Last_Jobstatus , ";
    $SQL = $SQL . " ea.Last_Txdate  AS Last_Txdate , ";
    $SQL = $SQL . " ea.Last_starttime  AS Last_starttime , ";
    $SQL = $SQL . " ea.Owner AS   OWNER , ";
    $SQL = $SQL . " ea.Pull_Start_Time_List              AS PULL_START_TIME_LIST, ";
    $SQL = $SQL . " ea.Last_Process_Ts AS Last_Process_Ts , ";
    $SQL = $SQL . " ea.Last_Process_Stat                 AS Last_Process_Stat , ";
    $SQL = $SQL . " ea.Filename_Mask                     AS FILENAME_MASK,  ";
    $SQL = $SQL . " ROW_NUMBER() OVER(PARTITION BY ea.ETL_Job ORDER BY ea.ETL_Job) AS SWV_Qualify ";
    $SQL = $SQL . "  FROM(SELECT ";
    $SQL = $SQL . "    CASE  WHEN d.Etl_System IS NOT NULL THEN d.Etl_System ";
    $SQL = $SQL . "    WHEN e.Dependency_System IS NOT NULL THEN e.Dependency_System ";
    $SQL = $SQL . "    ELSE a.Etl_System END AS ETL_System , ";
    $SQL = $SQL . "  a.Etl_Job, ";
    $SQL = $SQL . "  a.Last_Jobstatus, ";
    $SQL = $SQL . "  a.Last_Txdate, ";
    $SQL = $SQL . "  a.Last_starttime, ";
    $SQL = $SQL . "  COALESCE(b.Owner,f.Owner,'') AS Owner, ";
    $SQL = $SQL . "  c.Pull_Start_Time_List , ";
    $SQL = $SQL . "  c.Last_Process_Ts, ";
    $SQL = $SQL . "  c.Last_Process_Stat, ";
    $SQL = $SQL . "  c.Filename_Mask ";
    $SQL = $SQL . "    FROM ${ENV}_ETL.ETL_JOB a ";
    $SQL = $SQL . "    LEFT OUTER JOIN ${ENV}_ETL.SIT_OWNER_LIST b  ON a.Etl_Job = b.Table_Name ";
    $SQL = $SQL . "    LEFT OUTER JOIN ${ENV}_ETL.SIT_OWNER_LIST f  ON a.Etl_Job = 'B_' || trim(f.Table_Name) ";
    $SQL = $SQL . "    LEFT OUTER JOIN ${ENV}_ETL.ETL_SRC_FILE c  ON a.Etl_Job = c.Job_Name ";
    $SQL = $SQL . "    LEFT OUTER JOIN ${ENV}_ETL.ETL_JOB_STREAM d  ON a.Etl_Job = d.Stream_Job ";
    $SQL = $SQL . "    LEFT OUTER JOIN ${ENV}_ETL.ETL_JOB_DEPENDENCY e   ON a.Etl_Job = e.ETL_Job ";
    $SQL = $SQL . "    WHERE (a.last_jobstatus = 'Failed') ";
    $SQL = $SQL . "    AND a.Etl_System NOT   IN ('ORM','MPC','ORF','SIT','SUB','USR','BUV','DWS')) ea ";
    $SQL = $SQL . "     WHERE ETL_System  <> 'ADW' ";
    $SQL = $SQL . "   AND ETL_Job NOT LIKE 'B_%') EA ";
    $SQL = $SQL . " where ea.SWV_Qualify = 1 ";
    $SQL = $SQL . " ORDER BY 4,3,1,2; ";
        

    $STH = $DBH->prepare($SQL);
    unless ($STH) 
    {
        return -1;
    }

    $STH->execute();

    my @TABROW;

    while ( @TABROW = $STH->fetchrow() ) 
    {

        print EAREPORT "$TABROW[0]";
        print EAREPORT "$TABROW[1]";
        print EAREPORT "$TABROW[2]";
        print EAREPORT "$TABROW[3]";
        print EAREPORT "$TABROW[4]";
        print EAREPORT "$TABROW[5]";
        print EAREPORT "\n"; 
    }
    $STH->finish();
    
    disconnectETL($DBH);


    print EAREPORT "\n\n";
    print EAREPORT "Not Started Job\n";
    print EAREPORT "ETL_System     Etl_Job                       EA_Status      EA_TxDate      EA_Last_Process_Ts      Owner      \n";
    print EAREPORT "===================================================================================================================\n";
    
    
    my $DBHNS = connectETL();
    
    unless ( defined($DBHNS) ) 
    {
        print "ERROR - Unable to connect to TD!\n";
        my $ERRSTR = $DBI::errstr;
        print "$ERRSTR\n";
        return -1;
    }
    my $STHNS;
    
    $SQL = "";
    # $SQL = $SQL . " SELECT ";
    # $SQL = $SQL . " CAST(ea.ETL_System AS CHAR(15)) "; 
    # $SQL = $SQL . " ,CAST(ea.Etl_Job AS CHAR(30)) ";
    # $SQL = $SQL . " ,CAST(ea.Last_Jobstatus AS CHAR(15)) AS EA_Status ";
    # $SQL = $SQL . " ,CAST(ea.Last_Txdate AS CHAR(15)) AS EA_TxDate ";
    # $SQL = $SQL . " ,CAST(ea.Last_starttime AS CHAR(25)) AS EA_Last_Process_Ts ";
    # $SQL = $SQL . " ,CAST(ea.Owner AS CHAR(30)) ";
    # $SQL = $SQL . " ,ea.Pull_Start_Time_List ";
    # $SQL = $SQL . " ,CAST(ea.Last_Process_Ts AS CHAR(25)) AS PreProcess_Last_Process_Ts ";
    # $SQL = $SQL . " ,ea.Last_Process_Stat AS PreProcess_Status ";
    # $SQL = $SQL . " ,ea.Filename_Mask ";
    # $SQL = $SQL . " FROM  ";
    # $SQL = $SQL . " ( ";
    # $SQL = $SQL . "  ";
    # $SQL = $SQL . " SELECT  ";
    # $SQL = $SQL . " CASE WHEN d.Etl_System IS NOT NULL THEN d.Etl_System  ";
    # $SQL = $SQL . " WHEN e.Dependency_System IS NOT NULL THEN e.Dependency_System ";
    # $SQL = $SQL . " ELSE a.Etl_System END as ETL_System ";
    # $SQL = $SQL . " ,a.Etl_Job, a.Last_Jobstatus, a.Last_Txdate, a.Last_starttime, COALESCE(b.Owner,f.Owner,'') as Owner,  c.Pull_Start_Time_List ";
    # $SQL = $SQL . " ,c.Last_Process_Ts, c.Last_Process_Stat, c.Filename_Mask ";
    # $SQL = $SQL . " FROM ${ENV}_ETL.ETL_JOB a ";
    # $SQL = $SQL . " LEFT OUTER JOIN ";
    # $SQL = $SQL . " ${ENV}_ETL.SIT_OWNER_LIST b ";
    # $SQL = $SQL . " ON ";
    # $SQL = $SQL . " a.Etl_Job = b.Table_Name ";
    # $SQL = $SQL . " LEFT OUTER JOIN ";
    # $SQL = $SQL . " ${ENV}_ETL.SIT_OWNER_LIST f ";
    # $SQL = $SQL . " ON ";
    # $SQL = $SQL . " a.Etl_Job = 'B_' || trim(f.Table_Name) ";
    # $SQL = $SQL . " LEFT OUTER JOIN ";
    # $SQL = $SQL . " ${ENV}_ETL.ETL_SRC_FILE c ";
    # $SQL = $SQL . " ON  ";
    # $SQL = $SQL . " a.Etl_Job = c.Job_Name ";
    # $SQL = $SQL . " LEFT OUTER JOIN ";
    # $SQL = $SQL . " ${ENV}_ETL.ETL_JOB_STREAM d ";
    # $SQL = $SQL . " ON ";
    # $SQL = $SQL . " a.Etl_Job = d.Stream_Job ";
    # $SQL = $SQL . " LEFT OUTER JOIN ";
    # $SQL = $SQL . " ${ENV}_ETL.ETL_JOB_DEPENDENCY e ";
    # $SQL = $SQL . " ON ";
    # $SQL = $SQL . " a.Etl_Job = e.ETL_Job ";
    # $SQL = $SQL . " WHERE  ";
    # $SQL = $SQL . " ((a.last_jobstatus = 'Ready' OR a.Last_jobstatus = 'Done' OR a.Last_jobstatus IS NULL) ";
    # $SQL = $SQL . " and a.last_txdate <= cast(current_date as date)) ";
    # $SQL = $SQL . " AND a.Etl_System NOT IN ('ORM','MPC','ORF','SIT','SUB','USR','BUV','DWS') ";
    # $SQL = $SQL . " ) ea ";
    # $SQL = $SQL . " WHERE ";
    # $SQL = $SQL . " ETL_System <> 'ADW' ";
    # $SQL = $SQL . " AND ETL_Job NOT LIKE 'B_%' ";
    # $SQL = $SQL . " QUALIFY ROW_NUMBER() OVER ( ";
    # $SQL = $SQL . " PARTITION BY  ";
    # $SQL = $SQL . " ea.ETL_Job ";
    # $SQL = $SQL . " ORDER BY  ";
    # $SQL = $SQL . " ea.ETL_Job ";
    # $SQL = $SQL . " ) = 1 ";
    # $SQL = $SQL . " ORDER BY 4,3,1,2; ";
 
    $SQL = $SQL . "SELECT CAST(ea.ETL_System AS CHAR(15)) as etl_system ";
    $SQL = $SQL . ",CAST(ea.Etl_Job AS CHAR(30))  as etl_job ";
    $SQL = $SQL . ",CAST(ea.Last_Jobstatus AS CHAR(15)) AS EA_Status  ";
    $SQL = $SQL . ",CAST(to_char(ea.Last_Txdate,'YYYY-MM-DD') AS CHAR(15)) AS EA_TxDate  ";
    $SQL = $SQL . ",CAST(ea.Last_starttime AS CHAR(25)) AS EA_Last_Process_Ts  ";
    $SQL = $SQL . ",CAST(ea.Owner AS CHAR(30)) AS OWNER  ";
    $SQL = $SQL . ",ea.Pull_Start_Time_List   AS PULL_START_TIME_LIST ";
    $SQL = $SQL . ",CAST(ea.Last_Process_Ts AS CHAR(25)) AS PreProcess_Last_Process_Ts  ";
    $SQL = $SQL . ",ea.Last_Process_Stat AS PreProcess_Status  ";
    $SQL = $SQL . ",ea.Filename_Mask from ";
    $SQL = $SQL . "( select ea.ETL_System   ";
    $SQL = $SQL . ", ea.Etl_Job   ";
    $SQL = $SQL . ", ea.Last_Jobstatus   ";
    $SQL = $SQL . ", ea.Last_Txdate   ";
    $SQL = $SQL . ", ea.Last_starttime  ";
    $SQL = $SQL . ", ea.Owner   ";
    $SQL = $SQL . ", ea.Pull_Start_Time_List "; 
    $SQL = $SQL . ", ea.Last_Process_Ts   ";
    $SQL = $SQL . ", ea.Last_Process_Stat ";  
    $SQL = $SQL . ", ea.Filename_Mask ";
    $SQL = $SQL . ", ROW_NUMBER() OVER(PARTITION BY ea.ETL_Job ORDER BY ea.ETL_Job) SWV_Qualify ";
    $SQL = $SQL . "   FROM(SELECT CASE  ";
    $SQL = $SQL . "        WHEN d.Etl_System IS NOT NULL THEN d.Etl_System   ";
    $SQL = $SQL . "        WHEN e.Dependency_System IS NOT NULL THEN e.Dependency_System  ";
    $SQL = $SQL . "        ELSE a.Etl_System END as ETL_System  ";
    $SQL = $SQL . "        , a.Etl_Job ";
    $SQL = $SQL . "        , a.Last_Jobstatus ";
    $SQL = $SQL . "        , a.Last_Txdate ";
    $SQL = $SQL . "        , a.Last_starttime ";
    $SQL = $SQL . "        , COALESCE(b.Owner,f.Owner,'') as Owner ";
    $SQL = $SQL . "        , c.Pull_Start_Time_List  ";
    $SQL = $SQL . "        , c.Last_Process_Ts ";
    $SQL = $SQL . "        , c.Last_Process_Stat ";
    $SQL = $SQL . "        , c.Filename_Mask  ";
    $SQL = $SQL . "        FROM ${ENV}_ETL.ETL_JOB a  ";
    $SQL = $SQL . "        LEFT OUTER JOIN ${ENV}_ETL.SIT_OWNER_LIST b ON a.Etl_Job = b.Table_Name  ";
    $SQL = $SQL . "        LEFT OUTER JOIN ${ENV}_ETL.SIT_OWNER_LIST f ON a.Etl_Job = 'B_' || trim(f.Table_Name)  ";
    $SQL = $SQL . "        LEFT OUTER JOIN ${ENV}_ETL.ETL_SRC_FILE c   ON a.Etl_Job = c.Job_Name  ";
    $SQL = $SQL . "        LEFT OUTER JOIN ${ENV}_ETL.ETL_JOB_STREAM d ON a.Etl_Job = d.Stream_Job  ";
    $SQL = $SQL . "        LEFT OUTER JOIN ${ENV}_ETL.ETL_JOB_DEPENDENCY e ON a.Etl_Job = e.ETL_Job  ";
    $SQL = $SQL . "        WHERE ((a.last_jobstatus = 'Ready' OR a.Last_jobstatus = 'Done' OR a.Last_jobstatus IS NULL)  ";
    $SQL = $SQL . "        and a.last_txdate <= trunc(SYSDATE) )  ";
    $SQL = $SQL . "        AND a.Etl_System NOT IN ('ORM','MPC','ORF','SIT','SUB','USR','BUV','DWS')) ea  ";
    $SQL = $SQL . "        WHERE  ETL_System <> 'ADW'  ";
    $SQL = $SQL . "        AND ETL_Job NOT LIKE 'B_%') ea ";
    $SQL = $SQL . "   where SWV_Qualify = 1 ";
    $SQL = $SQL . " ORDER BY 4,3,1,2; ";     

    $STHNS = $DBHNS->prepare($SQL);
    unless ($STHNS) 
    {
        return -1;
    }

    $STHNS->execute();

    my @TABROW;

    while ( @TABROW = $STHNS->fetchrow() ) 
    {

        print EAREPORT "$TABROW[0]";
        print EAREPORT "$TABROW[1]";
        print EAREPORT "$TABROW[2]";
        print EAREPORT "$TABROW[3]";
        print EAREPORT "$TABROW[4]";
        print EAREPORT "$TABROW[5]";
        print EAREPORT "\n"; 
    }
    $STHNS->finish();
    
    disconnectETL($DBHNS);


    print EAREPORT "\n\n";


    print EAREPORT "Pending Job\n";
    print EAREPORT "ETL_System     Etl_Job                       EA_Status      EA_TxDate      EA_Last_Process_Ts      Owner      \n";
    print EAREPORT "===================================================================================================================\n";
    
    
    my $DBHPJ = connectETL();
    
    unless ( defined($DBHPJ) ) 
    {
        print "ERROR - Unable to connect to TD!\n";
        my $ERRSTR = $DBI::errstr;
        print "$ERRSTR\n";
        return -1;
    }
    my $STHPJ;
    
    $SQL = "";
    # Teradata - original query 3 
    # $SQL = $SQL . " SELECT ";
    # $SQL = $SQL . " CAST(ea.ETL_System AS CHAR(15)) "; 
    # $SQL = $SQL . " ,CAST(ea.Etl_Job AS CHAR(30)) ";
    # $SQL = $SQL . " ,CAST(ea.Last_Jobstatus AS CHAR(15)) AS EA_Status ";
    # $SQL = $SQL . " ,CAST(ea.Last_Txdate AS CHAR(15)) AS EA_TxDate ";
    # $SQL = $SQL . " ,CAST(ea.Last_starttime AS CHAR(25)) AS EA_Last_Process_Ts ";
    # $SQL = $SQL . " ,CAST(ea.Owner AS CHAR(30)) ";
    # $SQL = $SQL . " ,ea.Pull_Start_Time_List ";
    # $SQL = $SQL . " ,CAST(ea.Last_Process_Ts AS CHAR(25)) AS PreProcess_Last_Process_Ts ";
    # $SQL = $SQL . " ,ea.Last_Process_Stat AS PreProcess_Status ";
    # $SQL = $SQL . " ,ea.Filename_Mask ";
    # $SQL = $SQL . " FROM  ";
    # $SQL = $SQL . " ( ";
    # $SQL = $SQL . "  ";
    # $SQL = $SQL . " SELECT  ";
    # $SQL = $SQL . " CASE WHEN d.Etl_System IS NOT NULL THEN d.Etl_System  ";
    # $SQL = $SQL . " WHEN e.Dependency_System IS NOT NULL THEN e.Dependency_System ";
    # $SQL = $SQL . " ELSE a.Etl_System END as ETL_System ";
    # $SQL = $SQL . " ,a.Etl_Job, a.Last_Jobstatus, a.Last_Txdate, a.Last_starttime, COALESCE(b.Owner,f.Owner,'') as Owner,  c.Pull_Start_Time_List ";
    # $SQL = $SQL . " ,c.Last_Process_Ts, c.Last_Process_Stat, c.Filename_Mask ";
    # $SQL = $SQL . " FROM ${ENV}_ETL.ETL_JOB a ";
    # $SQL = $SQL . " LEFT OUTER JOIN ";
    # $SQL = $SQL . " ${ENV}_ETL.SIT_OWNER_LIST b ";
    # $SQL = $SQL . " ON ";
    # $SQL = $SQL . " a.Etl_Job = b.Table_Name ";
    # $SQL = $SQL . " LEFT OUTER JOIN ";
    # $SQL = $SQL . " ${ENV}_ETL.SIT_OWNER_LIST f ";
    # $SQL = $SQL . " ON ";
    # $SQL = $SQL . " a.Etl_Job = 'B_' || trim(f.Table_Name) ";
    # $SQL = $SQL . " LEFT OUTER JOIN ";
    # $SQL = $SQL . " ${ENV}_ETL.ETL_SRC_FILE c ";
    # $SQL = $SQL . " ON  ";
    # $SQL = $SQL . " a.Etl_Job = c.Job_Name ";
    # $SQL = $SQL . " LEFT OUTER JOIN ";
    # $SQL = $SQL . " ${ENV}_ETL.ETL_JOB_STREAM d ";
    # $SQL = $SQL . " ON ";
    # $SQL = $SQL . " a.Etl_Job = d.Stream_Job ";
    # $SQL = $SQL . " LEFT OUTER JOIN ";
    # $SQL = $SQL . " ${ENV}_ETL.ETL_JOB_DEPENDENCY e ";
    # $SQL = $SQL . " ON ";
    # $SQL = $SQL . " a.Etl_Job = e.ETL_Job ";
    # $SQL = $SQL . " WHERE  ";
    # $SQL = $SQL . " (a.last_jobstatus = 'Pending' ";
    # $SQL = $SQL . " and a.last_txdate <= cast(current_date as date)) ";
    # $SQL = $SQL . " AND a.Etl_System NOT IN ('ORM','MPC','ORF','SIT','SUB','USR','BUV','DWS') ";
    # $SQL = $SQL . " ) ea ";
    # $SQL = $SQL . " WHERE ";
    # $SQL = $SQL . " ETL_System <> 'ADW' ";
    # $SQL = $SQL . " AND ETL_Job NOT LIKE 'B_%' ";
    # $SQL = $SQL . " QUALIFY ROW_NUMBER() OVER ( ";
    # $SQL = $SQL . " PARTITION BY  ";
    # $SQL = $SQL . " ea.ETL_Job ";
    # $SQL = $SQL . " ORDER BY  ";
    # $SQL = $SQL . " ea.ETL_Job ";
    # $SQL = $SQL . " ) = 1 ";
    # $SQL = $SQL . " ORDER BY 4,3,1,2; ";
    $SQL = $SQL . "SELECT CAST(ea.ETL_System AS CHAR(15)) as etl_system "; 
    $SQL = $SQL . ",CAST(ea.Etl_Job AS CHAR(30))   as etl_job ";
    $SQL = $SQL . ",CAST(ea.Last_Jobstatus AS CHAR(15)) AS EA_Status  ";
    $SQL = $SQL . ",CAST(to_char(ea.Last_Txdate, 'YYYY-MM-DD') AS CHAR(15)) AS EA_TxDate  ";
    $SQL = $SQL . ",CAST(ea.Last_starttime AS CHAR(25)) AS EA_Last_Process_Ts  ";
    $SQL = $SQL . ",CAST(ea.Owner AS CHAR(30))   as owner ";
    $SQL = $SQL . ",ea.Pull_Start_Time_List   as pull_start_time_list ";
    $SQL = $SQL . ",CAST(ea.Last_Process_Ts AS CHAR(25)) AS PreProcess_Last_Process_Ts  ";
    $SQL = $SQL . ",ea.Last_Process_Stat AS PreProcess_Status  ";
    $SQL = $SQL . ",ea.Filename_Mask as filename_mask ";
    $SQL = $SQL . "from( ";
    $SQL = $SQL . "select ea.ETL_System  ";
    $SQL = $SQL . ", ea.Etl_Job   ";
    $SQL = $SQL . ", ea.Last_Jobstatus  "; 
    $SQL = $SQL . ", ea.Last_Txdate   ";
    $SQL = $SQL . ", ea.Last_starttime   ";
    $SQL = $SQL . ", ea.Owner   ";
    $SQL = $SQL . ", ea.Pull_Start_Time_List  ";
    $SQL = $SQL . ", ea.Last_Process_Ts   ";
    $SQL = $SQL . ", ea.Last_Process_Stat   ";
    $SQL = $SQL . ", ea.Filename_Mask, ROW_NUMBER() OVER(PARTITION BY ea.ETL_Job ORDER BY ea.ETL_Job) SWV_Qualify ";
    $SQL = $SQL . "   FROM(SELECT   ";
    $SQL = $SQL . "        CASE WHEN d.Etl_System IS NOT NULL THEN d.Etl_System   ";
    $SQL = $SQL . "            WHEN e.Dependency_System IS NOT NULL THEN e.Dependency_System  ";
    $SQL = $SQL . "            ELSE a.Etl_System END as ETL_System  ";
    $SQL = $SQL . "        ,a.Etl_Job ";
    $SQL = $SQL . "        , a.Last_Jobstatus ";
    $SQL = $SQL . "        , a.Last_Txdate ";
    $SQL = $SQL . "        , a.Last_starttime ";
    $SQL = $SQL . "        , COALESCE(b.Owner,f.Owner,'') as Owner ";
    $SQL = $SQL . "        ,  c.Pull_Start_Time_list ";
    $SQL = $SQL . "        , c.Last_Process_Ts ";
    $SQL = $SQL . "        , c.Last_Process_Stat ";
    $SQL = $SQL . "        , c.Filename_Mask  ";
    $SQL = $SQL . "      FROM ${ENV}_ETL.ETL_JOB a  ";
    $SQL = $SQL . "      LEFT OUTER JOIN ${ENV}_ETL.SIT_OWNER_LIST b ON a.Etl_Job = b.Table_Name  ";
    $SQL = $SQL . "      LEFT OUTER JOIN ${ENV}_ETL.SIT_OWNER_LIST f ON a.Etl_Job = 'B_' || trim(f.Table_Name)  ";
    $SQL = $SQL . "      LEFT OUTER JOIN ${ENV}_ETL.ETL_SRC_FILE c   ON a.Etl_Job = c.Job_Name  ";
    $SQL = $SQL . "      LEFT OUTER JOIN ${ENV}_ETL.ETL_JOB_STREAM d ON a.Etl_Job = d.Stream_Job  ";
    $SQL = $SQL . "      LEFT OUTER JOIN ${ENV}_ETL.ETL_JOB_DEPENDENCY e ON  a.Etl_Job = e.ETL_Job  ";
    $SQL = $SQL . "      WHERE  (a.last_jobstatus = 'Pending'  ";
    $SQL = $SQL . "      and a.last_txdate <= trunc(SYSDATE) ";
    $SQL = $SQL . "      )  ";
    $SQL = $SQL . "      AND a.Etl_System NOT IN ('ORM','MPC','ORF','SIT','SUB','USR','BUV','DWS')) ea  ";
    $SQL = $SQL . "      WHERE  ETL_System <> 'ADW'  ";
    $SQL = $SQL . "   AND ETL_Job NOT LIKE 'B_%') ea ";
    $SQL = $SQL . "where ea.SWV_Qualify = 1 ";
    $SQL = $SQL . " ORDER BY 4,3,1,2; ";   

    $STHPJ = $DBHPJ->prepare($SQL);
    unless ($STHPJ) 
    {
        return -1;
    }

    $STHPJ->execute();

    my @TABROW;

    while ( @TABROW = $STHPJ->fetchrow() ) 
    {

        print EAREPORT "$TABROW[0]";
        print EAREPORT "$TABROW[1]";
        print EAREPORT "$TABROW[2]";
        print EAREPORT "$TABROW[3]";
        print EAREPORT "$TABROW[4]";
        print EAREPORT "$TABROW[5]";
        print EAREPORT "\n"; 
    }
    $STHPJ->finish();
    
    disconnectETL($DBHPJ);

    print EAREPORT "\n\n";

    print EAREPORT "Finished Job\n";
    print EAREPORT "ETL_System     Etl_Job                       EA_Status      EA_TxDate      EA_Last_Process_Ts      Owner      \n";
    print EAREPORT "===================================================================================================================\n";
    
    
    my $DBHFJ = connectETL();
    
    unless ( defined($DBHFJ) ) 
    {
        print "ERROR - Unable to connect to TD!\n";
        my $ERRSTR = $DBI::errstr;
        print "$ERRSTR\n";
        return -1;
    }
    my $STHFJ;

    # Teradata - original query 4    
    $SQL = "";
    # $SQL = $SQL . " SELECT ";
    # $SQL = $SQL . " CAST(ea.ETL_System AS CHAR(15)) "; 
    # $SQL = $SQL . " ,CAST(ea.Etl_Job AS CHAR(30)) ";
    # $SQL = $SQL . " ,CAST(ea.Last_Jobstatus AS CHAR(15)) AS EA_Status ";
    # $SQL = $SQL . " ,CAST(ea.Last_Txdate  AS CHAR(15)) AS EA_TxDate ";
    # $SQL = $SQL . " ,CAST(ea.Last_starttime AS CHAR(25)) AS EA_Last_Process_Ts ";
    # $SQL = $SQL . " ,CAST(ea.Owner AS CHAR(30)) ";
    # $SQL = $SQL . " ,ea.Pull_Start_Time_List ";
    # $SQL = $SQL . " ,CAST(ea.Last_Process_Ts AS CHAR(25)) AS PreProcess_Last_Process_Ts ";
    # $SQL = $SQL . " ,ea.Last_Process_Stat AS PreProcess_Status ";
    # $SQL = $SQL . " ,ea.Filename_Mask ";
    # $SQL = $SQL . " FROM  ";
    # $SQL = $SQL . " ( ";
    # $SQL = $SQL . "  ";
    # $SQL = $SQL . " SELECT  ";
    # $SQL = $SQL . " CASE WHEN d.Etl_System IS NOT NULL THEN d.Etl_System  ";
    # $SQL = $SQL . " WHEN e.Dependency_System IS NOT NULL THEN e.Dependency_System ";
    # $SQL = $SQL . " ELSE a.Etl_System END as ETL_System ";
    # $SQL = $SQL . " ,a.Etl_Job, a.Last_Jobstatus, a.Last_Txdate, a.Last_starttime, COALESCE(b.Owner,f.Owner,'') as Owner,  c.Pull_Start_Time_List ";
    # $SQL = $SQL . " ,c.Last_Process_Ts, c.Last_Process_Stat, c.Filename_Mask ";
    # $SQL = $SQL . " FROM ${ENV}_ETL.ETL_JOB a ";
    # $SQL = $SQL . " LEFT OUTER JOIN ";
    # $SQL = $SQL . " ${ENV}_ETL.SIT_OWNER_LIST b ";
    # $SQL = $SQL . " ON ";
    # $SQL = $SQL . " a.Etl_Job = b.Table_Name ";
    # $SQL = $SQL . " LEFT OUTER JOIN ";
    # $SQL = $SQL . " ${ENV}_ETL.SIT_OWNER_LIST f ";
    # $SQL = $SQL . " ON ";
    # $SQL = $SQL . " a.Etl_Job = 'B_' || trim(f.Table_Name) ";
    # $SQL = $SQL . " LEFT OUTER JOIN ";
    # $SQL = $SQL . " ${ENV}_ETL.ETL_SRC_FILE c ";
    # $SQL = $SQL . " ON  ";
    # $SQL = $SQL . " a.Etl_Job = c.Job_Name ";
    # $SQL = $SQL . " LEFT OUTER JOIN ";
    # $SQL = $SQL . " ${ENV}_ETL.ETL_JOB_STREAM d ";
    # $SQL = $SQL . " ON ";
    # $SQL = $SQL . " a.Etl_Job = d.Stream_Job ";
    # $SQL = $SQL . " LEFT OUTER JOIN ";
    # $SQL = $SQL . " ${ENV}_ETL.ETL_JOB_DEPENDENCY e ";
    # $SQL = $SQL . " ON ";
    # $SQL = $SQL . " a.Etl_Job = e.ETL_Job ";
    # $SQL = $SQL . " WHERE  ";
    # $SQL = $SQL . " (a.last_jobstatus = 'Done' ";
    # $SQL = $SQL . " and a.last_txdate = cast(current_date as date)) ";
    # $SQL = $SQL . " AND a.Etl_System NOT IN ('ORM','MPC','ORF','SIT','SUB','USR','BUV','DWS') ";
    # $SQL = $SQL . " ) ea ";
    # $SQL = $SQL . " WHERE ";
    # $SQL = $SQL . " ETL_System <> 'ADW' ";
    # $SQL = $SQL . " AND ETL_Job NOT LIKE 'B_%' ";
    # $SQL = $SQL . " QUALIFY ROW_NUMBER() OVER ( ";
    # $SQL = $SQL . " PARTITION BY  ";
    # $SQL = $SQL . " ea.ETL_Job ";
    # $SQL = $SQL . " ORDER BY  ";
    # $SQL = $SQL . " ea.ETL_Job ";
    # $SQL = $SQL . " ) = 1 ";
    # $SQL = $SQL . " ORDER BY 4,3,1,2; ";
    $SQL = $SQL . "SELECT CAST(ea.ETL_System AS CHAR(15)) , ";
    $SQL = $SQL . " CAST(ea.Etl_Job AS         CHAR(30)) , ";
    $SQL = $SQL . " CAST(ea.Last_Jobstatus AS  CHAR(15)) AS EA_Status , ";
    $SQL = $SQL . " CAST(TO_CHAR(ea.Last_Txdate,'YYYY-MM-DD')  AS  CHAR(15)) AS EA_TxDate , ";
    $SQL = $SQL . " CAST(ea.Last_starttime AS  CHAR(25)) AS EA_Last_Process_Ts , ";
    $SQL = $SQL . " CAST(ea.Owner AS           CHAR(30)) , ";
    $SQL = $SQL . " ea.Pull_Start_Time_List , ";
    $SQL = $SQL . " CAST(ea.Last_Process_Ts AS CHAR(25)) AS PreProcess_Last_Process_Ts , ";
    $SQL = $SQL . " ea.Last_Process_Stat                 AS PreProcess_Status , ";
    $SQL = $SQL . " ea.Filename_Mask   as filename_mask ";
    $SQL = $SQL . " FROM ";
    $SQL = $SQL . " (SELECT  ea.ETL_System AS ETL_System , ";
    $SQL = $SQL . " ea.Etl_Job           as Etl_Job, ";
    $SQL = $SQL . " ea.Last_Jobstatus    AS Last_Jobstatus , ";
    $SQL = $SQL . " ea.Last_Txdate       AS Last_Txdate , ";
    $SQL = $SQL . " ea.Last_starttime    AS Last_starttime , ";
    $SQL = $SQL . " ea.Owner             AS owner , ";
    $SQL = $SQL . " ea.Pull_Start_Time_List as pull_start_time_list , ";
    $SQL = $SQL . " ea.Last_Process_Ts      AS Last_Process_Ts , ";
    $SQL = $SQL . " ea.Last_Process_Stat    AS last_Process_Stat , ";
    $SQL = $SQL . " ea.Filename_Mask        as filename_mask , ";
    $SQL = $SQL . " ROW_NUMBER() OVER(PARTITION BY ea.ETL_Job ORDER BY ea.ETL_Job) SWV_Qualify ";
    $SQL = $SQL . " FROM ";
    $SQL = $SQL . " (SELECT ";
    $SQL = $SQL . "  CASE ";
    $SQL = $SQL . "     WHEN d.Etl_System IS NOT NULL THEN d.Etl_System ";
    $SQL = $SQL . "     WHEN e.Dependency_System IS NOT NULL THEN e.Dependency_System ";
    $SQL = $SQL . "     ELSE a.Etl_System ";
    $SQL = $SQL . "   END AS ETL_System , ";
    $SQL = $SQL . "   a.Etl_Job, ";
    $SQL = $SQL . "  a.Last_Jobstatus, ";
    $SQL = $SQL . "  a.Last_Txdate, ";
    $SQL = $SQL . "  a.Last_starttime, ";
    $SQL = $SQL . "  COALESCE(b.Owner,f.Owner,'') AS Owner, ";
    $SQL = $SQL . "  c.Pull_Start_Time_List , ";
    $SQL = $SQL . "  c.Last_Process_Ts, ";
    $SQL = $SQL . "  c.Last_Process_Stat, ";
    $SQL = $SQL . "   c.Filename_Mask ";
    $SQL = $SQL . " FROM ${ENV}_ETL.ETL_JOB a ";
    $SQL = $SQL . " LEFT OUTER JOIN ${ENV}_ETL.SIT_OWNER_LIST b ON a.Etl_Job = b.Table_Name ";
    $SQL = $SQL . " LEFT OUTER JOIN ${ENV}_ETL.SIT_OWNER_LIST f ON a.Etl_Job = 'B_' || trim(f.Table_Name) ";
    $SQL = $SQL . " LEFT OUTER JOIN ${ENV}_ETL.ETL_SRC_FILE c  ON a.Etl_Job = c.Job_Name ";
    $SQL = $SQL . " LEFT OUTER JOIN ${ENV}_ETL.ETL_JOB_STREAM d ON a.Etl_Job = d.Stream_Job ";
    $SQL = $SQL . " LEFT OUTER JOIN ${ENV}_ETL.ETL_JOB_DEPENDENCY e ON a.Etl_Job = e.ETL_Job ";
    $SQL = $SQL . " WHERE (a.last_jobstatus = 'Done' ";
    $SQL = $SQL . " AND a.last_txdate       = TRUNC(SYSDATE)  ";
    $SQL = $SQL . "  AND a.Etl_System NOT   IN ('ORM','MPC','ORF','SIT','SUB','USR','BUV','DWS') ";
    $SQL = $SQL . " ) ea ";
    $SQL = $SQL . "  WHERE ETL_System <> 'ADW' ";
    $SQL = $SQL . " AND ETL_Job NOT LIKE 'B_%' ";
    $SQL = $SQL . " ) ea ";
    $SQL = $SQL . "WHERE ea.SWV_Qualify = 1  ";

    $STHFJ = $DBHFJ->prepare($SQL);
    unless ($STHFJ) 
    {
        return -1;
    }

    $STHFJ->execute();

    my @TABROW;

    while ( @TABROW = $STHFJ->fetchrow() ) 
    {

        print EAREPORT "$TABROW[0]";
        print EAREPORT "$TABROW[1]";
        print EAREPORT "$TABROW[2]";
        print EAREPORT "$TABROW[3]";
        print EAREPORT "$TABROW[4]";
        print EAREPORT "$TABROW[5]";
        print EAREPORT "\n"; 
    }
    $STHFJ->finish();
    
    disconnectETL($DBHFJ);

    print EAREPORT "\n\n";

    print EAREPORT "EOFMAIL";

    close (EAREPORT);
    
    system("chmod +x $REPORT_DIR");
    system("$REPORT_DIR");
    
}


#This function is used to update the image date in the ETL_TABLE_DATE table
sub updateBizTXDate
{
    my ($UTABLENAME) = pop(@_);
    
    if (length($UTABLENAME) < 2) { 
        $UTABLENAME = $ETLJOBNAME;
    }    

    my $rc = open(SQLPLUS, "| sqlplus /\@${TDDSN}");
    unless ($rc){
        print "Cound not invoke sqlplus command\n";
        return -1;
    }

    print SQLPLUS<<ENDOFINPUT;

    
Update ${ETLDB}.ETL_TABLE_DATE
SET As_Of_Ts = TO_TIMESTAMP('$TXDATE 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
,   Refresh_Ts = TO_TIMESTAMP('$LOADTIME', 'YYYY-MM-DD HH24:MI:SS')
WHERE
    ${ETLDB}.ETL_TABLE_DATE.Job_Name = '$UTABLENAME';
commit;
exit;
 

ENDOFINPUT

    close(SQLPLUS);
    my $RET_CODE = $? >> 8;
    if ($RET_CODE != 0){
        return 1;
    }else{
        return 0;
    }
}


#This function is used to update the As_Of_Ts(for trx month data) in the ETL_TABLE_DATE table
sub updateDataAsOfTs
{
    my ($UTABLENAME) = pop(@_);
    
    if (length($UTABLENAME) < 2) { 
        $UTABLENAME = $ETLJOBNAME;
    }    

    my $rc = open(SQLPLUS, "| sqlplus /\@${TDDSN}");
    unless ($rc){
        print "Cound not invoke sqlplus command\n";
        return -1;
    }

    print SQLPLUS<<ENDOFINPUT;

Update ${ETLDB}.ETL_TABLE_DATE
SET As_Of_Ts = TO_TIMESTAMP(TO_CHAR((TO_DATE('$TXMONTH' || '01' ,'YYYYMMDD')-1),'YYYY-MM-DD') || ' 23:59:59','YYYY-MM-DD HH24:MI:SS')
WHERE ${ETLDB}.ETL_TABLE_DATE.Job_Name = '$UTABLENAME';

commit;
exit;
 
ENDOFINPUT

    close(SQLPLUS);
    my $RET_CODE = $? >> 8;
    if ($RET_CODE != 0){
        return 1;
    }else{
        return 0;
    }
}

sub genLast2Month
{
    my $CUR_TXDATE = pop(@_);
    my $CETL = connectETL();
    
    unless ( defined($CETL) ) 
    {
        print "ERROR - Unable to connect to TD!\n";
        my $ERRSTR = $DBI::errstr;
        print "$ERRSTR\n";
        return -1;
    }
    # my $SQLTEXT = "SELECT SUBSTR(CAST(CAST(ADD_MONTHS(CAST('$CUR_TXDATE' AS DATE FORMAT 'YYYY-MM-DD'),-1)  AS DATE FORMAT 'YYYY-MM-DD') AS CHAR(8)),1,7) || '-01'";
    my $SQLTEXT = "SELECT TO_CHAR(ADD_MONTHS(TO_DATE('$CUR_TXDATE','YYYY-MM-DD'),-1),'YYYY-MM') || '-01' FROM DUAL";
    my $CTH;

    $CTH = $CETL->prepare($SQLTEXT);
    unless ($CTH) 
    {
        print ("\n SQL FAILED: $SQLTEXT \n");
        return -1;
    }

    $CTH->execute();

    my @TROW;
    my $TX_STR = "";
    
    @TROW = $CTH->fetchrow();
    
    $SUMM_TXDATE = "$TROW[0]";
    
    $CTH->finish();
    disconnectETL($CETL);
    return(0);
}


sub genLastMonth
{
    my ($CUR_TXDATE) = @_;
    
    $CUR_TXDATE = $SUMMLOADDATE;
    
    
    my $CETL = connectETL();
    
    unless ( defined($CETL) ) 
    {
        print "ERROR - Unable to connect to TD!\n";
        my $ERRSTR = $DBI::errstr;
        print "$ERRSTR\n";
        return -1;
    }
    
    # my $SQLTEXT = "SELECT SUBSTR(CAST(CAST(ADD_MONTHS(CAST('$CUR_TXDATE' AS DATE FORMAT 'YYYY-MM-DD'),-1)  AS DATE FORMAT 'YYYYMMDD') AS CHAR(8)),1,6)";
    my $SQLTEXT = "SELECT TO_CHAR(ADD_MONTHS( TO_DATE('${CUR_TXDATE}','YYYY-MM-DD'),-1),'YYYYMM') FROM DUAL";
    my $CTH;

    $CTH = $CETL->prepare($SQLTEXT);
    unless ($CTH) 
    {
        print ("\n SQL FAILED: $SQLTEXT \n");
        return -1;
    }

    $CTH->execute();

    my @TROW;
    my $TX_STR = "";
    
    @TROW = $CTH->fetchrow();
    
    my $GEN_FILE_TX_STR = "$TROW[0]";
    
    $CTH->finish();
    
    
    $SQLTEXT = "SELECT COALESCE('${GEN_FILE_TX_STR}','') FROM ${ADWDB}.SUMM_SWITCH WHERE Job_Name = '${ETLJOBNAME}' AND Summ_Month = '$GEN_FILE_TX_STR'";
    
    $CTH = $CETL->prepare($SQLTEXT);
    unless ($CTH) 
    {
        print ("\n SQL FAILED: $SQLTEXT \n");
        return -1;
    }

    $CTH->execute();
        
    @TROW = $CTH->fetchrow();
    
    my $GEN_FILE_TX_STR_2 = "$TROW[0]";
    
    $CTH->finish();
    
    if ($GEN_FILE_TX_STR_2 eq ""){

       my $rrc = open(SQLPLUS, "| sqlplus /\@${TDDSN}");
       unless ($rrc){
           print "Cound not invoke sqlplus command\n";
           return -1;
       }

    print SQLPLUS<<ENDOFINPUT;
    MERGE INTO ${ADWDB}.SUMM_SWITCH SS
    USING DUAL ON (SS.JOB_NAME='${ETLJOBNAME}')
    WHEN NOT MATCHED THEN INSERT 
    ( JOB_NAME
    , SUMM_MONTH
    , CREATE_TS
    , REFRESH_TS) 
    VALUES 
    ('${ETLJOBNAME}'
    ,'${GEN_FILE_TX_STR}'
    ,TO_TIMESTAMP('${LOADTIME}','YYYY-MM-DD HH24:MI:SS')
    ,TO_TIMESTAMP('${LOADTIME}','YYYY-MM-DD HH24:MI:SS')
    )
    WHEN MATCHED THEN UPDATE SET 
    Summ_Month = '${GEN_FILE_TX_STR}'
    , Refresh_Ts = TO_TIMESTAMP('${LOADTIME}','YYYY-MM-DD HH24:MI:SS')
    ;

ENDOFINPUT

    close(SQLPLUS);
       
    }else{
        $GEN_FILE_TX_STR = "";
    }  
    
    disconnectETL($CETL);

    # print("hw: $GEN_FILE_TX_STR");

    return($GEN_FILE_TX_STR);
}


sub gen_rated_cdr_summ_cmlmlm2
{
    my $CUR_TXDATE = pop(@_);
    my $CETL = connectETL();
    
    unless ( defined($CETL) ) 
    {
        print "ERROR - Unable to connect to TD!\n";
        my $ERRSTR = $DBI::errstr;
        print "$ERRSTR\n";
        return -1;
    }
    # my $SQLTEXT = "SELECT SUBSTR(CAST(CAST(ADD_MONTHS(CAST('$CUR_TXDATE' AS DATE FORMAT 'YYYY-MM-DD'),-2)  AS DATE FORMAT 'YYYY-MM-DD') AS CHAR(8)),1,7) || '-01'";
    my $SQLTEXT = "SELECT TO_CHAR(ADD_MONTHS( TO_DATE('${CUR_TXDATE}','YYYY-MM-DD'),-2),'YYYY-MM') || '-01' FROM DUAL";
    my $CTH;

    $CTH = $CETL->prepare($SQLTEXT);
    unless ($CTH) 
    {
        print ("\n SQL FAILED: $SQLTEXT \n");
        return -1;
    }

    $CTH->execute();

    my @TROW;
    my $TX_STR = "";
    
    @TROW = $CTH->fetchrow();
    
    $TX_STR = "Call_Start_Date >= '$TROW[0]'";
    
    $CTH->finish();
    disconnectETL($CETL);
    return($TX_STR);
}




sub gen_rated_cdr_summ_lm3lm4lm5
{
    my $CUR_TXDATE = pop(@_);
    my $CETL = connectETL();
    
    unless ( defined($CETL) ) 
    {
        print "ERROR - Unable to connect to TD!\n";
        my $ERRSTR = $DBI::errstr;
        print "$ERRSTR\n";
        return -1;
    }
    my $SQLTEXT = "SELECT 'CALL_START_DATE >= ''' || " .
                  " TO_CHAR(ADD_MONTHS(TO_DATE('${CUR_TXDATE}','YYYY-MM-DD'),-5),'YYYY-MM') || '-01''' || " .
                  " ' AND CALL_START_DATE < ''' || " .
                  " TO_CHAR(ADD_MONTHS(TO_DATE('${CUR_TXDATE}','YYYY-MM-DD'),-2),'YYYY-MM') || '-01'''  " .
                  " FROM DUAL ";
    my $CTH;

    $CTH = $CETL->prepare($SQLTEXT);
    unless ($CTH) 
    {
        print ("\n SQL FAILED: $SQLTEXT \n");
        return -1;
    }

    $CTH->execute();

    my @TROW;
    my $TX_STR = "";
    
    @TROW = $CTH->fetchrow();
    
    $TX_STR = "$TROW[0]";
    
    $CTH->finish();
    disconnectETL($CETL);
    return($TX_STR);
}




sub gen_rated_cdr_summ_lm6lm7lm8
{
    my $CUR_TXDATE = pop(@_);
    my $CETL = connectETL();
    
    unless ( defined($CETL) ) 
    {
        print "ERROR - Unable to connect to TD!\n";
        my $ERRSTR = $DBI::errstr;
        print "$ERRSTR\n";
        return -1;
    }
    my $SQLTEXT = "SELECT 'CALL_START_DATE >= ''' || " .
                  " TO_CHAR(ADD_MONTHS(TO_DATE('${CUR_TXDATE}','YYYY-MM-DD'),-8),'YYYY-MM') || '-01''' || " .
                  " ' AND CALL_START_DATE < ''' || " .
                  " TO_CHAR(ADD_MONTHS(TO_DATE('${CUR_TXDATE}','YYYY-MM-DD'),-5),'YYYY-MM') || '-01'''  " .
                  " FROM DUAL ";
    
    my $CTH;

    $CTH = $CETL->prepare($SQLTEXT);
    unless ($CTH) 
    {
        print ("\n SQL FAILED: $SQLTEXT \n");
        return -1;
    }

    $CTH->execute();

    my @TROW;
    my $TX_STR = "";
    
    @TROW = $CTH->fetchrow();
    
    $TX_STR = "$TROW[0]";
    
    $CTH->finish();
    disconnectETL($CETL);
    return($TX_STR);
}




sub gen_rated_cdr_summ_lm9lm12
{
    my $CUR_TXDATE = pop(@_);
    my $CETL = connectETL();
    
    unless ( defined($CETL) ) 
    {
        print "ERROR - Unable to connect to TD!\n";
        my $ERRSTR = $DBI::errstr;
        print "$ERRSTR\n";
        return -1;
    }
    my $SQLTEXT = "SELECT 'CALL_START_DATE < ''' || TO_CHAR(ADD_MONTHS( TO_DATE('${CUR_TXDATE}','YYYY-MM-DD'),-8),'YYYY-MM')  || '-01''' from dual";
    my $CTH;

    $CTH = $CETL->prepare($SQLTEXT);
    unless ($CTH) 
    {
        print ("\n SQL FAILED: $SQLTEXT \n");
        return -1;
    }

    $CTH->execute();

    my @TROW;
    my $TX_STR = "";
    
    @TROW = $CTH->fetchrow();
    
    $TX_STR = "$TROW[0]";
    
    $CTH->finish();
    disconnectETL($CETL);
    return($TX_STR);
}




sub genNextStart
{
    my ($ETL_TXDATE ,$StopDate, $JobName) = @_;

    if ( $ETL_TXDATE ge $StopDate )
    {
        return;
    }
        
    my $CETL = connectETL();
    
    unless ( defined($CETL) ) 
    {
        print "ERROR - Unable to connect to TD!\n";
        my $ERRSTR = $DBI::errstr;
        print "$ERRSTR\n";
        return -1;
    }
    
    # my $SQLTEXT = "SELECT CAST(CAST(CAST('$ETL_TXDATE' AS DATE FORMAT 'YYYY-MM-DD') + 1 AS DATE FORMAT 'YYYYMMDD') AS CHAR(8))";
    my $SQLTEXT = "SELECT TO_CHAR((TO_DATE('$ETL_TXDATE', 'YYYY-MM-DD') + 1),'YYYYMMDD') FROM DUAL";

    my $CTH;

    $CTH = $CETL->prepare($SQLTEXT);
    unless ($CTH) 
    {
        print ("\n SQL FAILED: $SQLTEXT \n");
        return -1;
    }

    $CTH->execute();

    my @TROW;
    my $TX_STR = "";
    
    @TROW = $CTH->fetchrow();
    
    my $GEN_FILE_TX_STR = "$TROW[0]";
    
    
    $CTH->finish();
    disconnectETL($CETL);
    
    
    $ETL_RECEIVE_DIR = "${ETL_RECEIVE_DIR}dir.${JobName}${GEN_FILE_TX_STR}";
    
    open DF, ">" . $ETL_RECEIVE_DIR || die "Can not open " . $ETL_GEN_SCRIPT;
    
    close DF;
    
    
    
}

sub getEmailList
{
        my $ADW_PARAM = "adw.par";
        my ($LIST_NAME ,$JOB_NAME) = @_;

        open (DS, "${ETL_ETC_DIR}/${ADW_PARAM}") || die "Can not open " . $RBD_PARAM;
        @EMAIL_ALERT_LIST = <DS>;
    close(DS);

        my $LIST = "";

        @EMAIL_ALERT_LIST = grep(!/^#/, @EMAIL_ALERT_LIST);

        @JOB_EMAIL_LIST = grep(/^${JOB_NAME}:/, @EMAIL_ALERT_LIST); 

        foreach $LINE (@JOB_EMAIL_LIST) {
                my @EMAIL_LIST = split(':',$LINE);

                if ( $EMAIL_LIST[1] eq $LIST_NAME ){
            $LIST = "${EMAIL_LIST[2]} ${LIST}";
        }

        }

        if ($LIST eq ""){
        print("Fail in getting email list\n");
                exit(1);
        }

        return $LIST;

}

sub sendRbdAlert
{
        my $INPUT = pop(@_);
        my @INPUT_LIST = split(' ', $INPUT);
        my $LIST_NUMBER = $INPUT_LIST[0];
        my @ERROR_LIST = split(',', $INPUT_LIST[1]);
        my $SRC_DATE = $INPUT_LIST[2];
        my $JOB_NAME = $INPUT_LIST[3];

        my $RBD_OUTPUT_DIR = "${ETL_OUTPUT_DIR}/RBD/${JOB_NAME}/output";

        my $RBD_PARAM = "rbd.par";
        open (DS, "${ETL_ETC_DIR}/${RBD_PARAM}") || die "Can not open " . $RBD_PARAM;
        @EMAIL_ALERT_LIST = <DS>;
        close(DS);

        my $RBDTOLIST = "";
        my $RBDCCLIST = "";
        my $RBDBCCLIST = "";

        foreach $LINE (@EMAIL_ALERT_LIST) {
            my @EMAIL_LIST = split(':',$LINE);
            
            if ($EMAIL_LIST[0] eq "TO${LIST_NUMBER}"){
                $RBDTOLIST = $EMAIL_LIST[1];
            }
            elsif ($EMAIL_LIST[0] eq "CC${LIST_NUMBER}"){
                $RBDCCLIST = $EMAIL_LIST[1];
            }
            elsif ($EMAIL_LIST[0] eq "BCC${LIST_NUMBER}"){
                $RBDBCCLIST = $EMAIL_LIST[1];
            }        
        }

        if (($RBDTOLIST eq "") || ($RBDCCLIST eq "") || ($RBDBCCLIST eq "")){
                print("Fail in getting email list\n");
                return 1;
        }

        my $ATTACH_LIST = "";

        foreach $ERROR_FILE (@ERROR_LIST) {
                my $ZIP = "${RBD_OUTPUT_DIR}/${ERROR_FILE}.err.gz";

                if ( -s ("${RBD_OUTPUT_DIR}/${ERROR_FILE}.err")) {

                ##  No comand "ux2dos" in bill02 side so comment it first
                ##      system("ux2dos ${RBD_OUTPUT_DIR}/${ERROR_FILE}.err > ${RBD_OUTPUT_DIR}/${DOS}");
                ##      if ( $? != 0 ) {
                ##              print("Fail in ux2dos\n");
                ##              return 1;
                ##      }

                ##      system("gzip -f ${RBD_OUTPUT_DIR}/${DOS}");
                        system("cp ${RBD_OUTPUT_DIR}/${ERROR_FILE}.err ${RBD_OUTPUT_DIR}/${ERROR_FILE}.bkup");
                        system("gzip -f ${RBD_OUTPUT_DIR}/${ERROR_FILE}.err");
                        system("mv ${RBD_OUTPUT_DIR}/${ERROR_FILE}.bkup ${RBD_OUTPUT_DIR}/${ERROR_FILE}.err");

                        $ATTACH_LIST = "${ATTACH_LIST} -a $ZIP";
                }
                else{
                        if ( -f ("${RBD_OUTPUT_DIR}/${ERROR_FILE}.err")) {
                                system("rm -f ${RBD_OUTPUT_DIR}/${ERROR_FILE}.err");
                        }
                }
        }

        if ( $ATTACH_LIST eq "" ) {
                print("No Error!\n");
                return 0;
        }

    my $rc = open(EMAIL_EOF, "| /usr/local/bin/mutt -s 'RBD - Data Error for ${SRC_DATE}' ${RBDCCLIST} ${RBDBCCLIST} ${RBDTOLIST} ${ATTACH_LIST}");
    unless ($rc){
        print "Cound not invoke FTP command\n";
        return -1;
    }

    print EMAIL_EOF<<ENDOFINPUT;
Hi,
Please check the error files for details. Thx.

ENDOFINPUT

    close(EMAIL_EOF);
    my $RET_CODE = $? >> 8;
    if ($RET_CODE != 0){
            print("Fail in sending email\n");
        return 1;
    }

        foreach $ERROR_FILE (@ERROR_LIST) {
                if ( -f ("${RBD_OUTPUT_DIR}/${ERROR_FILE}.err.gz") ) {
                        system("rm -f ${RBD_OUTPUT_DIR}/${ERROR_FILE}.err.gz");
                        if ( $? != 0 ) {
                                print("Fail in romove zip files\n");
                                return 1;
                        }
                }
        }

        print("Alert Sent.\n");

        return 1;
}


sub runFastloadWithoutFile
{
    my ($TDDBN, $MSTABLENAME) = @_;
    my $rc;
    my @DATA_FILE_LIST;
    my $REPFILENAME = $ETLJOBNAME;
    my $DSJOBNAME = $ETLJOBNAME;
    my @ROW_CNT;
    $TABLEDB = $TDDBN;
    
    
    $ERRTXDATE = "";
    
    ##open (DS, "${ETL_PROCESS_DIR_CTL}${CONTROL_FILE}") || die "Can not open " . $CONTROL_FILE;
    ##@DATA_FILE_LIST = <DS>;
    ##close(DS);

    ##foreach $DATAFILE (@DATA_FILE_LIST) {
    
        my ($esec,$emin,$ehour,$emday,$emon,$eyear,$ewday,$eyday,$eisdst) = localtime(time());
        $eyear += 1900;
        $emon = sprintf("%02d", $emon+1);
        $emday = sprintf("%02d", $emday);
        $ehour = sprintf("%02d",$ehour);
        $emin = sprintf("%02d",$emin);
        $ERRTXDATE = "${emon}${emday}${ehour}${emin}";
    
    
    
        system("rm ${ETL_TMP_DIR}/${REPFILENAME}.log $ulog");
        ##my @DFNAME = split(/ /,$DATAFILE);
    
        $rc = system("${DS_JOB_PATH}dsjob -run -mode RESET $DSPROJECT $DSJOBNAME $ulog");      
        sleep(10);
                # Oracle - change the parameter with oracle wallet settings   
                # $rc = system("${DS_JOB_PATH}dsjob -run -mode NORMAL -param TDDB=$TABLEDB -param TDSVR=$TDSVR -param TDUSR=$TDUSR -param TDPWD=$TDPWD -param OUTPUT_FILE_PATH=${ETL_TMP_DIR_DS} -param ERRTXDATE=$ERRTXDATE -param ENV=$ENV -wait -jobstatus $DSPROJECT $DSJOBNAME $ulog");
                $rc = system("${DS_JOB_PATH}dsjob -run -mode NORMAL -param OUTPUT_FILE_PATH=${ETL_TMP_DIR_DS} -param ENV=$ENV -wait -jobstatus $DSPROJECT $DSJOBNAME $ulog");


        if ($rc == 256) 
        {
            print("\n\n\n#####################################\n");
            print("#Teradata Fastload/Multiload Job log\n");
            print("#DataFile: $DFNAME[0]\n");
            print("#####################################\n\n");
            ##`echo "\$ getdslog" | ftp bill02`;
            my $rfile = $REPFILENAME;
            system("mv ${ETL_TMP_DIR}/${rfile}*.txt ${ETL_TMP_DIR}/${rfile}.log $ulog");
            system("more ${ETL_TMP_DIR}/${rfile}.log $ulog");
            print("\n\n");
            parseLogFile("${ETL_TMP_DIR}/${rfile}.log");
        }
        elsif ($rc == 512) 
        {
            
            print("\n\n\n#####################################\n");
            print("#DataStage complete with warnings\n\n");
            print("#Teradata Fastload/Multiload Job log\n");
            print("#DataFile: $DFNAME[0]\n");
            print("#####################################\n\n");
            ##`echo "\$ getdslog" | ftp bill02`;
            my $rfile = $REPFILENAME;
            system("mv ${ETL_TMP_DIR}/${rfile}*.txt ${ETL_TMP_DIR}/${rfile}.log $ulog");
            system("more ${ETL_TMP_DIR}/${rfile}.log $ulog");
            print("\n\n");
            parseLogFile("${ETL_TMP_DIR}/${rfile}.log");
        }
        else
        {
            ##print("Fail: ${DS_JOB_PATH}dsjob -run -mode NORMAL -param TDDB=$TABLEDB -param REPFILENAME=$REPFILENAME -param SRCFILENAME=${ETL_PROCESS_DIR}$DFNAME[0] -param TDSVR=$TDSVR -param TDUSR=$TDUSR -param TDPWD=$TDPWD -param OUTPUT_FILE_PATH=${ETL_TMP_DIR} -param ERRTXDATE=$ERRTXDATE -param ENV=$ENV -wait -jobstatus $DSPROJECT $DSJOBNAME $ulog");
            print("Fail: $DSPROJECT $DSJOBNAME (return code: $rc)\n");
            return (1);   
        }
    ##}
    
    
    
    if ($TOTAL_DUP != 0)
    {   
        system("${ETL_ETC_DIR}/check_dup.ksh $ETLJOBNAME $CONTROL_FILE $ETL_PROCESS_DIR $ETL_DUP_DIR");
        print("${ETL_ETC_DIR}/check_dup.ksh $ETLJOBNAME $CONTROL_FILE $ETL_PROCESS_DIR $ETL_DUP_DIR\n");
        print("There are duplicated rows in the data file\n");
        return(1);   
    }    
    
    
    
    
    
    
    
    $rc = updateValidationLog();
    
    if ($rc != 0)
    {
        print ("Update Validation Log fail!\n");
        removeValidationLog();
        return(1);    
    }
    
    $rc = checkVariance();
    
    if ($rc != 0)
    {
        removeValidationLog();
        return(1);    
    }
   
    print ("\n HW calling setTXDate .... \n");
 
    $rc = setTXDate($MSTABLENAME, $TABLEDB);
    
    if ($rc != 0)
    {
        print ("\n\n\nUpdate transaction date fail!\n");
        removeValidationLog();
        return(1);    
    }
   

    return 0;
}

sub updateEtlJobLimit{

        my ($JOB_COUNT) = @_;
        my $NEW_JOB_LIMIT;

        open (DS, "${ETL_JOB_LIMIT}") || die "Can not open " . $ETL_JOB_LIMIT;
    @OLD_JOB_LIMIT_DS = <DS>;
    close(DS);
    
    foreach $LINE (@OLD_JOB_LIMIT_DS) {
            my $OLD_JOB_LIMIT = $LINE;
            
            if ( defined($OLD_JOB_LIMIT) ) {
            
                        $NEW_JOB_LIMIT = $OLD_JOB_LIMIT + $JOB_COUNT;

                        system ("echo $NEW_JOB_LIMIT > ${ETL_JOB_LIMIT}");

                        print("${ETL_JOB_LIMIT} updated!\n");    

                }
        }

        return 0;

}


sub runTruncateTbl{
  my ($SCHEMA_NAME,$TBL_NAME) = @_;

  print("Calling TRUNCATE_TBL ${SCHEMA_NAME}.${TBL_NAME}");

    my $rct = open(SQLPLUS, "| sqlplus /\@${TDDSN}");
    unless ($rct){
        print "Cound not invoke sqlplus command\n";
    }

    print SQLPLUS<<ENDOFINPUT;
      EXECUTE ${UTLDB}.ETL_UTILITY.truncate_tbl(P_TABLE_SCHEMA => '${SCHEMA_NAME}',p_table_name=>'${TBL_NAME}');
      EXIT;

ENDOFINPUT

    close(SQLPLUS);
    my $RET_CODE = $? >> 8;
    if ($RET_CODE != 0){
        return 1;
    }


  return 0;
}


sub runGatherStats{
  my ($SCHEMA_NAME,$TBL_NAME) = @_;

  print("Calling Gather Stats ${SCHEMA_NAME}.${TBL_NAME}");

    my $rct = open(SQLPLUS, "| sqlplus /\@${TDDSN}");
    unless ($rct){
        print "Cound not invoke sqlplus command\n";
    }

    print SQLPLUS<<ENDOFINPUT;
      set timing on;
      set heading off;
      set lines 150;
      SELECT 'Calling EXECUTE ${UTLDB}.ETL_UTILITY.GATHER_TABLE_STATS(P_SCHEMA=>''${SCHEMA_NAME}'',p_table=>''${TBL_NAME}'');   ' from dual;
      EXECUTE ${UTLDB}.ETL_UTILITY.GATHER_TABLE_STATS(P_SCHEMA => '${SCHEMA_NAME}',p_table=>'${TBL_NAME}');
      EXIT;

ENDOFINPUT

    close(SQLPLUS);
    my $RET_CODE = $? >> 8;
    if ($RET_CODE != 0){
        return 1;
    }


  return 0;
}


 
