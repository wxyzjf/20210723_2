######################################################
#   $Header: /CVSROOT/smartone/Code/ETL/APP/ADW/B_HS_STK_ALLOCATE/bin/b_hs_stk_allocate0010.pl,v 1.1 2005/12/14 01:03:59 MichaelNg Exp $
#   Purpose:
#
#
######################################################

use DBI;
my $ETLVAR = $ENV{"AUTO_ETLVAR"};require $ETLVAR;
#my $ETLVAR = "/opt/etl/prd/etl/APP/ADW/Y_SHKDP_RPT/bin/master_dev.pl";
#require $ETLVAR;

my $MASTER_TABLE = "";#Please input the final target ADW table name here

my $ENV;

my $OUTPUT_FILE_PATH, $DEST_DIR,
   $OUTPUT_FILE_NAME_1;$OUTPUT_FILE_NAME_2;

my $FTP_FROM_HOST,$FTP_FROM_USERNAME,$FTP_FROM_PASSWORD;
my $FTP_TO_HOST,$FTP_TO_PORT,$FTP_TO_USERNAME,$FTP_TO_PASSWORD,$FTP_TO_DEST_PATH;

my $PROCESS_DATE;
my $PROCESS_DATE_LAST_YYYYMM;
my $CURR_MONTH_START;
my $CURR_MONTH_END;
my $LAST_MONTH_START;
my $LAST_MONTH_END;
my $NEXT_MONTH_START;

#########################################################################################################
#########################################################################################################
#########################################################################################################

sub initParam{

    $ENV = $ENV{"ETL_ENV"};

    use Date::Manip;

    my $PROCESS_DATE = &UnixDate("${etlvar::TXDATE}", "%Y%m%d");


    #my $PROCESS_DATE_YYYYMM = &UnixDate("${etlvar::F_D_MONTH[0]}", "%Y%m");

    # ------------------------------------------------------------------#
    #  Please define the parameters for this job below.                 #
    # ------------------------------------------------------------------#

    $OUTPUT_FILE_PATH = ${etlvar::ETL_OUTPUT_DIR}."/".${etlvar::ETLSYS}."/".${etlvar::ETLJOBNAME};
    #$OUTPUT_FILE_PATH = "/opt/etl/prd/etl/APP/ADW/Y_SHKDP_RPT/bin/el_test";
    
        if (! -d $OUTPUT_FILE_PATH) {
            system("mkdir ${OUTPUT_FILE_PATH}");
        }

    $OUTPUT_FILE_PREFIX = "SHKDP_RPT_".$PROCESS_DATE;
    $OUTPUT_COMPLETE  = "SHKDP_RPT_".$PROCESS_DATE.".complete";


    if ($ENV eq "DEV")
    {
        ##  DEVELOPMENT  ##
        $TDUSR = "${etlvar::TDUSR}";
        $TDPWD = "${etlvar::TDPWD}";
        $TDDSN = $ENV{"AUTO_DSN"};

        # PUT action
        $FTP_TO_HOST = "";                                         # Please define
        $FTP_TO_PORT = "";                                         # Please define
        $FTP_TO_USERNAME = "";                                     # Please define
        $FTP_TO_PASSWORD = "";                                     # Please define
        $FTP_TO_DEST_PATH = "";                                    # Please define

        # GET action   (ONLY  FOR  DEVELOPMENT)
        $FTP_FROM_HOST = "${etlvar::DSSVR}";
        $FTP_FROM_USERNAME = "${etlvar::DSUSR}";
        $FTP_FROM_PASSWORD = "${etlvar::DSPWD}";
    }
    else
    {
        ##  PRODUCTION  ##
        $TDUSR = "${etlvar::TDUSR}";
        $TDPWD = "${etlvar::TDPWD}";
        $TDDSN = $ENV{"AUTO_DSN"};

        # PUT action
        #$FTP_TO_HOST = "";                                         # Please define
        #$FTP_TO_PORT = "";                                         # Please define
        #$FTP_TO_USERNAME = "";                                     # Please define
        #$FTP_TO_PASSWORD = "";                                     # Please define
        #$FTP_TO_DEST_PATH = "";                                    # Please define

        $FTP_TO_HOST = "sftp://10.16.9.166 ";
        $FTP_TO_PORT = "22";
        $FTP_TO_USERNAME = "dwftp2";
        $FTP_TO_DEST_PATH = "/shkdp";
        
    }
}

#########################################################################################################
#########################################################################################################
#########################################################################################################

sub runSQLPLUS_EXPORT{

  my $SQLCMD_FILE="${etlvar::AUTO_GEN_TEMP_PATH}u_shkdp_rpt0020_sqlcmd.sql";
  #my $SQLCMD_FILE="/opt/etl/prd/etl/APP/ADW/Y_SHKDP_RPT/bin/el_test/z_shkdp_rpt0060_sqlcmd.sql";
  open SQLCMD, ">" . $SQLCMD_FILE || die "Cannot open file" ;

  print SQLCMD<<ENDOFINPUT;

        ${etlvar::LOGON_TD}
        ${etlvar::SET_MAXERR}
        ${etlvar::SET_ERRLVL_1}
        ${etlvar::SET_ERRLVL_2}
--Please type your SQL statement here

---------------------------------------------------------------------------------------------------
	exit;
ENDOFINPUT
  close(SQLCMD);
  print("sqlplus /\@${etlvar::TDDSN} \@$SQLCMD_FILE");
  my $ret = system("sqlplus /\@${etlvar::TDDSN} \@$SQLCMD_FILE");
  if ($ret != 0)
  {
    return (1);
  }

}

sub exportReport{
	##---- one parameter for file ###---
	my($p_batch_id,$p_expfile) = @_;
	#$p_batch_id ="CE2103";
	#$p_expfile = "./test.csv";
	my $v_sep=",";

	open ( FH, '>',$p_expfile) or die "Unable open target file ".$p_expfile;

	$dbh = DBI->connect("dbi:Oracle:${TDDSN}", "", "", { AutoCommit => 1 } ) ;
	$dbh->{ChopBlanks} = 1;
    	my $v_line="";
    	my $v_row="0";

	print("\n\n\n#####################################\n");
    	print("#  exporting report 1 with batch_id ".$p_batch_id." to file ".$p_expfile." \n");
    	print("#####################################\n\n");


    	$sql = "    select row_num,col_num,disp_val,batch_id
            	    from MIG_ADW.Y_SHKDP_RPT_005A01_T
		    where batch_id= ?
        	    order by to_number(row_num),to_number(col_num)";
    	$datastmt  = $dbh->prepare($sql) or die "Can't prepare SQL statement $DBI::errstr\n";
    	$datastmt->bind_param(1,$p_batch_id);
    	$datastmt->execute;
    	while (@fld = $datastmt->fetchrow_array){
		if (($v_row != "0") && ($v_row != $fld[0]) ){
			print FH $v_line."\n";
			$v_line="";
		}	
		$v_line = $v_line.$fld[2].$v_sep;
		$v_row = $fld[0];        
    	}
	##--- print last line ##
    	print FH $v_line."\n";
    	#$datastmt->finish;

	print("\n\n\n#####################################\n");
    	print("#  exporting report 2 with batch_id ".$p_batch_id." to file ".$p_expfile." \n");
    	print("#####################################\n\n");
	

    	print FH "\n\n\n\n\n";
    	my $v_line="";
    	my $v_row="0";

	$sql = "    select row_num,col_num,disp_val,batch_id
                    from MIG_ADW.Y_SHKDP_RPT_005C03_T
                    where batch_id= ?
                    order by to_number(row_num),to_number(col_num)";
        $datastmt  = $dbh->prepare($sql) or die "Can't prepare SQL statement $DBI::errstr\n";
        $datastmt->bind_param(1,$p_batch_id);
        $datastmt->execute;
        while (@fld = $datastmt->fetchrow_array){
                if (($v_row != "0") && ($v_row != $fld[0]) ){
                        print FH $v_line."\n";
                        $v_line="";
                }
                $v_line = $v_line.$fld[2].$v_sep;
                $v_row = $fld[0];
        }
        ##--- print last line ##
        print FH $v_line."\n";
        $datastmt->finish;

    	$dbh->disconnect;
    	close FH;
    	return 0;

}

sub genReport{
        print("\n\n\n#####################################\n");
        print("#  Generating report with batch_id list \n");
        print("#####################################\n\n");
	$dbh = DBI->connect("dbi:Oracle:${TDDSN}", "", "", { AutoCommit => 1 } ) ;
        $dbh->{ChopBlanks} = 1;
	$sql = "    select distinct batch_id
                    from MIG_ADW.SHKDP_RPT_H
                    order by batch_id";
        $datastmt  = $dbh->prepare($sql) or die "Can't prepare SQL statement $DBI::errstr\n";        
        $datastmt->execute;
	my @ary_batchid;
	while (@fld = $datastmt->fetchrow_array){
		push (@ary_batchid,$fld[0]);
        }
	$datastmt->finish;
	$dbh->disconnect;

	foreach (@ary_batchid ){
		$v_batch_id = $_;
		$v_outputfile =  $OUTPUT_FILE_PATH."\/".$OUTPUT_FILE_PREFIX."_".$v_batch_id.".csv";
		exportReport($v_batch_id,$v_outputfile);
	}
	return 0;
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
        mput $OUTPUT_FILE_PREFIX_*.csv
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

#########################################################################################################
#########################################################################################################
#########################################################################################################

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
etlvar::genFirstDayOfMonth($etlvar::TXDATE);

## Disable the Perl buffer, Print the log message immediately
$| = 1;

initParam();

my $ret = 0;

##################################################################################

# RUN SQL and EXPORT FILE
if ($ret == 0)
{
    $ret = genReport();
}

##################################################################################

## REMOVE FILE HEADER
if ($ret == 0)
{
    system("touch ${OUTPUT_FILE_PATH}/${OUTPUT_COMPLETE}");
}

if ($ret == 0)
{
    $ret = sendFile();
}



my $post = etlvar::postProcess();

exit($ret);



















