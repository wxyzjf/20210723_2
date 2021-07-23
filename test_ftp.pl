#!/usr/bin/perl
my $ETLVAR = $ENV{"AUTO_ETLVAR"};require $ETLVAR;
require $ETLVAR;

my $MASTER_TABLE = ""; #Please input the final target ADW table name here
my $ENV;

my $OUTPUT_FILE_PATH, $DEST_DIR,$OUTPUT_FILE_NAME_1;

my $FTP_FROM_HOST,$FTP_FROM_USERNAME,$FTP_FROM_PASSWORD;
my $FTP_TO_HOST,$FTP_TO_PORT,$FTP_TO_USERNAME,$FTP_TO_PASSWORD,$FTP_TO_DEST_PATH;
my $PROCESS_DATE;
my $comChanType;
my $head_value;
my $sql_head_value;



sub initParam{
    $ENV = $ENV{"ETL_ENV"};
    use POSIX;
    use Date::Manip;
    use DBI;
    my $PROCESS_DATE = &UnixDate("${etlvar::TXDATE}", "%Y%m%d");
    my $FILE_DATE= &UnixDate(DateCalc("${etlvar::F_D_MONTH[0]}","-1 months",\$err),'%Y%m%d');
    my $year_month_day=strftime("%Y%m%d",localtime());

$OUTPUT_FILE_PATH = "/opt/etl/prd/etl/APP/RPT/U_NORTON_RPT/bin";
$OUTPUT_FILE_NAME = "billing_summary.csv";
$FTP_TO_DEST_PATH = "/Norton";

$FTP_TO_HOST = "sftp://10.16.9.166 ";
$FTP_TO_PORT = "22";
$FTP_TO_USERNAME = "dwftp2";
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

sub EmailUser{
    my $TOLIST, $CCLIST, $BCCLIST, $SUBJECT, $ATTACH;
        $TOLIST = "Eloise_Wu".q(@)."smartone.com";
        $ATTACH = "-a ${OUTPUT_FILE_PATH}/${OUTPUT_FILE_NAME}";
    $SUBJECT = "raw data report on ${etlvar::TXDATE}";
    my $rc = open(EMAIL_EOF, "| /usr/local/bin/mutt -s '${SUBJECT}' ${TOLIST} ${ATTACH}");
    unless ($rc){
            print "Cound not invoke mutt command\n";
            return -1;
    }
    print EMAIL_EOF<<ENDOFINPUT;
Please check the report file raw_data_report.csv which is generated on ${etlvar::TXDATE}.
ENDOFINPUT
    close(EMAIL_EOF);
    my $RET_CODE = $? >> 8;
    if ($RET_CODE != 0){
        return 1;
    }
}


if ($#ARGV < 0){
    print("Syntax : perl <Script Name> <System Name>_<Job Name>_<TXDATE>.dir>\n");
    print("Example: perl b_cust_info0010.pl adw_b_cust_info_20051010.dir\n");
    exit(1);
}

open(STDERR, ">&STDOUT");
my $pre = etlvar::preProcess($ARGV[0]);
my $rc = etlvar::getTXDate($MASTER_TABLE);
etlvar::genFirstDayOfMonth($etlvar::TXDATE);


$| = 1;
initParam();
my $ret = 0;
if ($ret == 0)
{
     $ret = sendFile();
}
my $post = etlvar::postProcess();
exit($ret);
  