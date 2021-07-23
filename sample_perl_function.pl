sub checkRBDTraffic
{

 

print("\n\n\n#####################################\n");
print("# check RBD traffic\n");
print("#####################################\n\n");

 

$dbh = DBI->connect("dbi:Oracle:${TDDSN}", "", "", { AutoCommit => 1 } ) ;

 

$dbh->{ChopBlanks} = 1;

 


$sql = "
select
lower(a.Store_Cd), c.Email_Address
from
${etlvar::ADWDB}.RBD_HITRATE_STORE a
left outer join
(
select
Store_Cd, SUM(In_Cnt) Traffic_Cnt
from
${etlvar::TMPDB}.D_RBD_TRAFFIC
group by
Store_Cd
) b
on
a.Store_Cd = b.Store_Cd
left outer join
(SELECT STO_CD, Email_Address
from(select STO_CD, Email_Address, Row_Number() Over(PARTITION BY STO_CD ORDER BY TRX_MONTH Desc) SWV_Qualify FROM ${etlvar::ADWDB}.RBD_MGR)
where SWV_Qualify = 1
) c
on
a.Store_Cd = c.STO_CD
where
(b.Traffic_Cnt = 0 or b.Store_Cd is null)
and Check_Traffic_Flg = 'Y'
";

 

$datastmt = $dbh->prepare($sql) or die "Can't prepare SQL statement $DBI::errstr\n";
$datastmt->execute;

 

while (@fld = $datastmt->fetchrow_array) {
push(@Store_Code,$fld[0]);
push(@manager_mail,$fld[1]);
$found = 1;
print("$fld[0] data missing.\n");
}
$datastmt->finish;

 

$dbh->disconnect;

 

if ($found == 0){
print("No missing RBD traffic\n");
}

 

return 0;
}