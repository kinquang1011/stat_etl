<?php
date_default_timezone_set("Asia/Ho_Chi_Minh");
$connect = mysql_connect("10.60.22.2","root","Hyi76tZpro") or die (mysql_error());
mysql_select_db("ubstats");


$kpi_type = array("hourly_kpi","realtime_game_kpi", "channel_kpi","os_kpi","package_kpi","game_kpi","server_kpi");
//$kpi_type=array("game_retention");
$game_code="ntgh";
$game_name="Nhất Thống Giang Hồ";

$data_source="ingame";
$current = date('Y-m-d H:i:s');

insert_into_game($game_code, $game_name, $data_source, $create_date);

for($i=1;$i<4;$i++){
    for($j=0;$j<count($kpi_type);$j++){
        $_kpi_type = $kpi_type[$j];
        insert_into_db($game_code, $i, $_kpi_type, $data_source, $current);
    }
}

mysql_close($connect);

function insert_into_db($game_code, $group_id, $kpi_type, $data_source, $create_date)
{
    $sql = "insert into mt_report_source (game_code,kpi_type,group_id,data_source,create_date) values ('$game_code','$kpi_type', '$group_id','$data_source','$create_date')";
    mysql_query($sql);
    echo $sql . "\n";
}

function insert_into_game($game_code, $game_name, $data_source, $create_date)
{
    $sql = "insert into games values ('$game_code','$game_name', 2, 3, '$data_source', 0.3, 0.7, '$create_date','vinhdp', 2, '','$create_date', 0, 'local')";
    mysql_query($sql);
    echo $sql . "\n";
}
