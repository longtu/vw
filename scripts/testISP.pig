%default NO_REGISTER ''

$NO_REGISTER REGISTER /home/long/lib/dpp-pig-udf-0.0.1-SNAPSHOT.jar;
$NO_REGISTER REGISTER /home/long/lib/dpp-extract-profile-0.0.2-SNAPSHOT.jar;
$NO_REGISTER REGISTER /home/long/lib/misc-0.0.1-SNAPSHOT.jar;
$NO_REGISTER REGISTER /home/long/lib/geoip-api-1.2.11.jar;
$NO_REGISTER REGISTER /home/long/lib/piggybank-apache-lib.jar
$NO_REGISTER REGISTER /home/long/lib/avro-1.7.5-cdh5.1.0.jar
$NO_REGISTER REGISTER /home/long/lib/avro-mapred-1.7.5-cdh5.1.0.jar
$NO_REGISTER REGISTER /home/long/lib/jackson-core-asl-1.8.8.jar
$NO_REGISTER REGISTER /home/long/lib/jackson-mapper-asl-1.8.8.jar
$NO_REGISTER REGISTER /home/long/lib/json-simple-1.1.jar
$NO_REGISTER REGISTER /home/long/lib/logging-0.0.1-SNAPSHOT.jar;

DEFINE AvroStorage	org.apache.pig.builtin.AvroStorage('no_schema_check');
DEFINE GetIPOrg	com.apache-lib.dpp.pig.geo.IP2Organization();
DEFINE IP2Domain	com.apache-lib.dpp.pig.geo.IP2Domain();
DEFINE Ip2ISP	com.apache-lib.dpp.pig.geo.IP2Isp();
DEFINE IsValidIP	com.apache-lib.dpp.pig.log.IsValidIP();
DEFINE ProfileToIP              com.apache-lib.dpp.pig.profile.ProfileToIPFeatures();

SET mapred.output.compress true;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;
SET pig.exec.reducers.max 155;

SET mapreduce.reduce.memory.mb 4096;
SET mapreduce.reduce.java.opts '-Xmx3g';

%default IP '*.avro'
%default OUT '/user/long/testISP3'

IN = LOAD '$IP' USING AvroStorage();

IP1 = foreach IN generate key as id:chararray, ProfileToIP(value.profiles) as ipbag:bag{t:tuple (ip:chararray,cnt:long)};
IP2 = FOREACH IP1 GENERATE id, FLATTEN(ipbag) AS (ip:chararray, cnt:long);
IP3 = FILTER IP2 BY ip IS NOT NULL AND IsValidIP(ip);

IP4 = FOREACH IP3 GENERATE id, LOWER(TRIM(Ip2ISP(ip))) AS isp;

IP5 = DISTINCT IP4;

by_isp = GROUP IP5 BY isp;

isp_counts = FOREACH by_isp GENERATE group as isp, COUNT(IP5) AS cnts;

OUT1 = ORDER isp_counts BY cnts DESC;

STORE OUT1 INTO '$OUT';








