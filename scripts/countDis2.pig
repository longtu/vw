

SET job.name 'countHomeUdid.pig';
SET pig.exec.reducers.max 299;
%default IN '/homelocation'
%default OUT '/user/long/distinctHomeLocIP04'

rmf '$OUT';

A = load '$IN' USING PigStorage() AS (index:int, id:chararray,x:chararray, y:chararray);
B = GROUP A BY id;
C = FOREACH (GROUP B ALL) GENERATE COUNT(B);
STORE C INTO '$OUT';
