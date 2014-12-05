
SET job.name 'countUdid.pig';
SET pig.exec.reducers.max 299;


rmf '$OUT';

A = load '$IN' USING PigStorage() AS (id:chararray,ip:chararray, x:int, y:int);
B = GROUP A BY id;
C = FOREACH (GROUP B ALL) GENERATE COUNT(B);
STORE C INTO '$OUT';
