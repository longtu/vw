
%default IN1 '/user/long/hl-result'
%default IN2 '/user/long/hl-result2'

SET job.name 'HLResult.pig';
SET pig.exec.reducers.max 299;

A = load '$IN1' USING PigStorage() AS (index1:int, id1:chararray,city1:chararray, zip1:chararray, scity1:chararray, state1:chararray, country1:chararray);
B = load '$IN2' USING PigStorage() AS (index2:int, id2:chararray,city2:chararray, zip2:chararray, scity2:chararray, state2:chararray, country2:chararray);


JOINT = JOIN A BY id1, B BY id2;
C = FOREACH JOINT GENERATE id1 as id:chararray, city1, city2, zip1, zip2, scity1,scity2, state1,state2, country1,country2;

D = FILTER C BY (city1!=city2) OR (zip1!=zip2) OR (scity1 != scity2) OR (state1!=state2) OR (country1!=country2);

STORE D INTO '/user/long/hl-result-compare/';