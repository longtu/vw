

SET pig.exec.reducers.max 1;

%default IN '/user/long/homeLocHisto918'
%default OUT '/user/long/homeLocCombine918'

rmf '$OUT';

A = LOAD '$IN' USING PigStorage() AS (country:chararray, num:int);
B = ORDER A BY num;
STORE B INTO '$OUT';