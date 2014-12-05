

SET pig.exec.reducers.max 299;


rmf '$OUT';

A = LOAD '$IN' USING PigStorage() AS (index:int, id:chararray,city:chararray, zip:chararray, scity:chararray, state:chararray, country:chararray);
B = GROUP A BY country;
C = FOREACH B GENERATE group AS country, COUNT(A);
STORE C INTO '$OUT';
