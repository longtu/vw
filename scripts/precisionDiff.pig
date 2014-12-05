
SET job.name 'precisionDiff.pig';
SET pig.exec.reducers.max 299;



rmf '$OUT2';

A = load '$IN1' USING PigStorage() as (id1:chararray, label1:int, precision1:double);
B = load '$IN3' USING PigStorage() as (id2:chararray, label2:int, precision2:double);

C = JOIN A BY id1, B BY id2;
D = FOREACH C GENERATE ((B::precision2!=0)?((A::precision1 - B::precision2)/B::precision2):(A::precision1)) as diff:double;

STORE D INTO '$OUT2' USING PigStorage();

