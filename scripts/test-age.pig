SET mapreduce.map.memory.mb 2048;
SET mapreduce.map.java.opts '-Xmx1500m';

SET mapreduce.reduce.memory.mb 4096;
SET mapreduce.reduce.java.opts '-Xmx3g';

%default IN1 '/age_index/12_18_25/'
%default IN2 '/age_index/12_18_25/'

A = load '$IN1' USING PigStorage() AS (index1:int, id1:chararray, age1:int);
B = load '$IN2' USING PigStorage() AS (index2:int, id2:chararray, age2:int);

C = JOIN A BY id1, B BY id2;
D = FOREACH C GENERATE A::id1, (A::age1 - B::age2) AS agediff:int;
E = FILTER D BY (agediff != 0);
STORE E INTO '/user/long/agediff';
GD_GROUP = GROUP E ALL;
age_counts = FOREACH GD_GROUP GENERATE COUNT(E) AS cnts;
STORE age_counts INTO '/user/long/agecount';

