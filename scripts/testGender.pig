


SET pig.exec.reducers.max 155;


SET mapreduce.map.memory.mb 4096;
SET mapreduce.map.java.opts '-Xmx3g';

SET mapreduce.reduce.memory.mb 4096;
SET mapreduce.reduce.java.opts '-Xmx3g';



INPT = LOAD '$IP' USING PigStorage('\t') as (index:int, id:chararray, gender:int);

INPT2= FOREACH INPT GENERATE id, gender;

INPT3 = FILTER INPT2 BY (gender == 1) or (gender == 0);

GD_GROUP = GROUP INPT3 BY gender;

--STORE GD_GROUP INTO '/user/long/gdgroup';

gender_counts = FOREACH GD_GROUP GENERATE group as gender, COUNT(INPT3) AS cnts;

STORE gender_counts INTO '$OUT';
