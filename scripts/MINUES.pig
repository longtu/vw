%default IN1 '/user/long/hive1-modeltxt.txt'
%default IN2 '/user/long/sc2-modeltxt.txt'
%default OUT '/user/long/modelDiff'


REST1 = LOAD '$IN1' USING PigStorage() as (index:int, weights:double);

REST2 = LOAD '$IN2' USING PigStorage() as (index:int, weights:double);

JOINT = JOIN REST1 BY index,  REST2 BY index;

FINAL = FOREACH JOINT GENERATE REST1::index as index, (REST1::weights - REST2::weights)/REST1::weights as weight;

STORE FINAL INTO '$OUT' USING PigStorage();



