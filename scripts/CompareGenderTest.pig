

SET mapred.job.queue.name critical;

IN1 = LOAD '$INPUT1' AS (index1:int, id1:chararray, gender1:int);
IN2 = LOAD '$INPUT2' AS (index2:int, id2:chararray, gender2:int);

IN1_FILTER = FILTER IN1 BY gender1 is not null and not IsEmpty(gender1);
IN2_FILTER = FILTER IN2 BY gender2 is not null and not IsEmpty(gender2);

X = JOIN IN1_FILTER BY id1, IN2_FILTER BY id2;
Y = FOREACH X GENERATE IN1_FILTER::id1 as id:chararray, ABS(IN1_FILTER::gender1 - IN2_FILTER::gender2) AS abs:int;
Z = GROUP Y ALL;
K = FOREACH Z GENERATE group, SUM(Y.abs);
STORE K INTO '$OUT';

