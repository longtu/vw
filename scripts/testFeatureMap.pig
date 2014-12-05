
DEFINE SanitizeId       com.apache-lib.dpp.pig.log.SanitizeId();
DEFINE MergeFreq0       com.apache-lib.dpp.pig.log.MergeFreqBinary();
DEFINE AvroStorage	    org.apache.pig.builtin.AvroStorage('no_schema_check');
DEFINE PROFILETOAGE     com.apache-lib.dpp.pig.profile.ProfileToAgeFeatures();
DEFINE HashString       org.apache.pig.piggybank.evaluation.string.HashFNV();
DEFINE GetLabel         com.apache-lib.dpp.pig.vw.GetLabel('gender');
DEFINE GetFreqFeature   com.apache-lib.dpp.pig.vw.GetFeatureWithFrequency();
DEFINE RANKDB com.apache-lib.dpp.pig.util.Rank1();
DEFINE Log2  com.apache-lib.dpp.pig.util.Log2();

SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;
SET pig.maxCombinedSplitSize 1324177280;
SET mapred.task.timeout 0;
SET pig.exec.reducers.max 101;
SET job.name  'test-feature-map.pig';

SET mapreduce.map.memory.mb 5120;
SET mapreduce.map.java.opts '-Xmx4500m';

SET mapreduce.reduce.memory.mb 5120;
SET mapreduce.reduce.java.opts '-Xmx4500m';


DEFINE RANKUDF(S)
RETURNS RET {
	F = GROUP $S ALL;
	G = FOREACH F {
		O = ORDER $S BY feature desc;
		GENERATE RANKDB(O) AS ranks:{(feature:chararray, rank1:long)};
	};
	
	$RET = FOREACH G GENERATE FLATTEN(ranks) AS (prop:chararray, rank1:long);
};



IN = LOAD '$IN' USING AvroStorage();

IN2 = FOREACH IN GENERATE key as id:chararray,
								 FLATTEN(PROFILETOAGE(value.profiles)) AS (gender:chararray,
								 										   yob:chararray,
								 										   devtp:chararray, 
										 								   devos:chararray, 
										 								   devosv:chararray,
										 								   devmk:chararray, 
										 								   devmd:chararray,
										 								   prop_bag:{(prop:chararray, freq:long)},
										 								   flurry_gender:chararray,
										 								   flurry_age:chararray); 

JOINED = FOREACH IN2 GENERATE id,
                             gender as label:chararray,
                             devtp,
                             devosv,
                             MergeFreq0(prop_bag) as feat_bag:{(prop:chararray, freq:long)};

FEAT_FILTER = FILTER JOINED BY feat_bag IS NOT NULL AND NOT IsEmpty(feat_bag);

SPLIT FEAT_FILTER INTO LAB_FILT IF label IS NOT NULL, NO_LAB IF label IS NULL;


/*
 *  FEATURE INDEX: The following block put all property names into a tmp table
 *  and generate a feature index for each of them.
 */
-- prop
A = FOREACH LAB_FILT GENERATE FLATTEN(feat_bag) AS (prop:chararray, freq:long);
B = FOREACH A GENERATE prop AS feature:chararray;
C = GROUP B BY feature;
D = FOREACH C GENERATE group as feature;

-- static 
A1 = FOREACH LAB_FILT GENERATE devtp AS feature:chararray;
A1_G = GROUP A1 BY feature;
A1_F = FOREACH A1_G GENERATE group as feature;

A2 = FOREACH LAB_FILT GENERATE devosv AS feature:chararray;
A2_G = GROUP A2 BY feature;
A2_F = FOREACH A2_G GENERATE group as feature;

U = UNION D, A1_F, A2_F;
U_FEAT = FOREACH U GENERATE feature;
U_FEAT_DISCT = DISTINCT U_FEAT;

H = RANKUDF(U_FEAT_DISCT);
STORE H INTO '/user/long/labelfeatureMap' USING PigStorage();

-- end FEATURE INDEX

-- generate feature bit size based on feature count
/*
H_GROUP= GROUP H ALL;
I = FOREACH H_GROUP GENERATE Log2(COUNT(H));
STORE I INTO '$FEATURECOUNT' USING PigStorage();
*/