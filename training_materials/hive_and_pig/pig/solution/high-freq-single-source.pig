
-- extension 1 is a straightforward modification of the "bible not bard"
-- script shown in the lecture

bard   = LOAD '/user/hive/warehouse/shake_freq' USING PigStorage(',') AS (freq:int, word);
kjv    = LOAD '/user/hive/warehouse/bible_freq' USING PigStorage(',') AS (freq:int, word);
grpd   = COGROUP bard BY word, kjv BY word;

-- We change the filtering to pass through words which are in
-- bard but not bible.
nokjv  = FILTER grpd BY COUNT(kjv) == 0;
flat   = FOREACH nokjv GENERATE FLATTEN(bard);

-- and we add a sorting clause
srtd   = ORDER flat BY freq DESC;
STORE srtd INTO 'output';

