
-- Load in the jar that contains our UDFs
REGISTER textudf.jar;

-- load the (word, sentence) pairs in.
bard = LOAD 'input_idx' USING PigStorage('\t') AS (word:chararray, sentence:chararray);

-- Ignore anything that isn't a Capital word. We ignore FULLY capitalized words,
-- as well as the word 'I'.
caps = FILTER bard BY word matches '[A-Z][a-z][a-zA-Z]*';

-- Find all instances where the word is capitalized, and doesn't start its sentence
not_starting = FILTER caps BY NOT textudf.StartsWith(sentence, word);

-- throw away the sentence, we just want the word part of the record.
justword = FOREACH not_starting GENERATE word;

-- deduplicate...
nodups = DISTINCT justword;

-- and write out the results.
STORE nodups INTO 'proper_nouns';


