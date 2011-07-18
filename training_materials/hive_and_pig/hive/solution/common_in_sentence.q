
# The sentence index has two columns separated by a TAB character.
# We need to import this data into Hive -- so first we make a table
# for it, and then we load the files in.

CREATE TABLE sentences(word STRING, sentence STRING) ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';

LOAD DATA INPATH '/user/training/input_idx' INTO TABLE sentences;


# In Henry IV, act II, there is the sentence:
# "this house is turned upside down since Robin Ostler died"
# Let's confirm that these words appear with one another.

SELECT s1.word, s2.word, s1.sentence FROM sentences s1
  JOIN sentences s2 ON (s1.sentence = s2.sentence)
  WHERE s1.word = 'upside' AND s2.word = 'down';


# Similarly, let's confirm the opposite case -- the word 'hanged' and 'Till'
# are both in the corpus, but not in the same sentence. This should return
# an empty result set.

SELECT s1.word, s2.word, s1.sentence FROM sentences s1
  JOIN sentences s2 ON (s1.sentence = s2.sentence)
  WHERE s1.word = 'hanged' AND s2.word = 'Till';


