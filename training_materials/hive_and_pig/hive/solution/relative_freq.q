# make sure we use a single reducer so that SORT BY becomes ORDER BY
set mapred.reduce.tasks = 1;

# Establish the total size of the shakespeare corpus
SELECT SUM(freq) FROM shake_freq;

# remember this number, it's 974034 -- that's the total num of words.

# Now repeat this for the bible
SELECT SUM(freq) FROM bible_freq;
# ... the answer is 894145.


# Now let's create scaled tables
CREATE TABLE shake_freq_2(sfreq DOUBLE, word STRING);
CREATE TABLE bible_freq_2(sfreq DOUBLE, word STRING);

# And populate them
INSERT OVERWRITE TABLE shake_freq_2 SELECT freq/974034.0, word FROM shake_freq;
INSERT OVERWRITE TABLE bible_freq_2 SELECT freq/894145.0, word FROM bible_freq;

# And a table for the merging
CREATE TABLE merged_2 (word STRING, shake_f DOUBLE, kjv_f DOUBLE);
INSERT OVERWRITE TABLE merged_2 SELECT s.word, s.sfreq, b.sfreq
  FROM shake_freq_2 s JOIN bible_freq_2 b ON (s.word = b.word) 
  WHERE s.sfreq >= 0 AND b.sfreq >= 0;

# Sanity check the results:
SELECT * FROM merged_2 LIMIT 20;

# Finally, what's the most frequent?
SELECT word, shake_f, kjv_f, (shake_f + kjv_f) AS ss
    FROM merged_2 SORT BY ss DESC LIMIT 20;


# And you'll note -- there are in fact a few differences vs. the 'merged' table.


