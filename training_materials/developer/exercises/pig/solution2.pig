movie = LOAD 'movie' AS (id, name, year);
movierating = LOAD 'movierating' AS (userid, movieid, rating);
mr = COGROUP movie BY id, movierating BY movieid;
unrated = FILTER mr BY IsEmpty(movierating);
answer = FOREACH unrated GENERATE FLATTEN(movie);
DUMP answer;

