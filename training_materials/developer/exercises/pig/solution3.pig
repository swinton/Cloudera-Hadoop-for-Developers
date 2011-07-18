movierating = LOAD 'movierating' AS (userid, movieid, rating:INT);
groupmr = GROUP movierating BY movieid;
ratings = FOREACH groupmr GENERATE
		group AS movieid,
		COUNT(movierating.rating) AS numratings,
		AVG(movierating.rating) AS avgrating;
movie = LOAD 'movie' AS (id, name, year);
mr = JOIN movie BY id, ratings BY movieid;
result = FOREACH mr GENERATE id, name, year, numratings, avgrating;
STORE result INTO 'ratedmovie';

