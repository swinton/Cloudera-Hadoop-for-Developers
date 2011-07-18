ratedmovie = LOAD 'ratedmovie' AS (id, name, year, numratings, avgrating);
srted = ORDER ratedmovie BY avgrating DESC;
result = LIMIT srted 10;
DUMP result;

