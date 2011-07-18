movie = LOAD 'movie' AS (id, name, year);
yearmovie = FILTER movie BY year != 0;
sortmovie = ORDER yearmovie BY year;
oldestmovie = LIMIT sortmovie 1;
DUMP oldestmovie;

