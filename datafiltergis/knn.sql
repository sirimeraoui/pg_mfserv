DROP TABLE IF EXISTS ScoresAll;
CREATE TABLE ScoresAll(MMSI, Geom, Score) AS
WITH SelectedMMSI(MMSI, NTile) AS (
  SELECT DISTINCT ON (MMSI) MMSI, NTILE(1000) OVER (ORDER BY MMSI)
  FROM AISInputFiltered ),
Points(MMSI, Id, Geom, T) AS (
  SELECT MMSI, Id, Geom, T
  FROM (
    SELECT MMSI, ROW_NUMBER() OVER (PARTITION BY MMSI ORDER BY T) AS Id, Geom, T
    FROM AISInputFiltered
    WHERE MMSI IN (SELECT MMSI FROM SelectedMMSI WHERE NTile = 1 ) ) T1 ),
Temp1(MMSI, geoms) AS (
  SELECT MMSI, array_agg(Geom ORDER BY Id) FROM Points
  GROUP BY MMSI ),
Temp2 AS (
  SELECT MMSI, wLocalOutlierFactor(geoms, 3, 0) AS rec
  FROM Temp1 )
SELECT MMSI, (rec).geom, (rec).score
FROM Temp2;
DROP TABLE








^CCancel request sent
ERROR:  canceling statement due to user request
ais=#
\q
esteban@P-ULB-PF59GXL3:~/src/MobilityDB/meos/examples$ psql ais
psql (17.5)
Type "help" for help.

ais=# \timing
Timing is on.
ais=# DROP TABLE IF EXISTS ScoresAll;
CREATE TABLE ScoresAll(MMSI, Geom, Score) AS
WITH SelectedMMSI(MMSI, NTile) AS (
  SELECT DISTINCT ON (MMSI) MMSI, NTILE(1000) OVER (ORDER BY MMSI)
  FROM AISInputFiltered ),
Points(MMSI, Id, Geom, T) AS (
  SELECT MMSI, Id, Geom, T
  FROM (
    SELECT MMSI, ROW_NUMBER() OVER (PARTITION BY MMSI ORDER BY T) AS Id, Geom, T
    FROM AISInputFiltered
    WHERE MMSI IN (SELECT MMSI FROM SelectedMMSI WHERE NTile = 1 ) ) T1 ),
Temp1(MMSI, geoms) AS (
  SELECT MMSI, array_agg(Geom ORDER BY Id) FROM Points
  GROUP BY MMSI ),
Temp2 AS (
  SELECT MMSI, wLocalOutlierFactor(geoms, 3, 0) AS rec
  FROM Temp1 )
SELECT MMSI, (rec).geom, (rec).score
FROM Temp2;
NOTICE:  table "scoresall" does not exist, skipping
DROP TABLE
Time: 1.062 ms
SELECT 9461
Time: 18934.796 ms (00:18.935)

ais=# DROP TABLE IF EXISTS ScoresAll;
CREATE TABLE ScoresAll(MMSI, Geom, Score) AS
WITH SelectedMMSI(MMSI, NTile) AS (
  SELECT DISTINCT ON (MMSI) MMSI, NTILE(500) OVER (ORDER BY MMSI)
  FROM AISInputFiltered ),
Points(MMSI, Id, Geom, T) AS (
  SELECT MMSI, Id, Geom, T
  FROM (
    SELECT MMSI, ROW_NUMBER() OVER (PARTITION BY MMSI ORDER BY T) AS Id, Geom, T
    FROM AISInputFiltered
    WHERE MMSI IN (SELECT MMSI FROM SelectedMMSI WHERE NTile = 1 ) ) T1 ),
Temp1(MMSI, geoms) AS (
  SELECT MMSI, array_agg(Geom ORDER BY Id) FROM Points
  GROUP BY MMSI ),
Temp2 AS (
  SELECT MMSI, wLocalOutlierFactor(geoms, 3, 0) AS rec
  FROM Temp1 )
SELECT MMSI, (rec).geom, (rec).score
FROM Temp2;
DROP TABLE
Time: 7.447 ms
SELECT 9461
Time: 20423.977 ms (00:20.424)

ais=# DROP TABLE IF EXISTS ScoresAll;
CREATE TABLE ScoresAll(MMSI, Geom, Score) AS
WITH SelectedMMSI(MMSI, NTile) AS (
  SELECT DISTINCT ON (MMSI) MMSI, NTILE(100) OVER (ORDER BY MMSI)
  FROM AISInputFiltered ),
Points(MMSI, Id, Geom, T) AS (
  SELECT MMSI, Id, Geom, T
  FROM (
    SELECT MMSI, ROW_NUMBER() OVER (PARTITION BY MMSI ORDER BY T) AS Id, Geom, T
    FROM AISInputFiltered
    WHERE MMSI IN (SELECT MMSI FROM SelectedMMSI WHERE NTile = 1 ) ) T1 ),
Temp1(MMSI, geoms) AS (
  SELECT MMSI, array_agg(Geom ORDER BY Id) FROM Points
  GROUP BY MMSI ),
Temp2 AS (
  SELECT MMSI, wLocalOutlierFactor(geoms, 3, 0) AS rec
  FROM Temp1 )
SELECT MMSI, (rec).geom, (rec).score
FROM Temp2;
DROP TABLE
Time: 4.405 ms
SELECT 9461
Time: 36435.054 ms (00:36.435)

ais=# DROP TABLE IF EXISTS ScoresAll;
CREATE TABLE ScoresAll(MMSI, Geom, Score) AS
WITH SelectedMMSI(MMSI, NTile) AS (
  SELECT DISTINCT ON (MMSI) MMSI, NTILE(50) OVER (ORDER BY MMSI)
  FROM AISInputFiltered ),
Points(MMSI, Id, Geom, T) AS (
  SELECT MMSI, Id, Geom, T
  FROM (
    SELECT MMSI, ROW_NUMBER() OVER (PARTITION BY MMSI ORDER BY T) AS Id, Geom, T
    FROM AISInputFiltered
    WHERE MMSI IN (SELECT MMSI FROM SelectedMMSI WHERE NTile = 1 ) ) T1 ),
Temp1(MMSI, geoms) AS (
  SELECT MMSI, array_agg(Geom ORDER BY Id) FROM Points
  GROUP BY MMSI ),
Temp2 AS (
  SELECT MMSI, wLocalOutlierFactor(geoms, 3, 0) AS rec
  FROM Temp1 )
SELECT MMSI, (rec).geom, (rec).score
FROM Temp2;
DROP TABLE
Time: 4.272 ms
SELECT 9461
Time: 53906.311 ms (00:53.906)

ais=# DROP TABLE IF EXISTS ScoresAll;
CREATE TABLE ScoresAll(MMSI, Geom, Score) AS
WITH SelectedMMSI(MMSI, NTile) AS (
  SELECT DISTINCT ON (MMSI) MMSI, NTILE(10) OVER (ORDER BY MMSI)
  FROM AISInputFiltered ),
Points(MMSI, Id, Geom, T) AS (
  SELECT MMSI, Id, Geom, T
  FROM (
    SELECT MMSI, ROW_NUMBER() OVER (PARTITION BY MMSI ORDER BY T) AS Id, Geom, T
    FROM AISInputFiltered
    WHERE MMSI IN (SELECT MMSI FROM SelectedMMSI WHERE NTile = 1 ) ) T1 ),
Temp1(MMSI, geoms) AS (
  SELECT MMSI, array_agg(Geom ORDER BY Id) FROM Points
  GROUP BY MMSI ),
Temp2 AS (
  SELECT MMSI, wLocalOutlierFactor(geoms, 3, 0) AS rec
  FROM Temp1 )
SELECT MMSI, (rec).geom, (rec).score
FROM Temp2;
DROP TABLE
Time: 10.241 ms
SELECT 470018
Time: 582912.915 ms (09:42.913)

ais=# DROP TABLE IF EXISTS ScoresAll;
CREATE TABLE ScoresAll(MMSI, Geom, Score) AS
WITH SelectedMMSI(MMSI, NTile) AS (
  SELECT DISTINCT ON (MMSI) MMSI, NTILE(5) OVER (ORDER BY MMSI)
  FROM AISInputFiltered ),
Points(MMSI, Id, Geom, T) AS (
  SELECT MMSI, Id, Geom, T
  FROM (
    SELECT MMSI, ROW_NUMBER() OVER (PARTITION BY MMSI ORDER BY T) AS Id, Geom, T
    FROM AISInputFiltered
    WHERE MMSI IN (SELECT MMSI FROM SelectedMMSI WHERE NTile = 1 ) ) T1 ),
Temp1(MMSI, geoms) AS (
  SELECT MMSI, array_agg(Geom ORDER BY Id) FROM Points
  GROUP BY MMSI ),
Temp2 AS (
  SELECT MMSI, wLocalOutlierFactor(geoms, 3, 0) AS rec
  FROM Temp1 )
SELECT MMSI, (rec).geom, (rec).score
FROM Temp2;
DROP TABLE
Time: 8.201 ms
SELECT 947258
Time: 1163001.028 ms (19:23.001)

ais=# DROP TABLE IF EXISTS ScoresAll;
CREATE TABLE ScoresAll(MMSI, Geom, Score) AS
WITH SelectedMMSI(MMSI, NTile) AS (
  SELECT DISTINCT ON (MMSI) MMSI, NTILE(2) OVER (ORDER BY MMSI)
  FROM AISInputFiltered ),
Points(MMSI, Id, Geom, T) AS (
  SELECT MMSI, Id, Geom, T
  FROM (
    SELECT MMSI, ROW_NUMBER() OVER (PARTITION BY MMSI ORDER BY T) AS Id, Geom, T
    FROM AISInputFiltered
    WHERE MMSI IN (SELECT MMSI FROM SelectedMMSI WHERE NTile = 1 ) ) T1 ),
Temp1(MMSI, geoms) AS (
  SELECT MMSI, array_agg(Geom ORDER BY Id) FROM Points
  GROUP BY MMSI ),
Temp2 AS (
  SELECT MMSI, wLocalOutlierFactor(geoms, 3, 0) AS rec
  FROM Temp1 )
SELECT MMSI, (rec).geom, (rec).score
FROM Temp2;
DROP TABLE
Time: 13.910 ms
SELECT 2264315
Time: 2911775.253 ms (48:31.775)