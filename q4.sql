/* Q4 is a query that returns "when the advertisers target the user; that is, for each time of the day in hours, the number of ads they have been sent".


The query should return a table with a two columns labeled "Hour" (ranging from 00 to 23) and "Ad Count" (integer).

The query should not modify the database and be a single SQL statement. */


SELECT 
    CAST(SUBSTR(impressionTime, 12, 2) AS INTEGER) AS "Hour",
    COUNT(*) AS "Ad Count"
FROM impressions
GROUP BY "Hour"
ORDER BY "Hour";