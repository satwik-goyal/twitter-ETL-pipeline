/* Q1 is a query that returns "how many ads have been sent to the user, i.e., the number of all ads from the current dataset".

The query should return a table with a single column labeled "Ads Shown" (integer).

The query should not modify the database and be a single SQL statement. */
   

SELECT COUNT(*) AS "Ads Shown"
FROM impressions;
