/* Q2 is a query that returns "the number of different advertisers that have targeted the user's twitter account".

The query should return a table with a single column labeled "Advertisers" (integer).

The query should not modify the database and be a single SQL statement. */



SELECT COUNT(DISTINCT advertiserName) AS "Advertisers"
FROM advertiserInfo; 
