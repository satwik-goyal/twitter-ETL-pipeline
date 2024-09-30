/* Q3 is a query that returns "how the advertisers target the user; i.e., you want to see the top ten targeting types and the number of ads of that type".

The query should return a table with two columns labeled "Criteria Category" (text) and "Ad Count" (integer).

The query should not modify the database and be a single SQL statement. */
   

SELECT c.targetingType AS "Criteria Category", COUNT(*) AS "Ad Count"
FROM TargetingCriteria c
JOIN matchedTargetingCriteria m ON c.id = m.criteria
GROUP BY "Criteria Category"
ORDER BY "Ad Count" DESC
LIMIT 10;
