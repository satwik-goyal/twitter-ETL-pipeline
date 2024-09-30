/* Q5 is a query that returns "how exactly the advertisers target the user; that is, you want to see the top ten advertisers and, for each of these ten advertisers, their top ten combinations of targeting type and targeting value".

The query should return a table with three columns labeled "Advertiser" (text), "Criteria Type" (text), and "Criterion" (text).

The query should not modify the database and be a single SQL statement.  */

WITH top_advertisers AS (
    SELECT advertiser, COUNT(*) as ad_count
    FROM impressions
    GROUP BY advertiser
    ORDER BY ad_count DESC
    LIMIT 10
),
advertiser_targeting AS (
    SELECT 
        i.advertiser,
        tc.targetingType,
        tc.targetingValue,
        COUNT(*) as combination_count,
        ROW_NUMBER() OVER (PARTITION BY i.advertiser ORDER BY COUNT(*) DESC) as rank
    FROM impressions i
    JOIN matchedTargetingCriteria mtc ON i.id = mtc.impression
    JOIN TargetingCriteria tc ON mtc.criteria = tc.id
    WHERE i.advertiser IN (SELECT advertiser FROM top_advertisers)
    GROUP BY i.advertiser, tc.targetingType, tc.targetingValue
)
SELECT 
    advertiser as "Advertiser",
    targetingType as "Criteria Type",
    targetingValue as "Criterion"
FROM advertiser_targeting
WHERE rank <= 10
ORDER BY advertiser, combination_count DESC;