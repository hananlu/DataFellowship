SELECT
channelGrouping,
ARRAY_AGG(STRUCT(totals_transactions, geoNetwork_country,date) ORDER BY date ASC) AS transactions
FROM
`data-to-insights.ecommerce.rev_transactions`
GROUP BY channelGrouping;