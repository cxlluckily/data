SELECT
    tran_date, product_category, count(*) total
FROM
    skp.data_ypt_tran
WHERE
    tran_type IN ('53','54','62','63','73','74','90','91')
<#if startDate??> AND tran_date >= '${startDate}' </#if>
<#if endDate??> AND tran_date < '${endDate}' </#if>
<#if lastTimestamp??> AND t_last_timestamp <= '${lastTimestamp}' </#if>
GROUP BY tran_date, product_category