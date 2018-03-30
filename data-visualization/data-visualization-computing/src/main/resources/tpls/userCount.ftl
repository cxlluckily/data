SELECT  CITY_CODE, DATE_FORMAT(PAY_PAY_TIME, 'yyyy-MM-dd') days, PAY_PAY_ACCOUNT account
FROM skp.order_info
WHERE 
    PAY_PAY_TIME is not null
	AND CITY_CODE is not null
	AND PAY_PAY_ACCOUNT is not null
	<#if startDate??> AND DATE_FORMAT(PAY_PAY_TIME, 'yyyy-MM-dd') >= '${startDate}' </#if>
	<#if endDate??> AND DATE_FORMAT(PAY_PAY_TIME, 'yyyy-MM-dd') < '${endDate}' </#if>
	<#if lastTimestamp??> and t_last_timestamp <= ${lastTimestamp} </#if>

