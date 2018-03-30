SELECT ORDER_NO,city_code,  DATE_FORMAT(SJT_SALE_DATE, 'yyyy-MM-dd') AS days
			FROM SKP.SJT_QR_CODE
			WHERE sjt_status <> 03 AND sjt_status <> 99
			and city_code is not null
			<#if startDate??> and DATE_FORMAT(SJT_SALE_DATE, 'yyyy-MM-dd') >= '${startDate}' </#if>
			<#if endDate??> and DATE_FORMAT(SJT_SALE_DATE, 'yyyy-MM-dd') < '${endDate}' </#if>
	  		<#if lastTimestamp??> and t_last_timestamp<=${lastTimestamp} </#if>