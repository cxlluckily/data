select distinct TIKCET_TRANS_SEQ,METRO_MEMBER_CARD_NUM,CITY_CODE,
			date_format(to_date(TRANS_DATE, 'yyyyMMdd'), 'yyyy-MM-dd') AS days
		from SKP.METRO_MEMBER_SUBSCRIPTION_TRANS 
		where TIKCET_TRANS_SEQ is not null
		and METRO_MEMBER_CARD_NUM is not null
		<#if startTime??> and date_format(to_date(TRANS_DATE, 'yyyyMMdd'), 'yyyy-MM-dd')>= '${startTime}' </#if>
		<#if endTime??> and date_format(to_date(TRANS_DATE, 'yyyyMMdd'), 'yyyy-MM-dd') < '${endTime}' </#if>
		<#if lastTimestamp??> and t_last_timestamp<=${lastTimestamp} </#if>