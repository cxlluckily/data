select city_code,DATE_FORMAT(TICKET_NOTI_TAKE_TICKET_RESULT_DATE, 'yyyy-MM-dd') AS days,sum(ticket_actual_take_ticket_num) num,'dcp' orderType
			from skp.order_info 
			where order_type='1' and ticket_order_status = '5' 
			and city_code is not null and city_code!='5190'
	  		<#if startTime??> and DATE_FORMAT(TICKET_NOTI_TAKE_TICKET_RESULT_DATE, 'yyyy-MM-dd') >= '${startTime}' </#if>
			<#if endTime??> and DATE_FORMAT(TICKET_NOTI_TAKE_TICKET_RESULT_DATE, 'yyyy-MM-dd') < '${endTime}' </#if>
	  		<#if lastTimestamp??> and t_last_timestamp<=${lastTimestamp} </#if>
			group by city_code,days
UNION			
select city_code, DATE_FORMAT(TOPUP_TOPUP_DATE, 'yyyy-MM-dd') AS days, count(1) num,'nfc' orderType
			from skp.order_info 
			where order_type='2' and TOPUP_ORDER_STATUS = '5' 
			and city_code is not null
			<#if startTime??> and DATE_FORMAT(TOPUP_TOPUP_DATE, 'yyyy-MM-dd') >= '${startTime}' </#if>
			<#if endTime??> and DATE_FORMAT(TOPUP_TOPUP_DATE, 'yyyy-MM-dd') < '${endTime}' </#if>
	  		<#if lastTimestamp??> and t_last_timestamp<=${lastTimestamp} </#if>
			group by city_code,days 
UNION			
select CITY_CODE,date_format(to_date(TRANS_DATE, 'yyyyMMdd'), 'yyyy-MM-dd') days,count(1),'xxhf' orderType 
			from (select distinct TIKCET_TRANS_SEQ,METRO_MEMBER_CARD_NUM,CITY_CODE,TRANS_DATE
					from SKP.METRO_MEMBER_SUBSCRIPTION_TRANS 
					where TIKCET_TRANS_SEQ is not null
					and METRO_MEMBER_CARD_NUM is not null
					<#if startTime??> and date_format(to_date(TRANS_DATE, 'yyyyMMdd'), 'yyyy-MM-dd')>= '${startTime}' </#if>
					<#if endTime??> and date_format(to_date(TRANS_DATE, 'yyyyMMdd'), 'yyyy-MM-dd') < '${endTime}' </#if>
					<#if lastTimestamp??> and t_last_timestamp<=${lastTimestamp} </#if>
				)
					group by CITY_CODE,days				
UNION		
 select city_code,DATE_FORMAT(TICKET_NOTI_TAKE_TICKET_RESULT_DATE, 'yyyy-MM-dd') AS days,sum(TICKET_ACTUAL_TAKE_TICKET_NUM) num	,'xfhx' orderType
			from skp.order_info 
			where ticket_order_status = '5'
			AND  ORDER_PRODUCT_CODE IN('ZH_RAIL_A','ZH_RAIL_I')
			and city_code is not null 
	  		<#if startTime??> and DATE_FORMAT(TICKET_NOTI_TAKE_TICKET_RESULT_DATE, 'yyyy-MM-dd') >= '${startTime}' </#if>
			<#if endTime??> and DATE_FORMAT(TICKET_NOTI_TAKE_TICKET_RESULT_DATE, 'yyyy-MM-dd') < '${endTime}' </#if>
	  		<#if lastTimestamp??> and t_last_timestamp<=${lastTimestamp} </#if>
	  		group by city_code,days
UNION
SELECT city_code,  DATE_FORMAT(SJT_SALE_DATE, 'yyyy-MM-dd') AS days, count(1) num,'xfhx' orderType
			FROM SKP.SJT_QR_CODE
			WHERE sjt_status <> 03 AND sjt_status <> 99
			and city_code is not null
			<#if startTime??> and DATE_FORMAT(SJT_SALE_DATE, 'yyyy-MM-dd') >= '${startTime}' </#if>
			<#if endTime??> and DATE_FORMAT(SJT_SALE_DATE, 'yyyy-MM-dd') < '${endTime}' </#if>
	  		<#if lastTimestamp??> and t_last_timestamp<=${lastTimestamp} </#if>
	  		group by city_code,days			
UNION			
 SELECT city_code,  DATE_FORMAT(UPDATE_DATE, 'yyyy-MM-dd') AS days, count(1) num,'coffee' orderType
			FROM COFFEE.T_TASTE_ORDER
			WHERE ORDER_STATE in ('4','5')
			and city_code is not null		
			<#if startTime??> and DATE_FORMAT(UPDATE_DATE, 'yyyy-MM-dd') >= '${startTime}' </#if>
			<#if endTime??> and DATE_FORMAT(UPDATE_DATE, 'yyyy-MM-dd') < '${endTime}' </#if>
	  		<#if lastTimestamp??> and t_last_timestamp<=${lastTimestamp} </#if>
	  		group by city_code,days