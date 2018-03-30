select city_code,days,source,sum(num) from (
	SELECT city_code, sum(ticket_actual_take_ticket_num) num, 
	                CASE 
			WHEN pay_payment_type IN ('-1','0','1','3','5','8','11') THEN 'skf' 
			WHEN pay_payment_type IN ('12','13','14','7') THEN 'wx'  
			WHEN pay_payment_type IN ('6') THEN 'zfb' 
			WHEN pay_payment_type IN ('2','4') THEN 'ygpj' 
			WHEN pay_payment_type IN ('9') THEN 'hb' 
			ELSE 'qt' 
			END AS source,
			DATE_FORMAT(TICKET_NOTI_TAKE_TICKET_RESULT_DATE, 'yyyy-MM-dd') AS days
			FROM skp.order_info 
			WHERE order_type='1' AND ticket_order_status = '5' and city_code!='5190' and PAY_STATE='2'		
			<#if startTime??> and DATE_FORMAT(TICKET_NOTI_TAKE_TICKET_RESULT_DATE, 'yyyy-MM-dd') >= '${startTime}' </#if>	
			<#if endTime??> and DATE_FORMAT(TICKET_NOTI_TAKE_TICKET_RESULT_DATE, 'yyyy-MM-dd') < '${endTime}' </#if>	
			<#if lastTimestamp??> and t_last_timestamp<=${lastTimestamp} </#if> 
			 group by city_code, days,source
			 UNION
	SELECT city_code, count(1) num,
	                CASE 
			WHEN pay_payment_type IN ('-1','0','1','3','5','8','11') THEN 'skf' 
			WHEN pay_payment_type IN ('12','13','14','7') THEN 'wx'  
			WHEN pay_payment_type IN ('6') THEN 'zfb' 
			WHEN pay_payment_type IN ('2','4') THEN 'ygpj' 
			WHEN pay_payment_type IN ('9') THEN 'hb' 
			ELSE 'qt' 
			END AS source,
			DATE_FORMAT(TOPUP_TOPUP_DATE, 'yyyy-MM-dd') AS days
			FROM skp.order_info 
			WHERE order_type='2' AND TOPUP_ORDER_STATUS = '5' and PAY_STATE='2'
			<#if startTime??> and DATE_FORMAT(TOPUP_TOPUP_DATE, 'yyyy-MM-dd') >= '${startTime}' </#if>
			<#if endTime??> and DATE_FORMAT(TOPUP_TOPUP_DATE, 'yyyy-MM-dd') < '${endTime}' </#if>
			<#if lastTimestamp??> and t_last_timestamp<=${lastTimestamp} </#if>
			 group by city_code, days,source
			 UNION
	select CITY_CODE , count(1) num, 
			CASE 
			WHEN PROVIDER_ID IN ('01') THEN 'gzdt'
			WHEN PROVIDER_ID IN ('02') THEN 'skf' 
			WHEN PROVIDER_ID IN ('11') THEN 'zfb' 
			WHEN PROVIDER_ID IN ('12') THEN 'wx' 
			ELSE 'qt' 
			END AS source,days from
			(select distinct TIKCET_TRANS_SEQ,METRO_MEMBER_CARD_NUM,CITY_CODE,
				date_format(to_date(TRANS_DATE, 'yyyyMMdd'), 'yyyy-MM-dd') AS days,PROVIDER_ID
			from SKP.METRO_MEMBER_SUBSCRIPTION_TRANS 
			where TIKCET_TRANS_SEQ is not null
			and METRO_MEMBER_CARD_NUM is not null
			<#if startTime??> and date_format(to_date(TRANS_DATE, 'yyyyMMdd'), 'yyyy-MM-dd')>= '${startTime}' </#if>
			<#if endTime??> and date_format(to_date(TRANS_DATE, 'yyyyMMdd'), 'yyyy-MM-dd') < '${endTime}' </#if>
			<#if lastTimestamp??> and t_last_timestamp<=${lastTimestamp} </#if> 
			)group by city_code,days,source
			UNION
	SELECT city_code, sum(TICKET_ACTUAL_TAKE_TICKET_NUM) num, 
	                CASE 
			WHEN pay_payment_type IN ('-1','0','1','3','5','8','11') THEN 'skf' 
			WHEN pay_payment_type IN ('12','13','14','7') THEN 'wx'  
			WHEN pay_payment_type IN ('6') THEN 'zfb' 
			WHEN pay_payment_type IN ('2','4') THEN 'ygpj' 
			WHEN pay_payment_type IN ('9') THEN 'hb' 
			ELSE 'qt' 
			END AS source,
			DATE_FORMAT(TICKET_NOTI_TAKE_TICKET_RESULT_DATE, 'yyyy-MM-dd') AS days	
			FROM skp.order_info 
			WHERE ticket_order_status = '5' and PAY_STATE='2'
			AND ORDER_PRODUCT_CODE IN('ZH_RAIL_A','ZH_RAIL_I','EMP_TICKET_A','EMP_TICKET_I')
			<#if startTime??> and DATE_FORMAT(TICKET_NOTI_TAKE_TICKET_RESULT_DATE, 'yyyy-MM-dd') >='${startTime}' </#if>
			<#if endTime??> and DATE_FORMAT(TICKET_NOTI_TAKE_TICKET_RESULT_DATE, 'yyyy-MM-dd') < '${endTime}' </#if>
			<#if lastTimestamp??> and t_last_timestamp<=${lastTimestamp} </#if>
			 group by city_code,days,source
			 UNION
	SELECT city_code, count(1) num, 
	                CASE 
			WHEN pay_payment_type IN ('-1','0','1','3','5','8','11') THEN 'skf' 
			WHEN pay_payment_type IN ('12','13','14','7') THEN 'wx'  
			WHEN pay_payment_type IN ('6') THEN 'zfb' 
			WHEN pay_payment_type IN ('2','4') THEN 'ygpj' 
			WHEN pay_payment_type IN ('9') THEN 'hb' 
			ELSE 'qt' 
			END AS source,
	                 DATE_FORMAT(XFHX_SJT_SALE_DATE, 'yyyy-MM-dd') AS days	
			FROM skp.order_info 
			WHERE xfhx_sjt_status <> 03 AND xfhx_sjt_status <> 99 and PAY_STATE='2'
			<#if startTime??> and DATE_FORMAT(XFHX_SJT_SALE_DATE, 'yyyy-MM-dd') >='${startTime}' </#if>
			<#if endTime??> and DATE_FORMAT(XFHX_SJT_SALE_DATE, 'yyyy-MM-dd') < '${endTime}' </#if>
			<#if lastTimestamp??> and t_last_timestamp<=${lastTimestamp} </#if>
			group by city_code,days,source
			UNION
	SELECT city_code, count(1) num, 
	                 CASE 
			WHEN pay_payment_type IN ('-1','0','1','3','5','8','11') THEN 'skf' 
			WHEN pay_payment_type IN ('12','13','14','7') THEN 'wx'  
			WHEN pay_payment_type IN ('6') THEN 'zfb' 
			WHEN pay_payment_type IN ('2','4') THEN 'ygpj' 
			WHEN pay_payment_type IN ('9') THEN 'hb' 
			ELSE 'qt' 
			END AS source,
	                 DATE_FORMAT(COFFEE_UPDATE_DATE, 'yyyy-MM-dd') AS days
			FROM skp.order_info 	
			WHERE order_type = '5' AND COFFEE_ORDER_STATE in('4','5') and PAY_STATE='2'	
			<#if startTime??> and DATE_FORMAT(COFFEE_UPDATE_DATE, 'yyyy-MM-dd') >= '${startTime}' </#if>
			<#if endTime??> and DATE_FORMAT(COFFEE_UPDATE_DATE, 'yyyy-MM-dd') < '${endTime}' </#if>
			<#if lastTimestamp??> and t_last_timestamp<=${lastTimestamp} </#if>   
			group by city_code,days,source	
	)
group by city_code,days,source