SELECT city_code, days, payment_code, SUM(num) num FROM (
	SELECT city_code, 	
	  order_type, 
	  CASE WHEN pay_payment_type = 0 OR pay_payment_type = 2 OR pay_payment_type = 6 OR pay_payment_type = 10 THEN 'ZFB'
		ELSE 
			CASE WHEN pay_payment_type = 1 OR pay_payment_type = 9 THEN 'ZYD' 
		ELSE
			CASE WHEN pay_payment_type = 3 OR pay_payment_type = 4 OR pay_payment_type = 7 
			OR pay_payment_type = 12 OR pay_payment_type = 13 OR pay_payment_type = 14 THEN 'WX' 
		ELSE 
			CASE WHEN pay_payment_type = 5 THEN 'YZF' 
		ELSE 
			CASE WHEN pay_payment_type = 8 THEN 'SXYZF' 
		ELSE 
			CASE WHEN pay_payment_type = 11 OR pay_payment_type = 'YLSF' THEN 'YL' 
		ELSE 'OTHER' 
	    END END END END END
	  END payment_code,	
	  num,
	  DATE_FORMAT(days, 'yyyy-MM-dd') days 
	 FROM (	
		SELECT city_code, order_type, ticket_actual_take_ticket_num num, pay_payment_type, 
			TICKET_NOTI_TAKE_TICKET_RESULT_DATE
			AS days, t_last_timestamp	
		FROM skp.order_info o	
		WHERE order_type = '1' 
		AND ticket_order_status = '5' and pay_state = '2' AND city_code!='5190' and pay_payment_type is not null
		<#if startDate??> AND DATE_FORMAT(TICKET_NOTI_TAKE_TICKET_RESULT_DATE, 'yyyy-MM-dd') >= '${startDate}' </#if> 
		<#if endDate??> AND DATE_FORMAT(TICKET_NOTI_TAKE_TICKET_RESULT_DATE, 'yyyy-MM-dd') <= '${endDate}' </#if>
		<#if lastTimestamp?? >AND t_last_timestamp <= ${lastTimestamp}</#if>
		UNION 
		SELECT city_code, order_type, '1' num, pay_payment_type, 
			TOPUP_TOPUP_DATE AS days, t_last_timestamp	
		FROM skp.order_info o	
		WHERE order_type = '2' 
		and topup_order_status = '5' AND pay_state = '2'
		<#if startDate??> AND DATE_FORMAT(TOPUP_TOPUP_DATE, 'yyyy-MM-dd') >= '${startDate}' </#if> 
		<#if endDate??> AND DATE_FORMAT(TOPUP_TOPUP_DATE, 'yyyy-MM-dd') <= '${endDate}' </#if>
		<#if lastTimestamp?? >AND t_last_timestamp <= ${lastTimestamp}</#if>
		UNION 
		SELECT city_code, order_type, '1' num, pay_payment_type, 		
				 XXHF_ORDER_DATE AS days, t_last_timestamp	
			FROM skp.order_info o	
			WHERE 
			order_type = '3' AND XXHF_DEBIT_REQUEST_RESULT = '0' and pay_state = '2'
			<#if startDate??> AND DATE_FORMAT(XXHF_ORDER_DATE, 'yyyy-MM-dd') >= '${startDate}' </#if> 
			<#if endDate??> AND DATE_FORMAT(XXHF_ORDER_DATE, 'yyyy-MM-dd') <= '${endDate}' </#if>
			<#if lastTimestamp?? >AND t_last_timestamp <= ${lastTimestamp}</#if>
		UNION 
		SELECT city_code, order_type, 
			TICKET_ACTUAL_TAKE_TICKET_NUM num, pay_payment_type, 			
			TICKET_NOTI_TAKE_TICKET_RESULT_DATE 
				 AS days, t_last_timestamp	
			FROM skp.order_info o	
		WHERE ORDER_PRODUCT_CODE IN('ZH_RAIL_A','ZH_RAIL_I','EMP_TICKET_A','EMP_TICKET_I') AND ticket_order_status = '5'
			and pay_state = '2' and ticket_order_status = '5'
			<#if startDate??> AND DATE_FORMAT(TICKET_NOTI_TAKE_TICKET_RESULT_DATE, 'yyyy-MM-dd') >= '${startDate}' </#if> 
			<#if endDate??> AND DATE_FORMAT(TICKET_NOTI_TAKE_TICKET_RESULT_DATE, 'yyyy-MM-dd') <= '${endDate}' </#if> 
			<#if lastTimestamp?? >AND t_last_timestamp <= ${lastTimestamp}</#if>
		UNION
		SELECT city_code, order_type, '1' num, pay_payment_type, 			
			XFHX_SJT_SALE_DATE 
				 AS days, t_last_timestamp	
			FROM skp.order_info o	
			WHERE  pay_state = '2'
			AND xfhx_sjt_status <> '03' AND xfhx_sjt_status <> '99'
			<#if startDate??> AND DATE_FORMAT(XFHX_SJT_SALE_DATE, 'yyyy-MM-dd') >= '${startDate}' </#if> 
			<#if endDate??> AND DATE_FORMAT(XFHX_SJT_SALE_DATE, 'yyyy-MM-dd') <= '${endDate}' </#if>
			<#if lastTimestamp?? >AND t_last_timestamp <= ${lastTimestamp}</#if>
		UNION 
		SELECT city_code, order_type, '1' num, pay_payment_type, 
				 COFFEE_UPDATE_DATE AS days, t_last_timestamp	
			FROM skp.order_info o	
			WHERE order_type = '5' and pay_state = '2' AND COFFEE_ORDER_STATE in('4','5') AND pay_payment_type is not null	
			<#if startDate??> AND DATE_FORMAT(COFFEE_UPDATE_DATE, 'yyyy-MM-dd') >= '${startDate}' </#if> 
			<#if endDate??> AND DATE_FORMAT(COFFEE_UPDATE_DATE, 'yyyy-MM-dd') <= '${endDate}' </#if>
			<#if lastTimestamp?? >AND t_last_timestamp <= ${lastTimestamp}</#if>
		) a 
) b GROUP BY city_code, days, payment_code