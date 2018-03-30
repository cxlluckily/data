select city_code, days, count(pay_account) from (

		select t.city_code, t.days, t.pay_account from (
			SELECT t1.*, row_number() over (PARTITION BY t1.CITY_CODE, t1.PAY_ACCOUNT ORDER BY t1.days) AS rn FROM (
				SELECT  CITY_CODE, 
					PAY_PAY_ACCOUNT PAY_ACCOUNT,
				 	DATE_FORMAT(PAY_PAY_TIME, 'yyyy-MM-dd') days 
				FROM skp.order_info
				WHERE 
					PAY_PAY_ACCOUNT is not null
					AND pay_pay_time is not null
					AND city_code is not null	
					AND pay_state = 2
			) t1 		
		) t where t.rn = 1 
		<#if startDate??> AND days >= ${startDate} </#if> 
		<#if endDate??> AND days <= ${endDate} </#if> 				
	
) b group by city_code, days