select 
     t.city_code,
     t.device_id,
     from_unixtime((unix_timestamp(t.beat_date) + ${TTL_SECOND})) failure_date,
     case when t.beat_status_next = 0 then beat_date_next1 else beat_date_next2 end recovery_date
from
(
        select 
                t.*,
                LEAD(t.beat_date_next1, 1) over w beat_date_next2
        from 
        (
                select 
                      t.*,
                      SUM(t.beat_status) over (partition by t.city_code,t.device_id order by t.beat_date ROWS BETWEEN ${RECOVER_TIMES} PRECEDING AND 1 PRECEDING) beat_status_pre,
                      SUM(t.beat_status) over (partition by t.city_code,t.device_id order by t.beat_date ROWS BETWEEN 1 FOLLOWING AND ${RECOVER_TIMES} FOLLOWING) beat_status_next,
                      LAST_VALUE(t.beat_date) over (partition by t.city_code,t.device_id order by t.beat_date ROWS BETWEEN 1 FOLLOWING AND ${RECOVER_TIMES} FOLLOWING) beat_date_next1
                from 
                (
                        select 
                                t.city_code,
                                t.device_id,
                                t.beat_date,
                                -- LEAD(t.beat_date, 1) over w beat_date_next,
                                -- unix_timestamp(LEAD(t.beat_date, 1) over w) - unix_timestamp(t.beat_date) beat_interval,
                                case when (unix_timestamp(LEAD(t.beat_date, 1) over w) - unix_timestamp(t.beat_date)) > ${TTL_SECOND} then 1 else 0 end beat_status
                        from 
                        (
                                select * 
                                from shankephone.device_heart_beat 
                                <#if (starttime??) && (endtime??)>
									where beat_date>${starttime} and beat_date<${endtime}
									<#if select_city_code ??>
										and city_code=${select_city_code}
									</#if>	
								</#if>
                        ) t
                        WINDOW w AS (partition by t.city_code,t.device_id order by t.beat_date)
                ) t
        ) t
        where t.beat_status = 1 and (t.beat_status_pre = 0 or t.beat_status_next = 0)
        WINDOW w AS (partition by t.city_code,t.device_id order by t.beat_date)
) t
where t.beat_status_pre = 0