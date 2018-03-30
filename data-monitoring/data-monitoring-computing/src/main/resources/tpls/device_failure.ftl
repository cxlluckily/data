select 
	s.city_code,
    s.device_id, 
    s.status_id, 
    s.status_value, 
    f.update_date_cur,
    f.update_date_pre,
    f.failure_time,
    f.recover_time
from sub_device_status s, 
	(
		 select
		        m.city_code,
		        m.device_id,
		        m.update_date_cur,
		        m.update_date_pre,
		        case when m.status_value_pre=0 then m.update_date_cur when m.status_value_pre_pre=0 then m.update_date_pre else null end failure_time,
		        m.update_date_next recover_time
		 from(
		         select
		                d.city_code,
		                d.device_id,
		                d.status_value_pre,
		                d.status_value_next,
		                d.update_date_cur,
		                d.update_date_next,
		                LAG(d.status_value_pre,1) over w status_value_pre_pre,
		                LAG(d.update_date_cur,1) over w update_date_pre
		         from(
		                select
		                        t.city_code,
		                        t.device_id,
		                        t.status_value,
		                        LAG(t.status_value,1) over w status_value_pre,
		                        LEAD(t.status_value,1) over w status_value_next,
		                        concat(t.update_date,t.update_time) update_date_cur,
		                        LEAD(concat(t.update_date,t.update_time),1) over w update_date_next
		                from (select * from sub_device_status where status_id='0000') t
		                WINDOW w AS (partition by t.city_code,t.device_id order by t.update_date,t.update_time) 
		         ) d
		         where (d.status_value<>0 and (d.status_value_next=0 or d.status_value_next is null)) or (d.status_value<>0 and (d.status_value_pre=0 or d.status_value_pre is null))
		         WINDOW w AS (partition by d.city_code,d.device_id order by d.update_date_cur) 
		 ) m
		 where status_value_next = 0 or status_value_next is null
	) f
where 
	s.city_code = f.city_code
	and s.device_id = f.device_id
	and concat(s.update_date, s.update_time) = f.update_date_cur
	and s.status_value <> 0