select *
from shankephone.device_status
<#if (starttime??) && (endtime??)>
	where concat(update_date,update_time)>${starttime} and concat(update_date,update_time)<${endtime}
	<#if select_city_code ??>
		and city_code=${select_city_code}
	</#if>	
</#if>
