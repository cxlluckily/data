select device_id, city_code
from shankephone.device_info
 <#if select_city_code??>  where city_code = ${select_city_code} </#if>
