@echo off & setlocal enabledelayedexpansion

title statement-ct

set JAVA_OPTS=-Xms64m -Xmx512m -XX:MaxPermSize=128M

set MAIN_CLASS=com.cssweb.statement.Main

set CLASSPATH=..\conf;..\classes
cd ..\lib
for %%i in (*) do set CLASSPATH=!CLASSPATH!;..\lib\%%i
cd ..\bin

if ""%1"" == ""debug"" goto debug
if ""%1"" == ""jmx"" goto jmx

java %JAVA_OPTS% -classpath %CLASSPATH% %MAIN_CLASS%
goto end

:debug
java %JAVA_OPTS% -Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=8000,server=y,suspend=n -classpath %CLASSPATH% %MAIN_CLASS%
goto end

:jmx
java %JAVA_OPTS% -Dcom.sun.management.jmxremote.port=1099 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -classpath %CLASSPATH% %MAIN_CLASS%

:end
pause