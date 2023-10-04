@ echo off
setlocal

 curl http://localhost:8000/shutdown
 curl http://localhost:8001/shutdown

 curl http://localhost:9000/shutdown
 curl http://localhost:9001/shutdown
 curl http://localhost:9002/shutdown
 curl http://localhost:9003/shutdown

pause
endlocal
