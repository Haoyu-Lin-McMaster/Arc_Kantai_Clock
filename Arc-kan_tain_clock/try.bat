echo off
if "%1"=="hide" goto CmdBegin
start mshta vbscript:createobject("wscript.shell").run("""%~0"" hide",0)(window.close)&&exit
:CmdBegin
C:
cd C:\Users\Bill Lin\Desktop\Arc-kan_tain_clock\Arc-kan_tain_clock
call py Main.py