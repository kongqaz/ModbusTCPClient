' 使用WMI创建进程，以便获取真实的PID
Set objWMIService = GetObject("winmgmts:\\.\root\cimv2")
Set objStartup = objWMIService.Get("Win32_ProcessStartup")
Set objConfig = objStartup.SpawnInstance_()
objConfig.ShowWindow = 0 ' 隐藏窗口运行

' 创建进程并获取PID
Dim intProcessID, objProcess
Set objProcess = objWMIService.Get("Win32_Process")
errReturn = objProcess.Create("java -jar ModbusTCPClient-1.0-SNAPSHOT.jar 8018", _
                             "D:\ModbusTCPClient", _
                             objConfig, _
                             intProcessID)

' 检查进程是否成功创建
If errReturn = 0 Then
    ' 将真实的进程ID写入文件
    Set fso = CreateObject("Scripting.FileSystemObject")
    Set pidFile = fso.CreateTextFile("pid.txt", True)
    pidFile.Write intProcessID
    pidFile.Close
    Set fso = Nothing
Else
    WScript.Echo "创建进程失败，错误代码: " & errReturn
End If

Set objWMIService = Nothing
Set objStartup = Nothing
Set objConfig = Nothing
Set objProcess = Nothing
