' ʹ��WMI�������̣��Ա��ȡ��ʵ��PID
Set objWMIService = GetObject("winmgmts:\\.\root\cimv2")
Set objStartup = objWMIService.Get("Win32_ProcessStartup")
Set objConfig = objStartup.SpawnInstance_()
objConfig.ShowWindow = 0 ' ���ش�������

' �������̲���ȡPID
Dim intProcessID, objProcess
Set objProcess = objWMIService.Get("Win32_Process")
errReturn = objProcess.Create("java -jar ModbusTCPClient-1.0-SNAPSHOT.jar 8018", _
                             "D:\ModbusTCPClient", _
                             objConfig, _
                             intProcessID)

' �������Ƿ�ɹ�����
If errReturn = 0 Then
    ' ����ʵ�Ľ���IDд���ļ�
    Set fso = CreateObject("Scripting.FileSystemObject")
    Set pidFile = fso.CreateTextFile("pid.txt", True)
    pidFile.Write intProcessID
    pidFile.Close
    Set fso = Nothing
Else
    WScript.Echo "��������ʧ�ܣ��������: " & errReturn
End If

Set objWMIService = Nothing
Set objStartup = Nothing
Set objConfig = Nothing
Set objProcess = Nothing
