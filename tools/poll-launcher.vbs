' Hidden launcher for the PowerShell poll wrapper.
' Task Scheduler invokes this via wscript.exe; wscript launches the wrapper
' with windowStyle=0 (hidden), so no console window flashes on the desktop.
'
' Argument (optional): "snapshot-diff" to run the snapshot-diff sweep instead
' of the default heartbeat-and-changes cycle.
'
' Usage: wscript.exe poll-launcher.vbs [snapshot-diff]

Dim shell, mode, cmd
Set shell = CreateObject("WScript.Shell")

If WScript.Arguments.Count > 0 Then
  mode = WScript.Arguments(0)
Else
  mode = ""
End If

cmd = "powershell.exe -NoProfile -ExecutionPolicy Bypass -File ""C:\Repos\lag-observer\tools\poll-wrapper.ps1"""
If mode <> "" Then
  cmd = cmd & " " & mode
End If

' Run hidden (0), don't wait for completion (False).
shell.Run cmd, 0, False
