$Files = get-childitem 'C:\ady\landingzone' | Where-Object PSIsContainer -eq $false
$LimitTime = (Get-Date).AddHours(-6)
$Files | ForEach-Object {
    if ($_.LastWriteTime -lt $LimitTime) {
    Remove-Item -Path $_.FullName -Force
     }
}