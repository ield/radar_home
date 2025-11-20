# CONFIG
$remoteUser = "gmr-lab"
$remoteHost = "100.83.116.1"
$localPath  = "D:/GMR/Radar_24GHz_Infineon/medidas_home/raw_meas/"

# Ensure local folder exists
if (-not (Test-Path $localPath)) {
    New-Item -ItemType Directory -Path $localPath | Out-Null
}

# Get list of remote BIN files
Write-Host "Retrieving file list from Raspberry Pi..."
$remoteFilesRaw = ssh "$remoteUser@$remoteHost" 'ls -1t /home/gmr-lab/Desktop/radar_home/meas/*.bin'

# Split lines and trim
$remoteFiles = $remoteFilesRaw -split "`n" | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" }

# Today's date in YYYYMMDD format
$todayString = (Get-Date).ToString("yyyyMMdd")

# Filter files created before today
$filesBeforeToday = $remoteFiles | Where-Object {
    $filename = [System.IO.Path]::GetFileName($_)
    if ($filename -match '\d{14}(?=\.bin$)') {
        $fileDate = $Matches[0].Substring(0,8)
        [int]$fileDate -lt [int]$todayString
    } else {
        $false
    }
}

# Show what we got
if (-not $filesBeforeToday) {
    Write-Host "No files found on remote."
    exit
}

# Determine which files need to be copied
$filesToCopy = $filesBeforeToday

Write-Host "$($filesToCopy.Count) remote files found."

$totalFiles = $filesToCopy.Count
$counter = 0

# Copy files one by one and delete on success
foreach ($file in $filesToCopy) {
    $counter++
    $filename = [System.IO.Path]::GetFileName($file)
    $remoteFilePath = $remoteUser + "@" + $remoteHost + ":`"" + $file + "`""

    # Show progress bar
    Write-Progress -Activity "Transferring files from $remoteHost" `
                   -Status "Copying $filename ($counter of $totalFiles)" `
                   -PercentComplete (($counter / $totalFiles) * 100)

    scp -q $remoteFilePath $localPath

    if ($LASTEXITCODE -ne 0) {
        Write-Host "Failed to copy $filename. Skipping."
    }
}

# Delete all copied files in one SSH command
$remoteFilesToDelete = ($filesToCopy | ForEach-Object { "`"$_`"" }) -join " "
Write-Host "Deleting transferred files from Raspberry Pi..."
ssh -n -o BatchMode=yes $remoteUser@$remoteHost "rm $remoteFilesToDelete"

Write-Host "Sync complete."
