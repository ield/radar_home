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

# Show what we got
if (-not $remoteFilesRaw) {
    Write-Host "No files found on remote."
    exit
}
Write-Host "$($remoteFiles.Count) remote files found."

$remoteFiles = $remoteFilesRaw -split "`n" | ForEach-Object { $_.Trim() } | Where-Object { $_ }

# Get list of local BIN filenames
$localFiles = Get-ChildItem $localPath -Filter *.bin | ForEach-Object { $_.Name }

# Determine which files need to be copied
$filesToCopy = $remoteFiles | Where-Object { $localFiles -notcontains ([System.IO.Path]::GetFileName($_)) }

if (-not $filesToCopy) {
    Write-Host "No new files to transfer."
    exit
}

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
