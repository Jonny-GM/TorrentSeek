# TorrentSeek installer for Windows (amd64).
#
#   irm https://raw.githubusercontent.com/Jonny-GM/TorrentSeek/main/install.ps1 | iex
#
# While the repository is private the raw URL above 404s; fetch and run
# through the gh CLI (https://cli.github.com, after `gh auth login`)
# instead — the script then also downloads the release itself through gh:
#
#   iex (@(gh api -H "Accept: application/vnd.github.raw" repos/Jonny-GM/TorrentSeek/contents/install.ps1) -join "`n")
#
# Or download it and run with options:
#   .\install.ps1 [-Version vX.Y.Z] [-Latest] [-Service] [-Uninstall]
#
# Installs to %LOCALAPPDATA%\TorrentSeek and adds it to the user PATH — no
# admin rights needed. -Service registers a logon Scheduled Task so the
# daemon starts when you sign in. Downloads are verified against the
# release's sha256sums.txt. Deluge 2.0+ (deluged with the PiecePriority
# plugin) must be installed separately — see the README's Requirements.
param(
    [string]$Version = "",
    [switch]$Latest,
    [switch]$Service,
    [switch]$Uninstall
)
$ErrorActionPreference = "Stop"

$Repo = "Jonny-GM/TorrentSeek"
$Dest = Join-Path $env:LOCALAPPDATA "TorrentSeek"
$TaskName = "TorrentSeek"

# Release downloads go through the gh CLI whenever it's installed and
# authenticated: mandatory while the repository is private (unauthenticated
# requests to a private repo's releases return 404), a free rate-limit
# bump once it's public.
$UseGh = $false
if (Get-Command gh -ErrorAction SilentlyContinue) {
    gh auth status *> $null
    if ($LASTEXITCODE -eq 0) { $UseGh = $true }
}

function Get-ReleaseAsset([string]$Tag, [string]$Name, [string]$Dir) {
    if ($UseGh) {
        gh release download $Tag --repo $Repo --pattern $Name --dir $Dir --clobber
        if ($LASTEXITCODE -ne 0) { throw "gh release download failed for $Name (release '$Tag')" }
    } else {
        Invoke-WebRequest "https://github.com/$Repo/releases/download/$Tag/$Name" `
            -OutFile (Join-Path $Dir $Name) -UseBasicParsing
    }
}

function Remove-FromUserPath {
    $path = [Environment]::GetEnvironmentVariable("Path", "User")
    $newPath = ($path -split ";" | Where-Object { $_ -and $_ -ne $Dest }) -join ";"
    if ($newPath -ne $path) {
        [Environment]::SetEnvironmentVariable("Path", $newPath, "User")
    }
}

if ($Uninstall) {
    Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue |
        Unregister-ScheduledTask -Confirm:$false
    if (Test-Path $Dest) { Remove-Item -Recurse -Force $Dest }
    Remove-FromUserPath
    Write-Host ">> TorrentSeek removed."
    return
}

# --- resolve release tag ------------------------------------------------------

if ($Latest) {
    $tag = "latest"
} elseif ($Version) {
    $tag = $Version
} else {
    # GitHub's releases/latest endpoint never returns prereleases, so the
    # rolling "latest" build can't satisfy it and the fallback is explicit.
    try {
        if ($UseGh) {
            $tag = gh api "repos/$Repo/releases/latest" --jq .tag_name
            if ($LASTEXITCODE -ne 0 -or -not $tag) { throw "no versioned release" }
        } else {
            $tag = (Invoke-RestMethod "https://api.github.com/repos/$Repo/releases/latest").tag_name
        }
    } catch {
        Write-Host ">> no versioned release found; installing the rolling latest build"
        $tag = "latest"
    }
}

# --- download and verify ------------------------------------------------------

$tmp = Join-Path $env:TEMP "torrentseek-install"
New-Item -ItemType Directory -Force $tmp | Out-Null

Write-Host ">> fetching checksums for release '$tag'"
try {
    Get-ReleaseAsset $tag "sha256sums.txt" $tmp
} catch {
    throw "release '$tag' not found (or it has no sha256sums.txt). If the repository is private, install and authenticate the gh CLI first (gh auth login). $_"
}
$sums = Get-Content (Join-Path $tmp "sha256sums.txt") -Raw
$line = $sums -split "`r?`n" | Where-Object { $_ -match "_windows_amd64\.zip" } | Select-Object -First 1
if (-not $line) { throw "release '$tag' has no windows_amd64 artifact" }
$parts = $line.Trim() -split "\s+", 2
$expected, $artifact = $parts[0], $parts[1].Trim()
$zip = Join-Path $tmp $artifact

Write-Host ">> downloading $artifact"
Get-ReleaseAsset $tag $artifact $tmp

$actual = (Get-FileHash -Algorithm SHA256 $zip).Hash.ToLower()
if ($actual -ne $expected.ToLower()) {
    throw "checksum mismatch for ${artifact}: expected $expected, got $actual"
}
Write-Host ">> checksum verified"

# --- install ------------------------------------------------------------------

$extract = Join-Path $tmp "extracted"
if (Test-Path $extract) { Remove-Item -Recurse -Force $extract }
Expand-Archive $zip -DestinationPath $extract
$exe = Get-ChildItem $extract -Recurse -Filter "torrentseek.exe" | Select-Object -First 1
if (-not $exe) { throw "archive did not contain torrentseek.exe" }

New-Item -ItemType Directory -Force $Dest | Out-Null
Copy-Item $exe.FullName (Join-Path $Dest "torrentseek.exe") -Force
# torrentprobe (the fetch/verify diagnostic tool) ships alongside torrentseek
# in newer releases; older archives won't have it, so this is best-effort.
$probe = Get-ChildItem $extract -Recurse -Filter "torrentprobe.exe" | Select-Object -First 1
if ($probe) { Copy-Item $probe.FullName (Join-Path $Dest "torrentprobe.exe") -Force }
Copy-Item (Join-Path $exe.DirectoryName "README.md") $Dest -Force -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force $tmp

$path = [Environment]::GetEnvironmentVariable("Path", "User")
if (($path -split ";") -notcontains $Dest) {
    [Environment]::SetEnvironmentVariable("Path", "$path;$Dest", "User")
    Write-Host ">> added $Dest to your user PATH (open a new terminal to pick it up)"
}
Write-Host ">> installed $(& (Join-Path $Dest 'torrentseek.exe') -version)"

# --- optional logon task ------------------------------------------------------

if ($Service) {
    $action = New-ScheduledTaskAction -Execute (Join-Path $Dest "torrentseek.exe")
    $trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
    Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Force | Out-Null
    Start-ScheduledTask -TaskName $TaskName
    Write-Host ">> logon task '$TaskName' registered and started"
}

Write-Host ">> done. TorrentSeek needs deluged (Deluge 2.0+) with the PiecePriority"
Write-Host ">> plugin reachable from this host — see the README's Requirements. Then:"
Write-Host ">> torrentseek -deluge-user <user> -deluge-pass <pass>    (API on http://127.0.0.1:3480, see torrentseek -h)"
