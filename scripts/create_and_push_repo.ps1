<#
Helper PowerShell script: create GitHub repo and push current project using gh CLI
Usage: .\create_and_push_repo.ps1 -RepoOwnerAndName 'YOUR_USER/REPO_NAME'
#>
param(
  [Parameter(Mandatory=$true)]
  [string]$RepoOwnerAndName
)

# Ensure gh is available
if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
  Write-Error "GitHub CLI 'gh' not found. Install and run 'gh auth login' first."
  exit 1
}

Write-Host "Creating repo $RepoOwnerAndName and pushing current code..."
gh repo create $RepoOwnerAndName --public --source=. --remote=origin --push
Write-Host "Repository created and pushed. Add secrets: DOCKERHUB_USERNAME, DOCKERHUB_TOKEN, (optional) KUBE_CONFIG_DATA"
