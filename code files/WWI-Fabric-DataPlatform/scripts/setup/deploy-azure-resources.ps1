# =============================================================================
# WWI-Fabric-DataPlatform Setup Script (Windows)
# Purpose: Automate Azure resource deployment
# =============================================================================

param(
    [Parameter(Mandatory=$false)]
    [string]$Environment = "dev",
    
    [Parameter(Mandatory=$false)]
    [string]$Location = "eastus",
    
    [Parameter(Mandatory=$false)]
    [string]$ResourceGroupName = "rg-wwi-fabric-platform"
)

Write-Host "================================================" -ForegroundColor Cyan
Write-Host "WWI-Fabric-DataPlatform Azure Setup" -ForegroundColor Cyan
Write-Host "================================================" -ForegroundColor Cyan
Write-Host ""

# Check if Azure CLI is installed
try {
    $azVersion = az version --output json | ConvertFrom-Json
    Write-Host "Azure CLI installed: $($azVersion.'azure-cli')" -ForegroundColor Green
} catch {
    Write-Host "Azure CLI not found. Please install it first:" -ForegroundColor Red
    Write-Host "  https://docs.microsoft.com/en-us/cli/azure/install-azure-cli" -ForegroundColor Yellow
    exit 1
}

# Check login status
Write-Host ""
Write-Host "Checking Azure login status..." -ForegroundColor Yellow
$account = az account show --output json 2>$null | ConvertFrom-Json

if (-not $account) {
    Write-Host "Not logged in. Starting login..." -ForegroundColor Yellow
    az login
    $account = az account show --output json | ConvertFrom-Json
}

Write-Host "Logged in as: $($account.user.name)" -ForegroundColor Green
Write-Host "  Subscription: $($account.name)" -ForegroundColor Gray
Write-Host ""

# Confirm subscription
$confirm = Read-Host "Continue with this subscription? (y/n)"
if ($confirm -ne "y") {
    Write-Host "Available subscriptions:" -ForegroundColor Yellow
    az account list --output table
    $subName = Read-Host "Enter subscription name or ID"
    az account set --subscription $subName
}

# Create Resource Group
Write-Host ""
Write-Host "Creating Resource Group: $ResourceGroupName" -ForegroundColor Yellow

$rgExists = az group exists --name $ResourceGroupName
if ($rgExists -eq "true") {
    Write-Host "Resource Group already exists" -ForegroundColor Green
} else {
    az group create `
        --name $ResourceGroupName `
        --location $Location `
        --tags Project=WWI-Fabric-DataPlatform Environment=$Environment
    Write-Host "Resource Group created" -ForegroundColor Green
}

# Deploy Bicep template
Write-Host ""
Write-Host "Deploying Azure resources with Bicep..." -ForegroundColor Yellow

$bicepFile = "infrastructure/azure/main.bicep"
$parametersFile = "infrastructure/azure/parameters/$Environment.parameters.json"

if (Test-Path $bicepFile) {
    $deploymentName = "wwi-deploy-$(Get-Date -Format 'yyyyMMdd-HHmmss')"
    
    Write-Host "  Template: $bicepFile" -ForegroundColor Gray
    Write-Host "  Parameters: $parametersFile" -ForegroundColor Gray
    
    $deployment = az deployment group create `
        --name $deploymentName `
        --resource-group $ResourceGroupName `
        --template-file $bicepFile `
        --parameters "@$parametersFile" `
        --output json | ConvertFrom-Json
    
    if ($deployment) {
        Write-Host "Deployment completed successfully" -ForegroundColor Green
        Write-Host ""
        Write-Host "Deployment Outputs:" -ForegroundColor Cyan
        Write-Host "  Storage Account: $($deployment.properties.outputs.storageAccountName.value)" -ForegroundColor Gray
        Write-Host "  Storage Endpoint: $($deployment.properties.outputs.storageAccountEndpoint.value)" -ForegroundColor Gray
        Write-Host "  Key Vault: $($deployment.properties.outputs.keyVaultName.value)" -ForegroundColor Gray
    } else {
        Write-Host "Deployment failed" -ForegroundColor Red
    }
} else {
    Write-Host "Bicep template not found at: $bicepFile" -ForegroundColor Red
    Write-Host "  Please run this script from the project root directory" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "================================================" -ForegroundColor Cyan
Write-Host "Setup Complete!" -ForegroundColor Cyan
Write-Host "================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next Steps:" -ForegroundColor Yellow
Write-Host "1. Note the Storage Account name from outputs above" -ForegroundColor Gray
Write-Host "2. Configure Microsoft Fabric workspace" -ForegroundColor Gray
Write-Host "3. Set up Self-Hosted Integration Runtime" -ForegroundColor Gray
Write-Host "4. Connect Fabric workspace to Git" -ForegroundColor Gray
Write-Host ""
Write-Host "See docs/runbooks/environment-setup.md for detailed instructions" -ForegroundColor Cyan
