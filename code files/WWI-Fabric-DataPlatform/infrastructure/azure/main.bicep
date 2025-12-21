// =============================================================================
// WWI-Fabric-DataPlatform Azure Infrastructure
// Purpose: Deploy Azure resources using Infrastructure as Code
// =============================================================================

@description('The location for all resources')
param location string = resourceGroup().location

@description('Environment name (dev, test, prod)')
@allowed(['dev', 'test', 'prod'])
param environment string = 'dev'

@description('Project name prefix')
param projectName string = 'wwi'

@description('Tags to apply to all resources')
param tags object = {
  Project: 'WWI-Fabric-DataPlatform'
  Environment: environment
  ManagedBy: 'Bicep'
}

// =============================================================================
// Variables
// =============================================================================

var storageAccountName = 'st${projectName}${environment}${uniqueString(resourceGroup().id)}'
var keyVaultName = 'kv-${projectName}-${environment}'

// =============================================================================
// Storage Account (ADLS Gen2)
// =============================================================================

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: storageAccountName
  location: location
  tags: tags
  kind: 'StorageV2'
  sku: {
    name: 'Standard_LRS'
  }
  properties: {
    accessTier: 'Hot'
    isHnsEnabled: true  // Enable hierarchical namespace for ADLS Gen2
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
    allowBlobPublicAccess: false
    networkAcls: {
      defaultAction: 'Allow'
      bypass: 'AzureServices'
    }
  }
}

// Blob Services
resource blobServices 'Microsoft.Storage/storageAccounts/blobServices@2023-01-01' = {
  parent: storageAccount
  name: 'default'
  properties: {
    deleteRetentionPolicy: {
      enabled: true
      days: 7
    }
  }
}

// Container: Landing
resource containerLanding 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: blobServices
  name: 'landing'
  properties: {
    publicAccess: 'None'
  }
}

// Container: Archive
resource containerArchive 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: blobServices
  name: 'archive'
  properties: {
    publicAccess: 'None'
  }
}

// Container: External
resource containerExternal 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: blobServices
  name: 'external'
  properties: {
    publicAccess: 'None'
  }
}

// =============================================================================
// Key Vault (for storing secrets)
// =============================================================================

resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: keyVaultName
  location: location
  tags: tags
  properties: {
    tenantId: subscription().tenantId
    sku: {
      family: 'A'
      name: 'standard'
    }
    enabledForDeployment: false
    enabledForDiskEncryption: false
    enabledForTemplateDeployment: true
    enableSoftDelete: true
    softDeleteRetentionInDays: 7
    enableRbacAuthorization: true
    networkAcls: {
      defaultAction: 'Allow'
      bypass: 'AzureServices'
    }
  }
}

// =============================================================================
// Outputs
// =============================================================================

@description('Storage account name')
output storageAccountName string = storageAccount.name

@description('Storage account primary endpoint')
output storageAccountEndpoint string = storageAccount.properties.primaryEndpoints.dfs

@description('Key Vault name')
output keyVaultName string = keyVault.name

@description('Key Vault URI')
output keyVaultUri string = keyVault.properties.vaultUri
