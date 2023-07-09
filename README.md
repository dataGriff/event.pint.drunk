# Event Pint Drunk

* booze.cdc.pint

dotnet new console -n "pintdrinker" 

cd pintdrinker

dotnet build

dotnet run

dotnet add package Azure.Messaging.EventHubs
dotnet add package CloudNative.CloudEvents   
dotnet add package Microsoft.Extensions.Configuration.Json
dotnet add package Microsoft.Extensions.Configuration.EnvironmentVariables
dotnet add package Newtonsoft.Json

what .net framework I have

reg query "HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\NET Framework Setup\NDP\v4\Full" /v Release

## Cosmos Publisher

dotnet add package Microsoft.Azure.Cosmos
