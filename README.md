# <img src="https://uploads-ssl.webflow.com/5ea5d3315186cf5ec60c3ee4/5edf1c94ce4c859f2b188094_logo.svg" alt="Pip.Services Logo" width="200"> <br/> Azure specific components .NET

This module is a part of the [Pip.Services](http://pipservices.org) polyglot microservices toolkit.
The module contains components for working with the Microsoft Azure cloud service.

- NOTE: since version 3.5.0 .NET below 6.0 has been deprecated. 
- More details: https://learn.microsoft.com/en-us/lifecycle/products/microsoft-net-and-net-core

The module contains the following packages:

- **Auth** - KeyVault credential provider
- **Build** - Standard factories for designing Azure module components
- **Connect** - Components for creating and configuring a connection to Azure
- **Config** - KeyVault secure config reader
- **Count** - AppInsights performance counter component
- **Lock** - Components of working with locks in the Cloud Storage Table
- **Log** - AppInsights logger components
- **Metrics** - Components of working with CosmosDB SQL Metrics
- **Persistence** - Components of working with data in CosmosDB
- **Queues** - Azure Storage and Azure Service Bus message queues
- **Run** - Components of the process container for the Azure environment

<a name="links"></a> Quick links:

* [API Reference](https://pip-services3-dotnet.github.io/pip-services3-azure-dotnet/)
* [Change Log](CHANGELOG.md)
* [Get Help](http://docs.pipservices.org/get_help/)
* [Contribute](http://docs.pipservices.org/contribute/)


## Use

Install the dotnet package as
```bash
dotnet add package PipServices3.Azure
```

## Develop

For development you shall install the following prerequisites:
* Core .NET SDK 3.1+
* Visual Studio Code or another IDE of your choice
* Docker

Restore dependencies:
```bash
dotnet restore src/src.csproj
```

Compile the code:
```bash
dotnet build src/src.csproj
```

Run automated tests:
```bash
dotnet restore test/test.csproj
dotnet test test/test.csproj
```

Generate API documentation:
```bash
./docgen.ps1
```

Before committing changes run dockerized build and test as:
```bash
./build.ps1
./test.ps1
./clear.ps1
```

## Contacts

The .NET version of Pip.Services is created and maintained by:
- **Sergey Seroukhov**

Many thanks to contibutors, who put their time and talant into making this project better:
- **Andrew Harrington**, Kyrio
