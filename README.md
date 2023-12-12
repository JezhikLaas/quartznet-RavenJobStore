[![build and test](https://github.com/JezhikLaas/quartznet-RavenJobStore/actions/workflows/build-and-test.yml/badge.svg)](https://github.com/JezhikLaas/quartznet-RavenJobStore/actions/workflows/build-and-test.yml)

[![Qodana](https://github.com/JezhikLaas/quartznet-RavenJobStore/actions/workflows/code_quality.yml/badge.svg)](https://github.com/JezhikLaas/quartznet-RavenJobStore/actions/workflows/code_quality.yml)


# Quartz.NET-RavenJobStore
Not exactly a fork of, but inspired by [Quartz.NET-RavenDB](https://github.com/ravendb/quartznet-RavenDB)

First prerelease available on nuget.

## Project goals
* Can handle high volumes of jobs and triggers.
* Suitable for clustered schedulers.
* Different schedulers (instance names) can share the same database.

## How to get started
No docs right now, please see the example for now.

### To install the package

`dotnet add package Domla.Quartz.RavenJobStore --version 0.5.0-prerelease`