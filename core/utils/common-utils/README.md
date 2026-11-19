# Common-Utils Library

[![Apache License][li]][ll] ![Build Status](https://github.com/NationalSecurityAgency/datawave-common-utils/workflows/Tests/badge.svg)

This library holds low-level common utility classes that are used by iterators
on tablet servers, microservices, and the legacy Wildfly web service.

**WARNING:** Think very carefully about adding any dependencies to this
library. Only low-level additions with little or no external dependencies
should be added. Given that this library will be used system-wide at all
layers (see above), any changes either cause many components to themselves
be updated to use the new version, or they cause the evaluation by many
components to determine whether or not an upgrade is required.

## Choosing a visibility provider

`common-utils` defines `datawave.visibility.VisibilityFunctions` but does not
register a provider. The first call to `getInstance()` requires exactly one
registration visible to the class loader of the SPI. Missing, duplicate
(different implementations), and broken registrations are configuration errors.

Stock distributions select `gov.nsa.datawave.core:visibility-provider-standard`
at packaging time for the historical AND and cached-flatten behavior. Shared
DataWave libraries do not export that dependency. A consuming application can
instead deploy a provider JAR containing
`META-INF/services/datawave.visibility.VisibilityFunctions`, with the fully
qualified implementation class name on one line. No system property is needed
or used. When customizing a stock distribution, **replace** the stock provider
rather than installing both. Do this on tablet servers as well as clients; for
stock ingest scripts, `DATAWAVE_VISIBILITY_PROVIDER_JAR` can point to the
replacement on the launcher and distributed job classpaths.

[li]: http://img.shields.io/badge/license-ASL-blue.svg
[ll]: https://www.apache.org/licenses/LICENSE-2.0