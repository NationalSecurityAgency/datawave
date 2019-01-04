# Common-Utils Library

This library holds low-level common utility classes that are used by iterators
on tablet servers, microservices, and the legacy Wildfly web service.

**WARNING:** Think very carefully about adding any dependencies to this
library. Only low-level additions with little or no external dependencies
should be added. Given that this library will be used system-wide at all
layers (see above), any changes either cause many components to themselves
be updated to use the new version, or they cause the evaluation by many
components to determine whether or not an upgrade is required.
