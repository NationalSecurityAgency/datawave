# In-Memory Accumulo

[![Apache License][li]][ll] ![Build Status](https://github.com/NationalSecurityAgency/datawave-in-memory-accumulo/workflows/Tests/badge.svg)

In-Memory Accumulo is a simple in-memory implementation of Accumulo.
This does not attempt to replicate all features of Accumulo but rather
enough to use for executing unit tests that need to interact with
Accumulo (where starting Mini Accumulo Cluster might take too long) or
for use as an in-memory local cache fronting a remote Accumulo server.

[li]: http://img.shields.io/badge/license-ASL-blue.svg
[ll]: https://www.apache.org/licenses/LICENSE-2.0