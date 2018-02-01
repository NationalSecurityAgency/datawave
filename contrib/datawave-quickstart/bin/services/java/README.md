
The Java bootstrap is a bit different than those used for other services:

1) It will ensure that a valid JDK 1.8 is configured for the runtime environment,
   such that if JAVA_HOME is already set in the environment, and if JAVA_HOME
   points to a valid 1.8 JDK, then the existing installation will be used to build
   and execute other services. Otherwise, a fresh 1.8 JDK (tar.gz) will be downloaded
   from Oracle and installed/configured under `**/bin/services/java`

2) It uses no-op implementations for required functions such as `javaStart` and `javaStop`
