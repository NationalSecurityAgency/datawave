
The Maven bootstrap is a bit different than those used for other services:

1) It will ensure that a valid Apache Maven 3.x is configured for your environment,
   such that if MAVEN_HOME and/or M2_HOME is already established, and that *_HOME
   contains a 3.x 'mvn' executable, then the existing installation will be used to
   build DataWave. Otherwise, a fresh Maven tarball will be downloaded from a Maven
   download mirror and installed/configured under `**/bin/services/maven`

2) It uses no-op implementations for required functions such as `mavenStart` and `mavenStop`
