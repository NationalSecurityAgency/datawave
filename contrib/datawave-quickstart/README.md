# DataWave Quickstart

### Purpose

* This quickstart is intended to speed up your development workflow by automating
the steps that you'd otherwise need to perform manually to configure, build, deploy, 
and test a standalone DataWave environment

* It includes setup and tear-down automation for DataWave, Hadoop, Accumulo,
ZooKeeper, etc, and provides convenience methods for testing DataWave's
ingest and query components

* For simplicity, all cluster services are downloaded/installed automatically,
they are extracted and installed under the quickstart home directory, and all
are owned/executed by the current user

* Docker alternative: rather than installing and running services locally, you may prefer
to run the entire [quickstart environment as a Docker container](docker/README.md). This will
provide the exact same capabilities described below, but hosted on a CentOS 7 base image.

---

### Get Started

DataWave Quickstart installation instructions are [here](https://code.nsa.gov/datawave/docs/quickstart)

--- 
### A note on building:

You can specify to use maven to fetch the DataWave, Hadoop, Accumulo, ZooKeeper tar.gz files
by using the `-Dquickstart-maven` flag with your mvn command.  This will use maven to 
download the tar files to your local repository then copy them to the appropriate directory
after downloading.

In order to deploy a new dependency, you can use a command similar to the following, using
Acccumulo-2.1.3 as an example. The command must be run from the same directory as the tar file.

`mvn deploy:deploy-file -DrepositoryId=github-datawave -Durl=https://maven.pkg.github.com/NationalSecurityAgency/datawave -DgroupId=gov.nsa.datawave.quickstart -DartifactId=accumulo -Dversion=2.1.3 -Dfile=accumulo-2.1.3-bin.tar.gz -Dpackaging=tar.gz`
