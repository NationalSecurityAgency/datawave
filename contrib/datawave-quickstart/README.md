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

### Prerequisites

* None, other than Linux, Bash, and an Internet connection to `wget` tarballs

* JDK 1.8 and Maven 3.x are required. However, if it is determined that they are
*not* already installed/configured in your environment, they'll be downloaded and
installed automatically under `datawave-quickstart/bin/services/java` and `datawave-quickstart/bin/services/maven`
respectively

* To date, this quickstart has been tested on CentOS 6x, 7x and Ubuntu 16x. Any
Linux with at least Bash 4x will probably suffice

---

### Recommended (minimum) system configuration

* RAM: 8GB, CPU: 1 x quad-core
* Storage: To install everything, you'll need at least a few GB free
* ulimit -u (max user processes): 32768
* ulimit -n (open files): 32768
* vm.swappiness: 0

---

### Get started

The quickstart is comprised primarily of ` ../{serviceName}/boostrap.sh ` scripts. These define supporting bash variables
and bash functions, encapsulating both configuration and key functionality for each service in a consistent manner.

All boostrap scripts are sourced into your environment via [bin/env.sh](bin/env.sh), which itself should be sourced in your
` ~/.bashrc ` as described below. 

Note that the four bash commands shown in **Step 1** below will complete the entire setup process, but first you may want to read through subsequent sections to better understand how the setup works and how to customize it for your own preferences
 
1. Update your ` ~/.bashrc ` as indicated below and then run the commands that follow to bootstrap and install all services...

    ```bash
    $ echo "source /path/to/datawave-quickstart/bin/env.sh" >> ~/.bashrc  # Step 1
    $ source ~/.bashrc                                                    # Step 2
    $ allInstall                                                          # Step 3
    $ datawaveWebStart && datawaveWebTest                                 # Step 4
    # Setup is now complete
    ```
    If you happen to update ` ~/.bashrc ` via some other method, such as with **vi** or other, beware that the
    ` source /path/to/datawave-quickstart/bin/env.sh ` line should be placed **after** any existing Hadoop-, Accumulo-, 
    Wildfly-, or ZooKeeper-related variables you may have defined, in order to prevent those existing configs from 
    interfering with the quickstart install. Making it the last line in the file will mitigate most conflicts

2. After executing ` source ~/.bashrc ` as shown above, tarballs for registered services (Hadoop, 
   Accumulo, ZooKeeper, Wildfly, etc) will begin downloading automatically, and the DataWave build will be started 
   automatically in turn. This can take a while to complete, so this is a good time for a coffee break

    * It's also easy to affect which version of a binary gets downloaded/installed for a given service. Just override 
    the default ` DW_**_DIST_URI ` value defined in the service's ` bootstrap.sh ` before performing this step
      
      For example...
      ```bash
      $ vi ~/.bashrc

          ...
  
          export DW_ACCUMULO_DIST_URI=http://my.favorite.apache.mirror/accumulo/1.8.X/accumulo-1.8.X-bin.tar.gz
          export DW_JAVA_DIST_URI=file:///my/local/binaries/jdk-8-linux-x64.tar.gz
          export DW_HADOOP_DIST_URI=file:///my/local/binaries/hadoop-2.6.0-cdh5.9.1.tar.gz
          export DW_ZOOKEEPER_DIST_URI=file:///my/local/binaries/zookeeper-3.4.5-cdh5.9.1.tar.gz
  
          source /path/to/datawave-quickstart/bin/env.sh
    
      $ source ~/.bashrc
      ``` 

    * As for DataWave's ingest and web service binaries, a Maven build is kicked off automatically and the
      respective tarballs will be moved into place upon conclusion of the build
      
    * The ` DW_DATAWAVE_BUILD_COMMAND ` variable in [datawave/bootstrap.sh](bin/services/datawave/bootstrap.sh) defines
      the Maven command used for the build. It may be overridden in ` ~/.bashrc ` or from the command line as needed
      
3.  When tarball downloads are finished and the DataWave build has completed, the ` allInstall ` command
    will initialize and configure all services...
      
    * Alternatively, individual services may be installed one at a time, if desired, using ` {serviceName}Install `

    * All binaries are extracted/installed under [datawave-quickstart/bin/services](bin/services)
    
    * As part of the installation of DataWave Ingest, example data will be ingested automatically via M/R for demonstration purposes
    
    * Example datasets in JSON, CSV, and XML formats are among those ingested automatically
      
4. Lastly, ` datawaveWebStart && datawaveWebTest ` will start up DataWave Web and run preconfigured [tests](bin/services/datawave/test-web/tests) against DataWave's REST API. Note any test failures, if present, and check logs for error messages.
   
    * Troubleshooting help:
    
        * Get help on web test options: ` datawaveWebTest --help `
        * Rerun web tests with more output: ` datawaveWebTest --verbose --pretty-print `
        * Wildfly/DataWave Web logs: ` datawave-quickstart/wildfly/standalone/log `
        * Yarn/MR logs: ` datawave-quickstart/data/hadoop/yarn/log `
        * DataWave Ingest logs: ` datawave-quickstart/datawave-ingest/logs `

    * Note that an individual service may be started at any time with its associated ` {serviceName}Start ` command
    
    * Moreover, using a DataWave-specific command, such as ` datawaveWebStart ` for starting DataWave web services 
      in Wildfly, will automatically start up service dependencies, if necessary, such as Hadoop, Accumulo, ZooKeeper, etc

    * Alternatively, you may execute the ` allStart ` command to bring up all services in the proper order

5. Check the status of your services...

   * Execute the ` allStatus ` command or use ` {serviceName}Status ` for a given service to display its associated PID(s)
   
   * Try out the following queries from the command line and inspect the results...
   ```bash
   # Find some Wikipedia data based on page title, with --verbose flag to see the CURL command used...

   $ datawaveQuery --query "PAGE_TITLE:AccessibleComputing OR PAGE_TITLE:Anarchism" --verbose

   # TV show data from API.TVMAZE.COM (graph edge queries)

   $ datawaveQuery --logic EdgeQuery --syntax JEXL --query "SOURCE == 'kevin bacon' && TYPE == 'TV_COSTARS'" --pagesize 30
   $ datawaveQuery --logic EdgeQuery --syntax JEXL --query "SOURCE == 'william shatner' && TYPE == 'TV_CHARACTERS'"
   $ datawaveQuery --logic EdgeQuery --syntax JEXL --query "SOURCE == 'westworld' && TYPE == 'TV_SHOW_CAST'"  --pagesize 20

   # Check out all 'datawaveQuery' function options, inspect default parameter values, etc...

   $ datawaveQuery --help
   ```
   
   * Visit any of the following in your browser
       * Hadoop HDFS:  <http://localhost:50070/dfshealth.html#tab-overview>
       * Hadoop M/R:   <http://localhost:8088/cluster>
       * Accumulo:     <http://localhost:9995>
       * DataWave Web: <https://localhost:8443/DataWave/doc>
   
---

### Stop services, uninstall services, reset the environment, etc

1. To stop all services gracefully, execute the ` allStop ` command. Likewise, you may stop individual services
   with the associated ` {serviceName}Stop ` command
   
2. ` allStop --hard ` will bring down all services via ` kill -9 ` with no prompting

3. To tear down all services and remove *all* associated data directories, execute the ` allUninstall [ --remove-binaries, -rb ]`
   command. Individual services may be uninstalled at any time with ` {serviceName}Uninstall [ --remove-binaries, -rb ]`
   
4. To re-source all bootstrap.sh and related scripts without starting a new bash session: ` resetQuickstartEnvironment `

5. Nuclear options: 
   * ` resetQuickstartEnvironment --hard ` : kill -9 all services, remove everything including tarballs, re-download and re-build all
   * ` resetQuickstartEnvironment --hard && allInstall ` : same as above but install everything after reset

---

### Convenience functions implemented for DataWave Web and DataWave Ingest

DataWave-specific functions are implemented in a variety scripts. For example...
* [bin/query.sh](bin/query.sh): Query-related functions for interacting with DataWave Web's REST API
* [datawave/bootstrap.sh](bin/services/datawave/bootstrap.sh): Common functions. Parent wrapper for the DataWave Web and DataWave Ingest bootstraps
* [datawave/bootstrap-web.sh](bin/services/datawave/bootstrap-web.sh): Bootstrap for DataWave Web and associated functions
* [datawave/bootstrap-ingest.sh](bin/services/datawave/bootstrap-ingest.sh): Bootstrap for DataWave Ingest and associated functions
* [datawave/bootstrap-user.sh](bin/services/datawave/bootstrap-user.sh): User-specific configs/functions for your DataWave Web test user

The functions below are a small subset of those implemented for DataWave, but will likely be the most commonly used
    
* ` datawaveWebStart [ --debug ] `
    * Start up DataWave's web services in Wildfly. Pass the `--debug` flag to start Wildfly in debug mode
    
* ` datawaveQuery --query <query-expression> `
    * Submit queries on demand and inspect results 
    * Supports several options, use the ` --help ` flag for more info
    * After installation, try ` datawaveQuery --query "PAGE_TITLE:AccessibleComputing OR PAGE_TITLE:Anarchism" ` to view some Wikipedia search results
    * Also see [bin/query.sh](bin/query.sh) for more details
    * Query syntax guidance: <http://localhost:8443/DataWave/doc/query_help.html>
    
* ` datawaveWebTest `
    * Wrapper function for the [datawave/test-web/run.sh](bin/services/datawave/test-web/run.sh) script, for executing pre-configured, curl-based tests against DataWave's REST API
    * Supports several options, use the ` --help ` flag for more information
    
* ` datawaveIngestJson /path/to/local/file.json`
    * Kick off M/R job to ingest raw JSON file
    * Ingest config file: [myjson-ingest-config.xml](../../warehouse/ingest-configuration/src/main/resources/config/myjson-ingest-config.xml)
    * Raw file [ingested automatically](bin/services/datawave/install-ingest.sh) by the quickstart install: [tvmaze-api.json](../../warehouse/ingest-json/src/test/resources/input/tvmaze-api.json)
    * Execute the [ingest-tv-shows.sh](bin/services/datawave/ingest-examples/ingest-tv-shows.sh) script to download and ingest a JSON dataset dynamically via <http://tvmaze.com/api>
        * Upon completion, you should be able to execute queries related to cast and characters for several TV shows...
        * Shard query: ` datawaveQuery --query "EMBEDDED_CAST_PERSON_NAME == 'bryan cranston'" --syntax JEXL `
        * Edge query: ` datawaveQuery --logic EdgeQuery --syntax JEXL --query "SOURCE == 'don knotts' && TYPE == 'TV_COSTARS'" --pagesize 30 `
        * Edge-to-Shard query: ` datawaveQuery --logic EdgeEventQuery --syntax JEXL --query "SOURCE == 'don knotts' && SINK == 'andy griffith' && RELATION == 'PERSON-PERSON' &&  TYPE == 'TV_COSTARS'" `
    
* ` datawaveIngestWikipedia /path/to/local/enwiki*.xml `
    * Kick off M/R job to ingest a raw Wikipedia file. Any standard enwiki-flavored (raw XML) should suffice
    * Ingest config file: [wikipedia-ingest-config.xml](../../warehouse/ingest-configuration/src/main/resources/config/wikipedia-ingest-config.xml)
    * Raw file [ingested automatically](bin/services/datawave/install-ingest.sh) by the quickstart install: [enwiki-20130305-pages-articles-brief.xml](../../warehouse/ingest-wikipedia/src/test/resources/input/enwiki-20130305-pages-articles-brief.xml)
    * Example shard query: ` datawaveQuery --query "PAGE_TITLE:AccessibleComputing OR PAGE_TITLE:Anarchism" `
    * Example edge query: ` datawaveQuery --logic EdgeQuery --syntax JEXL --query "SOURCE == 'computer accessibility' && TYPE == 'REDIRECT'" `
    
* ` datawaveIngestCsv /path/to/local/file.csv `
    * Kick off M/R job to ingest a raw CSV file
    * Ingest config file: [mycsv-ingest-config.xml](../../warehouse/ingest-configuration/src/main/resources/config/mycsv-ingest-config.xml)
    * Raw file [ingested automatically](bin/services/datawave/install-ingest.sh) by the quickstart install: [my.csv](../../warehouse/ingest-csv/src/test/resources/input/my.csv)
    * Example query: ` datawaveQuery --query "FOO_FIELD:myfoo" `

* ` datawaveBuild ` and ` datawaveBuildDeploy ` 
    * Rebuild and/or redeploy DataWave as needed (i.e., after the initial install/deploy)
---

### Functions implemented by all service bootstraps

See `bin/services/{servicename}/bootstrap.sh`

| | |
|----------------:|:------------- |
| ` {servicename}Start ` | Start the service |
| ` {servicename}Stop ` | Stop the service |
| ` {servicename}Status ` | Display current status of the service, including PIDs if running |
| ` {servicename}Install ` | Install the service |
| ` {servicename}Uninstall ` | Uninstall the service, leaving tarballs in place. Optional `--remove-binaries, -rb` flag |
| ` {servicename}IsRunning ` | Returns 0 if running, non-zero otherwise. Mostly for internal use |
| ` {servicename}IsInstalled ` | Returns 0 if installed, non-zero otherwise. Mostly for internal use |
| ` {servicename}Printenv ` | Display current state of the service configuration, bash variables, etc |
| ` {servicename}PidList ` | Display all service PIDs on a single line, space-delimited |

Where ` {servicename} ` is one of ` hadoop `, ` accumulo `, ` zookeeper `, ` datawave `

### Common functions implemented for convenience

See [bin/common.sh](bin/common.sh)

| | |
|---------------:|:------------- |
| ` allStart ` | Start all services |
| ` allStop ` | Stop all services gracefully. Use ` --hard ` flag to ` kill -9 ` |
| ` allStatus ` | Display current status of all services, including PIDs if running |
| ` allInstall ` | Install all services |
| ` allUninstall ` | Uninstall all services, leaving tarballs in place by default. Optional '--remove-binaries' flag |
| ` allPrintenv ` | Display current state of all service configurations |

---

### PKI Notes
    
* Note that DataWave Web is PKI enabled by default and requires two-way authentication. The following self-signed materials are used for demonstration purposes
    * Server Truststore [JKS]: [ca.jks](../../web-services/deploy/application/src/main/wildfly/overlay/standalone/configuration/certificates/ca.jks)
    * Server Keystore [PKCS12]: [testServer.p12](../../web-services/deploy/application/src/main/wildfly/overlay/standalone/configuration/certificates/testServer.p12)
    * Client Cert [PKCS12]: [testUser.p12](../../web-services/deploy/application/src/main/wildfly/overlay/standalone/configuration/certificates/testUser.p12)
        
* Passwords for all of the above: `secret`

* This purpose of this PKI setup is to demonstrate DataWave's ability to be integrated easily into an organization's existing private key infrastructure and user authorization services. See [datawave/bootstrap-user.sh](bin/services/datawave/bootstrap-user.sh) for more information on configuration of the test user's roles and associated Accumulo authorizations

* To access DataWave Web endpoints in a browser, you'll need to import the client cert into the browser's certificate store

* If you'd like to test with your own certificate materials, see [datawave/bootstrap.sh](bin/services/datawave/bootstrap.sh) and override the trustore/keystore variables there prior to performing Step 2 of the install process above

#### Importing Client Certificate Into Firefox

* Open the Firefox Menu by clicking the three-bar icon.
* Select the 'Preferences' option.
* Click 'Privacy & Security' left-hand menu option.
* Click 'View Certificates' under the 'Certficates' section.
* Click 'Your Certificates' tab.
* Click 'Import' button.
* Select 'web-services/deploy/application/src/main/wildfly/overlay/standalone/configuration/certificates/testUser.p12', then click 'Open' button.
* Enter the 'secret' password, then click 'Ok'.
* Click 'Ok' to close the 'Certificate Manager' window.

#### Importing Client Certificate Into Chrome

* Open the Chrome Menu by clicking the three-bar icon.
* Select the 'Settings' option.
* Click 'Advanced' at the bottom of the page to open more setting options.
* Click 'Manage Certificates'
* Click 'IMPORT'
* Select 'web-services/deploy/application/src/main/wildfly/overlay/standalone/configuration/certificates/testUser.p12', then click 'Open' button.
* Enter the 'secret' password, then click 'Ok'.
* Click 'Ok' to close the 'Certificate Manager' window.
* Close the 'Settings' tab.

---
