# DataWave Quickstart

### Purpose

* This quickstart is intended to speed up your development workflow by automating
the steps that you'd otherwise need to perform manually to configure, build, deploy, 
and test a standalone DataWave environment

* It includes setup and tear-down automation for DataWave, Hadoop, Accumulo,
ZooKeeper, etc, and provides convenience methods for testing DataWave's
ingest and query components

* For simplicity, all cluster services are downloaded/installed automatically,
they are extracted and installed under the quickstart home diriectory, and all
are owned/executed by the current user

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

1. Update your ~/.bashrc...
    ```bash
    $ echo 'source /path/to/datawave-quickstart/bin/env.sh' >> ~/.bashrc 
    ```
    If you happen to update ~/.bashrc via some other method, such as with *vi* or other, beware that
    env.sh sourcing should be placed *AFTER* any of your existing Hadoop-, Accumulo-, Wildfly-, and
    ZooKeeper-related variable exports. This will prevent those existing configs from interfering
    with the quickstart installation

2. To bootstrap services, open a new bash terminal or ` source ~/.bashrc ` in your current terminal

    * Tarballs for any registered services (in this case, Hadoop, Accumulo, and ZooKeeper) 
      should begin downloading, and the DataWave build should be started automatically

    * If desired, it's easy to affect which version of a binary gets downloaded/installed
      for a given service, just override the ` DW_**_DIST_URI ` variable defined in the service's 
      ` bootstrap.sh ` before */datawave-quickstart/bin/env.sh gets sourced.
      
      For example...
      ```bash
      $ vi ~/.bashrc

          ...
  
          export DW_ACCUMULO_DIST_URI=http://my.favorite.apache.mirror/accumulo/1.8.X/accumulo-1.8.X-bin.tar.gz
          export DW_JAVA_DIST_URI=file:///my/local/binaries/jdk-8-linux-x64.tar.gz
          export DW_HADOOP_DIST_URI=file:///my/local/binaries/hadoop-2.6.0-cdh5.9.1.tar.gz
          export DW_ZOOKEEPER_DIST_URI=file:///my/local/binaries/zookeeper-3.4.5-cdh5.9.1.tar.gz
  
          source /path/to/datawave-quickstart/bin/env.sh
      ``` 

    * In the case of DataWave's ingest and web service binaries, a Maven build will be kicked
      off automatically, and their respective tarballs will be moved into place upon successful
      conclusion of the build. The ` DW_DATAWAVE_BUILD_COMMAND ` variable in datawave/bootstrap.sh
      defines the Maven command used for the build and may be overridden if needed 
      
3. Install services...

    * When tarball downloads are finished and the DataWave build has completed, execute the 
    ` allInstall ` command to initialize and configure all services.
      
    * Individual services may be installed one at a time, if desired, using ` {serviceName}Install `

    * All binaries are extracted & installed under ` /path/to/datawave-quickstart/bin/services/* `
    
    * When the DataWave Ingest install has completed, sample data data will be ingested 
      automatically via M/R
      
    * Lastly, when ingest jobs have completed, the DataWave Web installation will commence. 

4. Start services...

    * An individual service may be started at any time with its associated ` {serviceName}Start ` command
    
    * Using a DataWave-specific command, such as ` datawaveWebStart ` for starting DataWave web services in Wildfly, 
      will automatically start up any dependencies, if necessary, such as Hadoop, Accumulo, ZooKeeper, etc

    * Or execute the ` allStart ` command to bring up all services

5. Check the status of your services...

   * Execute the ` allStatus ` command or use ` {serviceName}Status ` for a given service to display PIDs
   
   * Execute ` datawaveWebTest ` to run pre-configured curl-based tests against DataWave Web's REST API.
     See ` datawaveWebTest --help ` for all available options   
   
   * Visit any of the following in your browser
       * Hadoop HDFS:  <http://localhost:50070/dfshealth.html#tab-overview>
       * Hadoop M/R:   <http://localhost:8088/cluster>
       * Accumulo:     <http://localhost:9995>
       * DataWave Web: <https://localhost:8443/DataWave/doc>
   
---

### Stop and/or uninstall services

1. To stop all services gracefully, execute the ` allStop ` command. Likewise, you may stop individual services
   with the associated ` {serviceName}Stop ` command
   
2. ` allStop --hard ` will bring down all services via ` kill -9 ` with no prompting

3. To tear down all services and remove *all* associated data directories, execute the ` allUninstall `
   command. Individual services may be uninstalled at any time with ` {serviceName}Uninstall `

---

### Convenience functions implemented specifically for DataWave 

See `bin/services/datawave/bootstrap*.sh`

* ` datawaveBuildDeploy ` 
    * Rebuild and redeploy DataWave as needed (i.e., after the initial install/deploy)
    
* ` datawaveWebStart [ --debug ] `
    * Start up DataWave's web services in Wildfly. Pass `--debug` flag to start Wildfly in debug mode
    
* ` datawaveWebTest `
    * Wrapper function for the ` services/datawave/test-web/run.sh ` script, for executing pre-configured, curl-based tests against DataWave's REST API
    * Supports several options, use the ` --help ` flag for more information
    
* ` datawaveIngestWikipedia /path/to/local/enwiki*.xml `
    * Kick off M/R job to ingest a raw Wikipedia file as needed
    
* ` datawaveIngestCsv /path/to/local/file.csv `
    * Kick off M/R job to ingest a raw CSV file as needed
    
* ` datawaveIngestJson /path/to/local/file.json`
    * Coming soon. Work in progress

---

### Functions implemented by all service bootstraps

See `bin/services/{servicename}/bootstrap.sh`

| | |
|----------------:|:------------- |
| ` {servicename}Start ` | Start the service |
| ` {servicename}Stop ` | Stop the service |
| ` {servicename}Status ` | Display current status of the service, including PIDs if running |
| ` {servicename}Install ` | Install the service |
| ` {servicename}Uninstall ` | Uninstall the service, leaving tarballs in place. Optional '--remove-binaries' flag |
| ` {servicename}IsRunning ` | Returns 0 if running, non-zero otherwise. Mostly for internal use |
| ` {servicename}IsInstalled ` | Returns 0 if installed, non-zero otherwise. Mostly for internal use |
| ` {servicename}Printenv ` | Display current state of the service configuration, bash variables, etc |
| ` {servicename}PidList ` | Display all service PIDs on a single line, space-delimited |

Where ` {servicename} ` is one of ` hadoop `, ` accumulo `, ` zookeeper `, ` datawave `

### Common functions implemented for convenience

See ` bin/common.sh `

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
    
* Note that DataWave Web is PKI enabled and by default uses the following self-signed materials from
`{source-root}/web-services/deploy/application/src/main/wildfly/overlay/standalone/configuration/certificates/`

    * Server Truststore [JKS]: ca.jks
    * Server Keystore [PKCS12]: testServer.p12
    * Client Cert [PKCS12]: testUser.p12
        
* Passwords for all of the above: `secret`

* If you'd like to test with your own certs, update the PKI config in ` bin/services/datawave/bootstrap.sh `
  prior to downloading and installing services

---
