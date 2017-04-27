# DataWave Quickstart

### Purpose

* This quickstart is intended to speed up your DataWave development 
workflow. It automates the steps that you'd otherwise need to perform
manually to configure, build, deploy, and test a standalone DataWave 
environment

* It includes setup and tear-down automation for DataWave, Hadoop, Accumulo,
ZooKeeper, etc, and it provides convenience methods for testing DataWave's
ingest and query components

* The scripts for installing and bootstrapping services are pluggable, so
you can select which services are activated in your environment, and you may 
modify/reconfigure existing (or create new) services without much hassle.
See `*/bin/env.sh` for more details

* For simplicity, all cluster services are downloaded/installed automatically,
they are extracted and installed under the quickstart home diriectory, and they
are all owned/executed by the current user

---

### Prerequisites

* None, other than Linux, Bash, and an Internet connection to `wget` tarballs

* JDK 1.8 and Maven 3.x are required. However, if it is determined that they are
*not* already installed/configured in your environment, they'll be downloaded and
installed automatically under `*/bin/services/java` and `*/bin/services/maven`
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

1. Update your environment...
    ```bash
    $ echo 'source /path/to/datawave-quickstart/bin/env.sh' >> ~/.bashrc 
    ```

2. Open a new bash terminal, or ` source ~/.bashrc ` in your current terminal...

    * Tarballs for any registered services (in this case, Hadoop, Accumulo, and ZooKeeper) 
      will begin downloading immediately

    * If desired, it's easy to select which version of a binary gets downloaded/installed
      for a given service, just update the service's ` **_DIST_URI ` variable in its bootstrap.sh

    * In the case of DataWave's ingest and web service binaries, a Maven build will be kicked
      off automatically, and their respective tarballs will be moved into place upon successful
      conclusion of the build
      
3. Install services...

    * When tarball downloads are finished and the DataWave build has completed, execute the 
    ` installAll ` command to initialize and configure all services.
      
    * Individual services may be installed one at a time, if desired, using ` {serviceName}Install `

    * All binaries are extracted & installed under ` /path/to/datawave-quickstart/bin/services/* `
    
    * When the DataWave Ingest install has completed, sample Wikipedia data will be ingested 
      automatically
      
    * Lastly, after the Wikipedia ingest job finishes, the DataWave Web installation will commence. 

4. Start services...

    * An individual service may be started at any time with its associated ` {serviceName}Start ` command
    
    * Using a DataWave-specific commands such as ` datawaveWebStart ` (for starting DataWave web services in Wildfly) 
      will automatically start up any dependencies, if necessary, such as Hadoop, Accumulo, ZooKeeper, etc

    * Execute the ` startAll ` command to bring up all services

5. Check the status of your services...

   * Execute the ` statusAll ` command, or use ` {serviceName}Status ` for the given service
   
   * Visit any of the following in your browser
       * Hadoop HDFS:  <http://localhost:50070/dfshealth.html#tab-overview>
       * Hadoop M/R:   <http://localhost:8088/cluster>
       * Accumulo:     <http://localhost:9995>
       * DataWave Web: <https://localhost:8443/DataWave/doc>
   
---

### Stop and/or uninstall services

1. To stop all services, execute the ` stopAll ` command. Likewise, you may stop individual services
   with the associated ` {serviceName}Stop ` command

2. To tear down all services and remove *all* associated data directories, execute the ` uninstallAll `
   command. Individual services may be uninstalled at any time with ` {serviceName}Uninstall `

---

### Build and deploy DataWave

After the initial installation and deployment of services, you may execute the ` datawaveBuildDeploy ` command
at any time to rebuild & deploy DataWave's ingest and web service components

---

### PKI Notes
    
* Note that DataWave Web is PKI enabled and by default uses the following self-signed materials from
`{source-root}/web-service-deployment/web-service-ear/src/main/wildfly/overlay/standalone/configuration/certificates/`

    * Server Truststore [JKS]: ca.jks
    * Server Keystore [PKCS12]: testServer.p12
    * Client Cert [PKCS12]: testUser.p12
        
* Passwords for all of the above: `secret`

* If you'd like to test with your own certs, update the PKI config in ` bin/services/datawave/bootstrap.sh `
  prior to downloading and installing services

---
