---
title: "Troubleshooting"
tags: [getting_started, troubleshooting]
summary: This page provides troubleshooting help for the DataWave Quickstart
---

## DataWave-Specific Issues

Take a moment to search [previously reported issues](https://github.com/NationalSecurityAgency/datawave/issues) for key terms associated with any errors that you're observing

### Check Query/Web Logs for Errors

```bash
  $ cd QUICKSTART_DIR/wildfly/standalone/log/
```

Typically, the earliest sign of a web service deployment problem will be output as shown here, indicating that DataWave's EJB-based services are failing to deploy within the Wildfly container...
```
$ datawaveWebStart
[DW-INFO] - Starting Wildfly
[DW-INFO] - Polling for EAR deployment status every 4 seconds (15 attempts max)
-- Wildfly process not found (1/15)
+- Wildfly up (12345). EAR deployment pending (2/15)
+- Wildfly up (12345). EAR deployment pending (3/15)
+- Wildfly up (12345). EAR deployment pending (4/15)
+- Wildfly up (12345). EAR deployment pending (5/15)
+- Wildfly up (12345). EAR deployment pending (6/15)
+- Wildfly up (12345). EAR deployment pending (7/15)
+- Wildfly up (12345). EAR deployment pending (8/15)
+- Wildfly up (12345). EAR deployment pending (9/15)
+- Wildfly up (12345). EAR deployment pending (10/15)
+- Wildfly up (12345). EAR deployment pending (11/15)
+- Wildfly up (12345). EAR deployment pending (12/15)
+- Wildfly up (12345). EAR deployment pending (13/15)
+- Wildfly up (12345). EAR deployment pending (14/15)
+- Wildfly up (12345). EAR deployment pending (15/15)
```
In this case, the root cause of the issue can usually be determined by noting the first occurrence of errors in `$WILDFLY_HOME/standalone/log/server.log`.
Subsequent errors in `server.log` tend to be less relevant

### Check Ingest Job/Yarn Logs for Errors
```bash
  $ cd QUICKSTART_DIR/data/hadoop/yarn/log/
```

### Investigate Web Test Failures

If you observe test failures from the `datawaveWebTest` function...
```bash
  # View inline help for additional test options
  $ datawaveWebTest --help
  ...

  # Rerun web tests with more/better output
  $ datawaveWebTest --verbose --pretty-print
  ...
  
  # View DataWave Query/Web logs
  $ cd QUICKSTART_DIR/wildfly/standalone/log/
```

### 403 Forbidden from Browser?

If you're receiving *403 - Forbidden* errors when accessing DataWave Web endpoints from a web browser, make
sure that you've imported the test user's client certificate into the browser's certificate store. See the 
quickstart [PKI Notes](quickstart-reference#pki-notes)

### Build &amp; Runtime Errors

Check the Maven output to determine the cause of any build failures...
```bash
 # A copy of Maven's output is always saved here...
 $ less QUICKSTART_DIR/data/datawave/build-properties/build-progress.tmp
```
Inspect build properties to see if anything seems amiss. For example, any configured paths that do not have
your QUICKSTART_DIR as the parent should be suspect...

```bash
 # The properties file used by the quickstart's Maven build is here...
 $ less QUICKSTART_DIR/data/datawave/build-properties/{profile}.properties
```
  
Verify that you have the symlink, **~/.m2/datawave/properties/{profile}.properties**, for the file above.
For example...
```bash
 $ ls -l ~/.m2/datawave/properties/dev.properties
 ... ~/.m2/datawave/properties/dev.properties -> QUICKSTART_DIR/data/datawave/build-properties/dev.properties
```

## Check Accumulo Logs for Errors

```bash
  $ cd QUICKSTART_DIR/accumulo/logs/
```

## Check Hadoop Logs for Errors

```bash
  $ cd QUICKSTART_DIR/hadoop/logs/
```

## Password-less SSH

Hadoop needs password-less ssh. You should be able to do this without being prompted for a password...

```bash
  $ ssh localhost
```
* Note that the quickstart's Hadoop install will set up password-less ssh for you automatically, *unless* it detects that you already have ssh keys
* Get help [here](https://hadoop.apache.org/docs/r2.9.1/hadoop-project-dist/hadoop-common/SingleCluster.html#Setup_passphraseless_ssh)

## View Status of Services

Inspect all of your running JVMs...
 
```bash
  $ jps -lm
```

Compare the `jps` output with your quickstart service status: `allStatus` will report PIDs of all your services, and should
appear similar to the following (Note: ZooKeeper PID appears in the Accumulo PID list, as ZK is managed by the Accumulo bootstrap)...
```bash
  $ allStatus
    Hadoop is running. PIDs: 9318 9629 9988 10188 10643 10705
    Accumulo is running. PIDs: 11196 11326 11536 11645 11750 11081
    DataWave Ingest is not running
    DataWave Web is running. PIDs: 17462
```

## Quickstart Help

View the [reference guide](quickstart-reference) for more information
