---
title: "Troubleshooting"
tags: [getting_started, troubleshooting]
summary: This page provides troubleshooting help for the DataWave Quickstart
---

## DataWave-Specific Issues

### Check Query/Web Logs for Errors
```bash
  $ cd QUICKSTART_DIR/wildfly/standalone/log/
```

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
