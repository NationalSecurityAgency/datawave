---
title: "Quickstart Reference"
tags: [getting_started]
summary: This page provides reference material for the DataWave Quickstart
---

## Global Bootstrap Functions

The functions below are implemented in [bin/common.sh][dw_blob_common_sh]

| Function Name | Description |
|---------------:|:------------- |
| `allStart` | Start up all services in the appropriate sequence |
| `allStop` | Stop all services gracefully. Use `--hard` flag to `kill -9` |
| `allStatus` | Display current status of all services, including PIDs if running |
| `allInstall` | Install all services |
| `allUninstall` | Uninstall all services, leaving tarballs in place. Optional `--remove-binaries` flag |
| `allPrintenv` | Display current state of all service configurations |

## Service Bootstrap Functions

The functions below are implemented for each service, where {*servicename*} can be one of...

**hadoop** | **accumulo** | **zookeeper** | **datawaveWeb**, **datawaveIngest**, or simply **datawave** for both

| Function Name&nbsp;&nbsp;&nbsp; | Description |
|----------------:|:------------- |
| `{servicename}Start` | Start the service |
| `{servicename}Stop` | Stop the service |
| `{servicename}Status` | Display current status of the service, including PIDs if running |
| `{servicename}Install` | Install the service |
| `{servicename}Uninstall` | Uninstall but leave tarball(s) in place. Optional `--remove-binaries` flag |
| `{servicename}IsRunning` | Returns 0 if running, non-zero otherwise. Mostly for internal use |
| `{servicename}IsInstalled` | Returns 0 if installed, non-zero otherwise. Mostly for internal use |
| `{servicename}Printenv` | Display current state of the service configuration, bash variables, etc |
| `{servicename}PidList` | Display all service PIDs on a single line, space-delimited |

---

## Accumulo Shell Alias (ashell)

To quickly launch the Accumulo Shell and authenticate as the *root* user, use the quickstart's `ashell` alias

```bash
  $ ashell

  Shell - Apache Accumulo Interactive Shell
  -
  - version: 2.1.2
  - instance name: my-instance-01
  - instance id: cc3e8158-a94a-4f2e-af9e-d1014b5d1912 
  -
  - type 'help' for a list of available commands
  -
  root@my-instance-01>

``` 

## Nuclear Options

### Quick Uninstall

To quickly kill any running services and uninstall everything (leaving downloaded *.tar.gz files in place):
```bash
   $ allStop --hard ; allUninstall
```
Same as above, but also remove any downloaded *.tar.gz files:
```bash
  $ allStop --hard ; allUninstall --remove-binaries
```
{% include important.html content="As the final step of the uninstall, it's always a good idea to exit your
current bash session in order to clear out lingering quickstart environment variables from memory" %}

### Quick Reinstall

Same as above, but re-download and reinstall everything:
```bash
  $ allStop --hard ; allUninstall --remove-binaries && allInstall
```
{% include important.html content="Before performing a reinstall, it's always a good idea to exit your
previous bash session in order to clear out lingering quickstart environment variables from memory, and then start
a new bash session to perform the reinstall" %}
---

## DataWave Functions

### Scripts

DataWave's features are exposed primarily through configs and functions defined within the scripts listed below

| Script Name&nbsp;&nbsp;&nbsp; | Description |
|---------------:|:------------- |
| [query.sh][dw_blob_query_sh] | Query-related functions for interacting with DataWave Web's REST API |
| [bootstrap.sh][dw_blob_datawave_bootstrap] | Common functions. Parent wrapper for web &amp; ingest bootstraps |
| [bootstrap-web.sh][dw_blob_datawave_bootstrap_web] | Bootstrap for DataWave Web and associated functions |
| [bootstrap-ingest.sh][dw_blob_datawave_bootstrap_ingest] | Bootstrap for DataWave Ingest and associated functions |
| [bootstrap-user.sh][dw_blob_datawave_bootstrap_user] | Configs for defining DataWave Web test user's identity, roles, auths, etc |

A few noteworthy functions and their descriptions are listed by category below

### DataWave Web Functions

| `datawaveWebStart [ --debug ]` |
| Start up DataWave's web services in Wildfly. Pass the `--debug` flag to start Wildfly in debug mode |
| Implementation: [bootstrap-web.sh][dw_blob_datawave_bootstrap_web_start] |

| `datawaveQuery --query <query-expression>` |
| Submit queries on demand and inspect results. Use the `--help` flag for information on query options | 
| Query syntax guidance is [here](../query/syntax) |
| Implementation: [query.sh][dw_blob_query_sh_query_func] |

| `datawaveWebTest` |
| Wrapper function for [test-web/run.sh][dw_blob_test_web] script. Run a suite of curl-based [tests][dw_web_tests] against DataWave Web |
| Supports several options. Use the `--help` flag for more information |
| Implementation: [bootstrap-web.sh][dw_blob_datawave_bootstrap_web_test] |

### DataWave Ingest Functions
    
| `datawaveIngestJson /path/to/some/tvmaze.json` |
| Kick off M/R job to ingest raw JSON file containing TV show data from <http://tvmaze.com/api> |
| Ingest config file: [myjson-ingest-config.xml][dw_blob_myjson_config] |
| File ingested automatically by the DataWave Ingest installer (install-ingest.sh): [tvmaze-api.json][dw_blob_tvmaze_raw_json] |
| Use the [ingest-tv-shows.sh][dw_blob_ingest_tvshows] script to download &amp; ingest more of your favorite shows |
| Implementation: [bootstrap-ingest.sh][dw_blob_datawave_bootstrap_ingest_json] |

| `datawaveIngestWikipedia /path/to/some/enwiki.xml` |
| Kick off M/R job to ingest a raw Wikipedia XML file. Any standard *enwiki*-flavored file should suffice |
| Ingest config file: [wikipedia-ingest-config.xml][dw_blob_mywikipedia_config] |
| File ingested automatically by the DataWave Ingest installer (install-ingest.sh): [enwiki-20130305*.xml][dw_blob_enwiki_raw_xml] |
| Implementation: [bootstrap-ingest.sh][dw_blob_datawave_bootstrap_ingest_wiki] |
    
| `datawaveIngestCsv /path/to/some/file.csv` |
| Kick off M/R job to ingest a raw CSV file similar to [my.csv][dw_blob_my_raw_csv] |
| Ingest config file: [mycsv-ingest-config.xml][dw_blob_mycsv_config] |
| File ingested automatically by the DataWave Ingest installer (install-ingest.sh): [my.csv][dw_blob_my_raw_csv] |
| Implementation: [bootstrap-ingest.sh][dw_blob_datawave_bootstrap_ingest_csv] |

### Build/Deploy Functions

| `datawaveBuild` |
| Rebuild DataWave as needed (i.e., after the initial install/deploy) |
| Implementation: [bootstrap.sh][dw_blob_datawave_bootstrap_build] |

| `datawaveBuildDeploy` |
| Redeploy DataWave as needed (i.e., after the initial install/deploy) |
| Implementation: [bootstrap.sh][dw_blob_datawave_bootstrap_build_deploy] |

---

## DataWave Build Notes
 
The quickstart performs the following steps at build time in order to reliably configure DataWave for deployment under
your *DW_SOURCE/contrib/datawave-quickstart* directory. Note that, by default, it leverages the *dev* profile as defined in *DW_SOURCE/pom.xml*

1. Copies the existing *DW_SOURCE/properties/dev.properties* file to *datawave-quickstart/data/datawave/build-properties/dev.properties*
2. Appends to the copied *dev.properties* file any property overrides that are necessary for deployment
3. Creates *~/.m2/datawave/properties/dev.properties* symlink that points to the copied *dev.properties* file
4. Executes the Maven build command given by *$DW_DATAWAVE_BUILD_COMMAND*

Note that you may select a different Maven profile for this purpose by simply overriding *$DW_DATAWAVE_BUILD_PROFILE*
in your environment

---

## PKI Notes
    
In the quickstart environment, DataWave Web is PKI enabled and uses two-way authentication by default. Moreover, the following
self-signed materials are used...

| File Name | Type | Description |
|----------:|------|------------ |
| [ca.jks][dw_blob_ca_jks] | JKS | Truststore for the Wildfly JEE Application Server |
| [testServer.p12][dw_blob_server_p12] | PKCS12 | Server Keystore for the Wildfly JEE Application Server |
| [testUser.p12][dw_blob_user_p12] | PKCS12 | Test user client cert |
        
* Passwords for all of the above: *`ChangeIt`*

* To access DataWave Web endpoints in a browser, you'll need to import the client cert into the browser's certificate store

* The goal of the quickstart's PKI setup is to demonstrate DataWave's ability to be integrated easily into an organization's existing
  private key infrastructure and user auth services. See [datawave/bootstrap-user.sh][dw_blob_datawave_bootstrap_user]
  for more information on configuring the test user's roles and associated Accumulo authorizations

* To test with your own certificate materials, override the keystore &amp; truststore variables from [datawave/bootstrap.sh][dw_blob_datawave_bootstrap_pki]
  within your *~/.bashrc* prior to [installing the quickstart](quickstart-install)

[dw_blob_env_sh]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/contrib/datawave-quickstart/bin/env.sh
[dw_blob_common_sh]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/contrib/datawave-quickstart/bin/common.sh#L117
[dw_blob_query_sh]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/contrib/datawave-quickstart/bin/query.sh
[dw_blob_query_sh_query_func]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/contrib/datawave-quickstart/bin/query.sh#L16
[dw_blob_datawave_bootstrap_pki]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/contrib/datawave-quickstart/bin/services/datawave/bootstrap.sh#L50
[dw_blob_datawave_bootstrap]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/contrib/datawave-quickstart/bin/services/datawave/bootstrap.sh
[dw_blob_datawave_bootstrap_web]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/contrib/datawave-quickstart/bin/services/datawave/bootstrap-web.sh
[dw_blob_datawave_bootstrap_web_start]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/contrib/datawave-quickstart/bin/services/datawave/bootstrap-web.sh#L116
[dw_blob_datawave_bootstrap_web_test]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/contrib/datawave-quickstart/bin/services/datawave/bootstrap-web.sh#L63
[dw_blob_datawave_bootstrap_ingest]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/contrib/datawave-quickstart/bin/services/datawave/bootstrap-ingest.sh
[dw_blob_datawave_bootstrap_ingest_json]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/contrib/datawave-quickstart/bin/services/datawave/bootstrap-ingest.sh#L222
[dw_blob_datawave_bootstrap_ingest_wiki]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/contrib/datawave-quickstart/bin/services/datawave/bootstrap-ingest.sh#L161
[dw_blob_datawave_bootstrap_ingest_csv]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/contrib/datawave-quickstart/bin/services/datawave/bootstrap-ingest.sh#L198
[dw_blob_datawave_bootstrap_user]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/contrib/datawave-quickstart/bin/services/datawave/bootstrap-user.sh
[dw_web_tests]: https://github.com/NationalSecurityAgency/datawave/tree/{{ page.release_tag }}/contrib/datawave-quickstart/bin/services/datawave/test-web/tests
[dw_datawave_home]: https://github.com/NationalSecurityAgency/datawave/tree/{{ page.release_tag }}/contrib/datawave-quickstart/bin/services/datawave
[dw_blob_ca_jks]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/web-services/deploy/application/src/main/wildfly/overlay/standalone/configuration/certificates/ca.jks
[dw_blob_server_p12]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/web-services/deploy/application/src/main/wildfly/overlay/standalone/configuration/certificates/testServer.p12
[dw_blob_user_p12]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/web-services/deploy/application/src/main/wildfly/overlay/standalone/configuration/certificates/testUser.p12
[dw_blob_test_web]: https://github.com/NationalSecurityAgency/datawave/tree/{{ page.release_tag }}/contrib/datawave-quickstart/bin/services/datawave/test-web
[dw_blob_myjson_config]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/warehouse/ingest-configuration/src/main/resources/config/myjson-ingest-config.xml
[dw_blob_tvmaze_raw_json]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/warehouse/ingest-json/src/test/resources/input/tvmaze-api.json
[dw_blob_ingest_tvshows]: https://github.com/NationalSecurityAgency/datawave/tree/{{ page.release_tag }}/contrib/datawave-quickstart/bin/services/datawave/ingest-examples
[dw_blob_mywikipedia_config]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/warehouse/ingest-configuration/src/main/resources/config/wikipedia-ingest-config.xml
[dw_blob_mycsv_config]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/warehouse/ingest-configuration/src/main/resources/config/mycsv-ingest-config.xml
[dw_blob_enwiki_raw_xml]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/warehouse/ingest-wikipedia/src/test/resources/input/enwiki-20130305-pages-articles-brief.xml
[dw_blob_my_raw_csv]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/warehouse/ingest-csv/src/test/resources/input/my.csv
[dw_blob_datawave_bootstrap_build]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/contrib/datawave-quickstart/bin/services/datawave/bootstrap.sh#L403
[dw_blob_datawave_bootstrap_build_deploy]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/contrib/datawave-quickstart/bin/services/datawave/bootstrap.sh#L393
