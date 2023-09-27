---
title: "Quickstart Installation"
tags: [getting_started]
toc: false
summary: |
  <p>This quickstart provides a single-node standalone DataWave instance that you may use to follow along with the
  <a href="../tour/getting-started">guided tour</a>. It is also generally useful as a development tool, as it provides a
  consistent, repeatable process for deploying and testing your local DataWave build.</p>
  <p>The quickstart provides setup and tear-down automation for DataWave, Hadoop, Accumulo, ZooKeeper, and Wildfly,
  and it includes many <a href="quickstart-reference">utility functions</a> that will streamline most interactions
  with these services.</p>
---

## Before You Start

### Prerequisites

* Linux, Bash, and an Internet connection to `wget` tarballs
* JDK 8 must be installed with JAVA_HOME and PATH configured accordingly
* You should be able to [ssh to localhost without a passphrase](https://hadoop.apache.org/docs/r2.9.1/hadoop-project-dist/hadoop-common/SingleCluster.html#Setup_passphraseless_ssh)
  * Note that the quickstart's Hadoop install will set up passphrase-less ssh for you automatically, *unless* it detects that you already have a private/public key pair generated

### System Recommendations

* OS: To date, this quickstart has been tested on CentOS 6x, 7x and Ubuntu 16x
* 8GB RAM + single quad-core processor (minimum)
* Storage: To install everything, you'll need at least a few GB free
* ulimit -u (max user processes): 32768
* ulimit -n (open files): 32768
* vm.swappiness: 0

### Get the Source Code

<a class="btn btn-success" style="width: 220px;" href="{{ site.repository_url }}/" role="button" target="_blank"><i class="fa fa-github fa-lg"></i> Clone the Repo</a>

{% include note.html content="The local path that you select for DataWave's source code will be refererred to as
**DW_SOURCE** from this point forward. Wherever it appears in a bash command below, be sure to substitute it with the
actual path" %}

### Docker Alternative

If you would prefer to run the DataWave Quickstart environment as a Docker container, skip the 4 steps described below.
Go [here][dw_docker_alternative] instead and view the *README* file. Note that the *[Override Default Binaries](#override-default-binaries)*
section below is also relevant to the Docker image build.

---

## Quickstart Setup

```bash
# Step 1
$ echo -e "activateDW() {\n source DW_SOURCE/contrib/datawave-quickstart/bin/env.sh\n}" >> ~/.bashrc
# Step 2a
$ source ~/.bashrc
# Step 2b
$ activateDW
# Step 3
$ allInstall
# Step 4
$ datawaveWebStart && datawaveWebTest
# Setup is now complete
```

The commands above will complete the entire quickstart installation. However, it's a good idea
to at least skim over the sections below to get an idea of how the setup works and how to customize it
for your own preferences.

To keep things simple, **DataWave**, **Hadoop**, **Accumulo**, **ZooKeeper**, and **Wildfly** will be installed under your
*DW_SOURCE/contrib/datawave-quickstart* directory, and all will be owned by / executed as the current user.

{% include important.html content="If you currently have any of the above installed locally under *any* user account,
you should ensure that all are stopped/disabled before proceeding" %}

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a class="noCrossRef" href="#update-bashrc" data-toggle="tab"><b>1: Update ~/.bashrc</b></a></li>
    <li><a class="noCrossRef" href="#bootstrap-env" data-toggle="tab"><b>2: Bootstrap the Environment</b></a></li>
    <li><a class="noCrossRef" href="#install-services" data-toggle="tab"><b>3: Install Services</b></a></li>
    <li><a class="noCrossRef" href="#wildfly-test" data-toggle="tab"><b>4: Start Wildfly &amp; Run Tests</b></a></li>
</ul>
<div class="tab-content">

<div role="tabpanel" class="tab-pane active" id="update-bashrc" markdown="1">
### Step 1: Update ~/.bashrc

#### 1.1 Add the Quickstart Environment

This step ensures that your DataWave environment and all its services can be configured on-demand across
your various bash sessions, as needed.

```bash
  # Step 1
  $ echo -e "activateDW() {\n source DW_SOURCE/contrib/datawave-quickstart/bin/env.sh\n}" >> ~/.bashrc
```
The *activateDW* bash function, when invoked for the first time in a bash session will source
*[env.sh][dw_blob_env_sh]*, which in turn bootstraps each DataWave service via its respective
*{servicename}/bootstrap.sh* script. The bootstrap scripts define supporting bash variables and functions,
encapsulating configuration and basic start/stop functionality consistently for all services.

#### 1.2 Override Default Binaries

To override the quickstart's default version of a particular installation binary, you may override the
desired *DW_\*_DIST_URI* value as shown below. URIs may be local or remote. Local file URI values must
be prefixed with *file://*

```bash
  $ vi ~/.bashrc
     ...
     export DW_HADOOP_DIST_URI=file:///my/local/binaries/hadoop-x.y.z.tar.gz
     export DW_ACCUMULO_DIST_URI=http://some.apache.mirror/accumulo/1.x/accumulo-1.x-bin.tar.gz
     export DW_ZOOKEEPER_DIST_URI=http://some.apache.mirror/zookeeper/x.y/zookeeper-x.y.z.tar.gz
     export DW_WILDFLY_DIST_URI=file:///my/local/binaries/wildfly-10.x.tar.gz
     export DW_MAVEN_DIST_URI=file:///my/local/binaries/apache-maven-x.y.z.tar.gz

     function activateDW() {                                     # Added by Step 1
       source DW_SOURCE/contrib/datawave-quickstart/bin/env.sh
     }
     ...
```

</div>

<div role="tabpanel" class="tab-pane" id="bootstrap-env" markdown="1">
### Step 2: Bootstrap the Environment
```bash
  $ source ~/.bashrc                                                    # Step 2a
  $ activateDW                                                          # Step 2b
```
When the *activateDW* bash function is invoked as shown above, tarballs for registered services will be automatically copied/downloaded
from their configured locations into their respective service directories, i.e., under *datawave-quickstart/bin/services/{servicename}/*.

A DataWave build will also be kicked off automatically.
DataWave's ingest and web service binaries will be copied into place upon conclusion of the build. This step can take
several minutes to complete, so now is a good time step away for a break.

{% include note.html content="The **DW_DATAWAVE_BUILD_COMMAND** variable in [datawave/bootstrap.sh][dw_blob_datawave_bootstrap_mvn_cmd]
defines the Maven command used for the build. It may be overridden in *~/.bashrc* or from the command line as needed" %}
</div>

<div role="tabpanel" class="tab-pane" id="install-services" markdown="1">
### Step 3: Install Services
```bash
  $ allInstall                                                                 # Step 3
```
The `allInstall` bash function will initialize and configure all services in the correct sequence. Alternatively,
individual services may be installed one at a time, if desired, using their respective `{servicename}Install` bash functions.

</div>

<div role="tabpanel" class="tab-pane" id="wildfly-test" markdown="1">
### Step 4: Start Wildfly and Test Web Services
```bash
  $ datawaveWebStart && datawaveWebTest                                        # Step 4
```

This will start up DataWave Web and run [preconfigured tests][dw_web_tests] against
DataWave's REST API. Note any test failures, if present, and check logs for error messages
</div>

</div>

---

## What's Next?

* If all web service tests passed in *Step 4*, then you're ready for the [guided tour](../tour/getting-started).
* If you encountered any issues along the way, please read through the [troubleshooting](quickstart-trouble) guide.
* To learn more about the quickstart environment and its available features, check out the [quickstart reference](quickstart-reference).


[dw_blob_env_sh]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/contrib/datawave-quickstart/bin/env.sh
[dw_blob_common_sh]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/contrib/datawave-quickstart/bin/common.sh
[dw_blob_query_sh]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/contrib/datawave-quickstart/bin/query.sh
[dw_blob_datawave_bootstrap_mvn_cmd]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/contrib/datawave-quickstart/bin/services/datawave/bootstrap.sh#L34
[dw_blob_datawave_bootstrap]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/contrib/datawave-quickstart/bin/services/datawave/bootstrap.sh
[dw_blob_datawave_bootstrap_web]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/contrib/datawave-quickstart/bin/services/datawave/bootstrap-web.sh
[dw_blob_datawave_bootstrap_ingest]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/contrib/datawave-quickstart/bin/services/datawave/bootstrap-ingest.sh
[dw_blob_datawave_bootstrap_user]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/contrib/datawave-quickstart/bin/services/datawave/bootstrap-user.sh
[dw_web_tests]: https://github.com/NationalSecurityAgency/datawave/tree/{{ page.release_tag }}/contrib/datawave-quickstart/bin/services/datawave/test-web/tests
[dw_datawave_home]: https://github.com/NationalSecurityAgency/datawave/tree/{{ page.release_tag }}/contrib/datawave-quickstart/bin/services/datawave
[dw_docker_alternative]: https://github.com/NationalSecurityAgency/datawave/tree/{{ page.release_tag }}/contrib/datawave-quickstart/docker
