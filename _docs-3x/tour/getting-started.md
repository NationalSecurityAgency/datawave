---
title: "DataWave Tour: Getting Started"
layout: tour
tags: [getting_started]
summary: |
  The guided tour will introduce you to DataWave's ingest and query components and provide several examples of how to use
  and configure them. In order to follow along with the examples, you should first complete the
  <a href="../getting-started/quickstart-install">Quickstart Installation</a>
---

## Verify Your Environment Setup

At this point, you should have a standalone DataWave Quickstart environment instantiated. Throughout the tour, we'll be
using its example datasets, configuration files, and many of its [utility functions](../getting-started/quickstart-reference).

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a class="noCrossRef" href="#check-services" data-toggle="tab"><b>1: Check Status of Services</b></a></li>
    <li><a class="noCrossRef" href="#verify-web" data-toggle="tab"><b>2: Verify DataWave Web Deployment</b></a></li>
</ul>
<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="check-services" markdown="1">
### Step 1: Check the Status of Services

```bash
 $ allStatus
   ======  Hadoop Status  ======
   pids: 30722 31065 31328 31824 31894
   [DW-INFO] - ResourceManager => 31328
   [DW-INFO] - NameNode => 30722
   [DW-INFO] - NodeManager => 31824
   [DW-INFO] - JobHistoryServer => 31894
   [DW-INFO] - DataNode => 31065
   ======  Accumulo Status  ======
   pids: 327 430 32371 32509 32697
   [DW-INFO] - gc => 327
   [DW-INFO] - tracer => 430
   [DW-INFO] - monitor => 32371
   [DW-INFO] - master => 32697
   [DW-INFO] - tserver => 32509
   ======  ZooKeeper Status  ======
   [DW-INFO] - ZooKeeper => 32245
   ======  DataWave Ingest Status  ======
   [DW-INFO] - No ingest processes are running
   ======  DataWave Web Status  ======
   [DW-INFO] - Wildfly is running => 518
    
 # If DataWave Ingest is running, stop it for now...
 $ datawaveIngestStop
   ...
```
If no services are currently running, go to **Step 2**
</div>
<div role="tabpanel" class="tab-pane" id="verify-web" markdown="1">
### Step 2: Verify DataWave Web Deployment

```bash
 # If DataWave Web is not currently running, start it up with this function...
 # (this will also start Hadoop, Accumulo, ZooKeeper, if needed)
 $ datawaveWebStart
 
 [DW-INFO] - Starting Wildfly
 [DW-INFO] - Polling for EAR deployment status every 4 seconds (15 attempts max)
     -- Wildfly process not found (1/15)
     +- Wildfly up (7663). EAR deployment pending (2/15)
        ...
        ...
     ++ DataWave Web successfully deployed
```

```bash
 # All of the pre-configured web tests should pass...
 $ datawaveWebTest
   ...
   ...
   QueryMetricsQueryCreateAndNext
   QueryMetricsQueryClose
   QueryMetricsQueryZeroResults204
   
   Failed Tests: 0 
```
</div>
</div>

---

## Troubleshooting

If you experience issues with the quickstart environment at any time, please see the [troubleshooting guide](../getting-started/quickstart-trouble) 


