Filename:    README.txt
Description: Basic instructions on building and using the Query Heads 
             Up Display: (Query-HUD)
Date:        12 September 2014
---------------------------------------------------------------------
Building:
---------

The Query-HUD war is build using the command:

$ mvn clean package -P <profileName>

The value <profileName> is used to specify the maven profile that specifies
the particulars of the war file that is generated.  Presently, there are
two profiles defined "dev" and "prod".  Use "dev" to build for a developer's
local workstation, and use "prod" to build for a cluster deployment.

Deployment:
-----------

The build process generates a war file named "${basedir}/target/Query-HUD.war". 
Copy this war file to the deployment directory of your jboss server to complete 
deployment.

Accessing and Using the Query-HUD:
----------------------------------
The Query-HUD is accessed through the URL: 

https://<hostname>:<port>/Query-HUD/

In the top panel labeled "Select User", select the user-name of the user to monitor from the drop-down box.
Normal users will only be able to select their own user-name, whereas users with the "Administrator"
or "JBossAdministrator" roles will be able to select the user-name of any user with with an
actively running query.

The Running Queries Panel will now show the running queries (present in the Persister EJB) for 
the selected user.

Choose a specific query-id to see the metrics for the pages returned from this query.  The
panel labeled "Percentages" will show the percent volume for the number of pages/results and bytes
returned by the selected query as compared to all queries on the system for the last hour.

The UI will periodically update the Running Queries and Summary Query Metrics, as will as 
update the displayed percentages and page metrics for the currently selected Query.

Web Services used by the Query-HUD:
-----------------------------------

The Query-HUD depends on web-services that are provided by the HudBean EJB.  This EJB is 
defined in the Query sub-project in the datawave web-service project. The package path 
is "nsa.datawve.webservice.query.hud".

Webservice paths used:
/queryhud/runningqueries/{userid} : returns the query metrics for running queries for the userid
/queryhud/summaryall : returns the summary Query metrics formatted into a JSON structure 
                       that is easily used by extJS. To get the data, it used the data returned by the
                       existing webservice call: "/DataWave/Query/Metrics/summary"
/queryhud/activeusers: Lists all of the active users.  For normal users this is just their id.
                       Administrators and JBossAdministrators will get all active user IDs



