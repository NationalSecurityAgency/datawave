This script is deployed to warehouse/scripts/target/classes/bin/index.

This script can be used to view the last time indexes in an accumulo table has been used.
A summary output will be displayed on the console and also in the log.

If an output filename is given then an accumulo shell script will be generated.  The entire
script can be copied and pasted into an accumulo shell and run to remove "old" indexes.  Or
a user can copy and paste a subsection of the output file.  Be sure to copy the line with
the table name.

To run the script do the following
> mvn clean install assembly:single
> ./displayIndexesAge.sh -i instanceName -z zooKeeperServer(s) -t tableName -u userName -c columns <-p password> <-f outputFileName> <-b dayBuckets>
