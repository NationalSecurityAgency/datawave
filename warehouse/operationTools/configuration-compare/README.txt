----------------------
1) What is this?
----------------------
This is a tool used for comparing the configurations between two DataWave config.xml files.

The expectations are that each file has a configured 'data.name' field that is also used as the
prefix for other fields.

The tool will report which fields are configured the same (ignoring the differing prefixes), different (fields that are
in both config files but with different values), or only in one of the files.

----------------------
2) Running
----------------------
This can most easily be run with the maven-exec-plugin. Use the following command:

> mvn clean install
> mvn exec:java -Dexec.mainClass=nsa.datawave.configuration.RunCompare -Dexec.args="CONFIG1 CONFIG2"

Where CONFIG1 and CONFIG2 are two local files.

Also, there is a short helper script:

> ./compare.sh CONFIG1 CONFIG2
