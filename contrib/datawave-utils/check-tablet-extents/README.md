# Tablet Extent Checker

This project contains a tool for identifying tablets in a table that are candidates for compaction.

## Building the tool

Building the tool is done with the following command. To run unit tests, omit `-DskipTests`.

```bash
$ mvn clean install -DskipTests -P create-shade-jar
```

This will create the executable jar `target/create-tablet-extents-shaded.jar`.

## Running the tool

The tool can be executed with the following command:

```bash
$ java -cp path/to/check-tablet-extents-shaded.jar datawave.TabletExtentChecker <options>
```

Use the `--help` option to list all available options:

```bash
$ java -cp path/to/check-tablet-extents-shaded.jar datawave.TabletExtentChecker -h
Usage: TabletExtentChecker [options]
  Options:
  * -a, --accumulo-instance
      The Accumulo instance.
    -b, --begin
      The starting row (exclusive) of the range of tablets to scan
    -c, --compact
      Compact the tablets
      Default: false
    -e, --end
      The ending row (inclusive) of the range of tablets to scan
    -h, -?, --help, -help

    -m, --merge, --merge-extents
      Merges suggested compaction ranges for neighboring compactable tablets
      Default: false
  * -p, --password
      The Accumulo password. Can be supplied from an environment variable via
      env:ENV_VARIABLE
  * -t, --table
      The table name
  * -u, --username
      The Accumulo username.
  * -z, --zookeper-instance
      The Zookeeper instance.
```

The tool will list a set of recommended compaction commands if it finds any tablets that require compaction. For example, given a table `test_table` that has tablets with data outside the tablet extents for the following extents:
- Start: "2000", End: "3000"
- Start: "3000", End: "5000"
- Start: "8000", End: "9000"

You can expect the following recommended compaction commands.

```bash
$ java -cp target/check-tablet-extents-shaded.jar datawave.TabletExtentChecker -p $ACCUMULO_HOME/conf/accumulo-client.properties -t test_table
SLF4J(W): No SLF4J providers were found.
SLF4J(W): Defaulting to no-operation (NOP) logger implementation
SLF4J(W): See https://www.slf4j.org/codes.html#noProviders for further details.
compact -t chop_test -b 2000 -e 3000
compact -t chop_test -b 3000 -e 5000
compact -t chop_test -b 8000 -e 9000
```

If the option `--merge-extents` was specified above, neighboring compactable tablet extents will be merged within the recommended compaction commands:
```bash
$ java -cp target/check-tablet-extents-shaded.jar datawave.TabletExtentChecker -p $ACCUMULO_HOME/conf/accumulo-client.properties -t test_table
SLF4J(W): No SLF4J providers were found.
SLF4J(W): Defaulting to no-operation (NOP) logger implementation
SLF4J(W): See https://www.slf4j.org/codes.html#noProviders for further details.
compact -t chop_test -b 2000 -e 5000
compact -t chop_test -b 8000 -e 9000
```

If the option `--compact` was specified above, the tablets would be compacted. 
