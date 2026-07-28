# Tablet Extent Checker

This project contains a server utility tool for identifying tablets in a table that are candidates for compaction.

## Building the tool

Building the tool is done with the following command. To run unit tests, omit `-DskipTests`.

```bash
$ mvn clean install -DskipTests
```

This will create the executable jar `target/check-tablet-extents.jar`.

## Running the tool

Copy the jar to the Accumulo lib folder:

```bash
$ cp path/to/check-tablet-extents.jar $ACCUMULO_HOME/lib
```

Then use the following command:

```bash
$ accumulo check-tablets <options>
```

or

```bash
$ accumulo datawave.TabletExtentChecker <options>
```


Use the `--help` option to list all available options:

```bash
$ accumulo check-tablets -h
```

or

```bash
$ accumulo datawave.TabletExtentChecker -h
```

Output:

```bash
Usage: TabletExtentChecker [options]
  Options:
    -b, --begin
      The starting row (exclusive) of the range of tablets to scan
    -c, --compact
      Compact the tablets
      Default: false
    -d, --debug
      Display tool debug info
      Default: false
    -e, --end
      The ending row (inclusive) of the range of tablets to scan
    -h, -?, --help, -help

    -m, --merge, --merge-extents
      Merges suggested compaction ranges for neighboring compactable tablets
      Default: false
    -p, -props, --props
      Sets path to accumulo.properties.The classpath will be searched if this
      property is not set
  * -t, --table
      The table name
    -o
      Overrides configuration set in accumulo.properties (but NOT system-wide
      config set in Zookeeper). Expected format: -o <key>=<value>
      Default: []
```


The tool will list a set of recommended compaction commands if it finds any tablets that require compaction. 
For example, given a table `chop_test` that has tablets with data outside the tablet extents for the following extents:

- Start: null, End: "2500"
- Start: "2500", End: "5000"
- Start: "5000", End: "7500"
- Start: "7500", End: null


You can expect the following output:
```bash
$ accumulo check-tablets -p $ACCUMULO_HOME/conf/accumulo.properties -t chop_test
2026-07-10T09:17:10,431 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.hadoop.io.compress.ZStandardCodec
2026-07-10T09:17:10,433 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.hadoop.io.compress.LzoCodec
2026-07-10T09:17:10,434 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.hadoop.io.compress.BZip2Codec
2026-07-10T09:17:10,436 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.accumulo.core.file.rfile.bcfile.IdentityCodec
2026-07-10T09:17:10,437 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.hadoop.io.compress.Lz4Codec
2026-07-10T09:17:10,438 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.hadoop.io.compress.SnappyCodec
2026-07-10T09:17:10,439 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.hadoop.io.compress.DefaultCodec
2026-07-10T09:17:10,441 [compress.CodecPool] INFO : Got brand-new compressor [.identity]
2026-07-10T09:17:10,658 [zlib.ZlibFactory] INFO : Successfully loaded & initialized native-zlib library
2026-07-10T09:17:10,658 [compress.CodecPool] INFO : Got brand-new decompressor [.deflate]
2026-07-10T09:17:10,659 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.hadoop.io.compress.DefaultCodec
Extent: 1;2500<
First key: 0000 cf:cq [] 1783674529729 false
Last key: 9999 cf:cq [] 1783674529729 false
Extent: 1;5000;2500
First key: 0000 cf:cq [] 1783674529729 false
Last key: 9999 cf:cq [] 1783674529729 false
Extent: 1;7500;5000
First key: 0000 cf:cq [] 1783674529729 false
Last key: 9999 cf:cq [] 1783674529729 false
Extent: 1<;7500
First key: 0000 cf:cq [] 1783674529729 false
Last key: 9999 cf:cq [] 1783674529729 false
compact -t chop_test -e 2500
compact -t chop_test -b 2500 -e 5000
compact -t chop_test -b 5000 -e 7500
compact -t chop_test -b 7500
2026-07-10T09:17:10,709 [metrics.MetricsInfoImpl] INFO : micrometer metrics enabled: false
2026-07-10T09:17:10,709 [metrics.MetricsInfoImpl] INFO : Closing metrics registry
```
The tool recommended multiple compaction ranges for the table.


If the option `--merge-extents` is specified, neighboring compactable tablet extents will be merged within the recommended compaction commands
```bash
accumulo check-tablets -p $ACCUMULO_HOME/conf/accumulo.properties -t chop_test --merge-extents
2026-07-10T09:42:21,296 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.accumulo.core.file.rfile.bcfile.IdentityCodec
2026-07-10T09:42:21,307 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.hadoop.io.compress.BZip2Codec
2026-07-10T09:42:21,308 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.hadoop.io.compress.LzoCodec
2026-07-10T09:42:21,309 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.hadoop.io.compress.DefaultCodec
2026-07-10T09:42:21,311 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.hadoop.io.compress.ZStandardCodec
2026-07-10T09:42:21,312 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.hadoop.io.compress.SnappyCodec
2026-07-10T09:42:21,313 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.hadoop.io.compress.Lz4Codec
2026-07-10T09:42:21,316 [compress.CodecPool] INFO : Got brand-new compressor [.identity]
2026-07-10T09:42:21,527 [zlib.ZlibFactory] INFO : Successfully loaded & initialized native-zlib library
2026-07-10T09:42:21,527 [compress.CodecPool] INFO : Got brand-new decompressor [.deflate]
2026-07-10T09:42:21,528 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.hadoop.io.compress.DefaultCodec
Extent: 1;2500<
First key: 0000 cf:cq [] 1783674529729 false
Last key: 9999 cf:cq [] 1783674529729 false
Extent: 1;5000;2500
First key: 0000 cf:cq [] 1783674529729 false
Last key: 9999 cf:cq [] 1783674529729 false
Extent: 1;7500;5000
First key: 0000 cf:cq [] 1783674529729 false
Last key: 9999 cf:cq [] 1783674529729 false
Extent: 1<;7500
First key: 0000 cf:cq [] 1783674529729 false
Last key: 9999 cf:cq [] 1783674529729 false
compact -t chop_test
2026-07-10T09:42:21,578 [metrics.MetricsInfoImpl] INFO : micrometer metrics enabled: false
2026-07-10T09:42:21,578 [metrics.MetricsInfoImpl] INFO : Closing metrics registry
```
The compaction ranges with contiguous tablets have been combined. The tool recommended one command, `compact -t chop_test` which will compact the entire chop_test table.


If the options `-b 2500` and `-e 7500` are specified, only the suggestions for the rows ranging from 2500 to 7500 will be displayed:
```bash
accumulo check-tablets -p $ACCUMULO_HOME/conf/accumulo.properties -t chop_test -b 2500 -e 7500
2026-07-10T09:46:22,218 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.accumulo.core.file.rfile.bcfile.IdentityCodec
2026-07-10T09:46:22,231 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.hadoop.io.compress.BZip2Codec
2026-07-10T09:46:22,233 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.hadoop.io.compress.DefaultCodec
2026-07-10T09:46:22,234 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.hadoop.io.compress.LzoCodec
2026-07-10T09:46:22,238 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.hadoop.io.compress.Lz4Codec
2026-07-10T09:46:22,240 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.hadoop.io.compress.SnappyCodec
2026-07-10T09:46:22,243 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.hadoop.io.compress.ZStandardCodec
2026-07-10T09:46:22,248 [compress.CodecPool] INFO : Got brand-new compressor [.identity]
2026-07-10T09:46:22,461 [zlib.ZlibFactory] INFO : Successfully loaded & initialized native-zlib library
2026-07-10T09:46:22,461 [compress.CodecPool] INFO : Got brand-new decompressor [.deflate]
2026-07-10T09:46:22,461 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.hadoop.io.compress.DefaultCodec
Extent: 1;5000;2500
First key: 0000 cf:cq [] 1783674529729 false
Last key: 9999 cf:cq [] 1783674529729 false
Extent: 1;7500;5000
First key: 0000 cf:cq [] 1783674529729 false
Last key: 9999 cf:cq [] 1783674529729 false
compact -t chop_test -b 2500 -e 5000
compact -t chop_test -b 5000 -e 7500
2026-07-10T09:46:22,487 [metrics.MetricsInfoImpl] INFO : micrometer metrics enabled: false
2026-07-10T09:46:22,487 [metrics.MetricsInfoImpl] INFO : Closing metrics registry
```
If the --merge-extent option was used as well, the tool would have combined the extents and suggested: `compact -t chop_test -b 2500 -e 7500`.


If the option `--compact` is specified, the tablets would be compacted automatically:
```bash
accumulo check-tablets -p $ACCUMULO_HOME/conf/accumulo.properties -t chop_test -c
2026-07-10T10:03:56,040 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.hadoop.io.compress.LzoCodec
2026-07-10T10:03:56,042 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.hadoop.io.compress.BZip2Codec
2026-07-10T10:03:56,043 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.hadoop.io.compress.SnappyCodec
2026-07-10T10:03:56,045 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.hadoop.io.compress.Lz4Codec
2026-07-10T10:03:56,046 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.accumulo.core.file.rfile.bcfile.IdentityCodec
2026-07-10T10:03:56,047 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.hadoop.io.compress.ZStandardCodec
2026-07-10T10:03:56,048 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.hadoop.io.compress.DefaultCodec
2026-07-10T10:03:56,050 [compress.CodecPool] INFO : Got brand-new compressor [.identity]
2026-07-10T10:03:56,266 [zlib.ZlibFactory] INFO : Successfully loaded & initialized native-zlib library
2026-07-10T10:03:56,266 [compress.CodecPool] INFO : Got brand-new decompressor [.deflate]
2026-07-10T10:03:56,266 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.hadoop.io.compress.DefaultCodec
Extent: 1;2500<
First key: 0000 cf:cq [] 1783674529729 false
Last key: 9999 cf:cq [] 1783674529729 false
Extent: 1;5000;2500
First key: 0000 cf:cq [] 1783674529729 false
Last key: 9999 cf:cq [] 1783674529729 false
Extent: 1;7500;5000
First key: 0000 cf:cq [] 1783674529729 false
Last key: 9999 cf:cq [] 1783674529729 false
Extent: 1<;7500
First key: 0000 cf:cq [] 1783674529729 false
Last key: 9999 cf:cq [] 1783674529729 false
Compacting range from null-2500
Compacting range from 2500-5000
Compacting range from 5000-7500
Compacting range from 7500-null
2026-07-10T10:03:59,007 [metrics.MetricsInfoImpl] INFO : micrometer metrics enabled: false
2026-07-10T10:03:59,007 [metrics.MetricsInfoImpl] INFO : Closing metrics registry
```


We can verify that the compaction was successful by running the tool again:
```bash
accumulo check-tablets -p $ACCUMULO_HOME/conf/accumulo.properties -t chop_test
2026-07-10T10:08:17,880 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.hadoop.io.compress.DefaultCodec
2026-07-10T10:08:17,892 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.hadoop.io.compress.LzoCodec
2026-07-10T10:08:17,893 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.hadoop.io.compress.SnappyCodec
2026-07-10T10:08:17,895 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.accumulo.core.file.rfile.bcfile.IdentityCodec
2026-07-10T10:08:17,895 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.hadoop.io.compress.ZStandardCodec
2026-07-10T10:08:17,896 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.hadoop.io.compress.BZip2Codec
2026-07-10T10:08:17,898 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.hadoop.io.compress.Lz4Codec
2026-07-10T10:08:17,900 [compress.CodecPool] INFO : Got brand-new compressor [.identity]
2026-07-10T10:08:18,123 [zlib.ZlibFactory] INFO : Successfully loaded & initialized native-zlib library
2026-07-10T10:08:18,123 [compress.CodecPool] INFO : Got brand-new decompressor [.deflate]
2026-07-10T10:08:18,124 [bcfile.CompressionAlgorithm] INFO : Trying to load codec class org.apache.hadoop.io.compress.DefaultCodec
Extent: 1;2500<
First key: 0000 cf:cq [] 1783674529729 false
Last key: 2500 cf:cq [] 1783674529729 false
Extent: 1;5000;2500
First key: 2501 cf:cq [] 1783674529729 false
Last key: 5000 cf:cq [] 1783674529729 false
Extent: 1;7500;5000
First key: 5001 cf:cq [] 1783674529729 false
Last key: 7500 cf:cq [] 1783674529729 false
Extent: 1<;7500
First key: 7501 cf:cq [] 1783674529729 false
Last key: 9999 cf:cq [] 1783674529729 false
No candidates suitable for compaction.
2026-07-10T10:08:18,191 [metrics.MetricsInfoImpl] INFO : micrometer metrics enabled: false
2026-07-10T10:08:18,191 [metrics.MetricsInfoImpl] INFO : Closing metrics registry
```
We can see that the tablets now only contain rows within their respective extents, and the tool outputs a message that there are no compactable tablets.