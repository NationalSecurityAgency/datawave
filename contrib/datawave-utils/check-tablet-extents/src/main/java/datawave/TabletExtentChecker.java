package datawave;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.spi.crypto.NoCryptoServiceFactory;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.accumulo.tserver.tablet.Tablet;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.auto.service.AutoService;

/**
 * A utility that will identify tablets that require compaction within a given table for a given range and lists recommended compaction commands.
 */
@AutoService(KeywordExecutable.class)
public final class TabletExtentChecker implements KeywordExecutable {

    /**
     * Converts a string argument to a {@link Text} instance.
     */
    private static class TextConverter implements IStringConverter<Text> {
        @Override
        public Text convert(String s) {
            return s == null ? null : new Text(s);
        }
    }

    /**
     * Represents a set of options that can parsed and used via {@link TabletExtentChecker#execute(String[])}. ConfigOpts provides getSiteConfiguration() to be
     * used to create ServerContext
     */
    public static class Opts extends ConfigOpts {
        @Parameter(names = {"-t", "--table"}, description = "The table name", required = true)
        public String tableName = null;

        @Parameter(names = {"-b", "--begin"}, description = "The starting row (exclusive) of the range of tablets to scan", converter = TextConverter.class)
        public Text beginRow = null;

        @Parameter(names = {"-e", "--end"}, description = "The ending row (inclusive) of the range of tablets to scan", converter = TextConverter.class)
        public Text endRow = null;

        @Parameter(names = {"-m", "--merge", "--merge-extents"}, description = "Merges suggested compaction ranges for neighboring compactable tablets")
        public boolean mergeExtents = false;

        @Parameter(names = {"-c", "--compact"}, description = "Compact the tablets")
        public boolean compactTablets = false;

        @Parameter(names = {"-d", "--debug"}, description = "Display tool debug info")
        public boolean debug = false;

        /**
         * Parse the given arguments array and populate this {@link Opts}.
         *
         * @param args
         *            the arguments to parse.
         */
        @Override
        public void parseArgs(String programName, String[] args, Object... others) {
            JCommander commander = JCommander.newBuilder().addObject(this).programName(TabletExtentChecker.class.getSimpleName()).build();

            try {
                commander.parse(args);
            } catch (ParameterException ex) {
                System.err.println("ERROR :" + ex.getMessage());
                commander.usage();
                System.exit(1);
            }

            if (help) {
                commander.usage();
                System.exit(0);
            }
        }
    }


    /**
     * @return the keyword to use this tool on the command line using $ accumulo {keyword}
     */
    @Override
    public String keyword() {
        return "check-tablets";
    }

    /**
     * @return the description of this tablet extent checker tool
     */
    @Override
    public String description() {
        return "Identifies tablets with data outside their extents and if specified, compacts them.";
    }

    public static void main(String[] args) throws AccumuloException, TableNotFoundException, IOException, AccumuloSecurityException {
        new TabletExtentChecker().execute(args);
    }

    /**
     * Parses the user-provided arguments and prints out recommended ranges for compaction.
     *
     * @param args
     *            the arguments
     * @throws AccumuloException
     *             if an error occurs while connecting to Accumulo
     * @throws TableNotFoundException
     *             if the specified table does not exist
     * @throws IOException
     *             if an error occurs while reading the client properties file
     */
    @Override
    public void execute(String[] args) throws AccumuloException, TableNotFoundException, IOException, AccumuloSecurityException {
        // Parse the arguments.
        Opts opts = new Opts();

        opts.parseArgs(TabletExtentChecker.class.getName(), args);

        try (ServerContext context = new ServerContext(opts.getSiteConfiguration())) {
            // Fetch the recommended tablet ranges to compact.
            checkTablets(context, opts);
        }
    }

    /**
     *
     * @param context
     *             the context to use when connecting to Accumulo
     * @param opts
     *             the Opts object providing options to configure the tool
     * @throws AccumuloException
     *             if an error occurs while connecting to Accumulo
     * @throws TableNotFoundException
     *             if the specified table does not exist
     * @throws IOException
     *             if an error occurs while reading the client properties file
     * @throws AccumuloSecurityException
     *             if an error occurs during authentication
     */
    public static void checkTablets(ClientContext context, Opts opts) throws AccumuloException, TableNotFoundException, IOException, AccumuloSecurityException {
        List<Pair<Text,Text>> compactionExtents = findCompactableTablets(context, opts);

        // Print a message when compactionExtents is empty
        if (compactionExtents.isEmpty()) {
            System.out.println("No candidates suitable for compaction.");
        } else {
            for (Pair<Text,Text> pair : compactionExtents) {
                Text startRow = pair.getLeft();
                Text endRow = pair.getRight();
                if (opts.compactTablets) {
                    System.out.println("Compacting range from " + startRow + "-" + endRow);
                    context.tableOperations().compact(opts.tableName, startRow, endRow, true, true);
                } else {
                    System.out.println("compact -t " + opts.tableName + formatArg("-b", startRow) + formatArg("-e", endRow));
                }
            }
        }
    }

    /**
     * Return the given value formatted as an argument snippet if the value is not null.
     *
     * @param arg
     *            the argument option
     * @param value
     *            the value
     * @return the formatted arg to value command snippet if the value is not null, otherwise returns an empty string
     */
    private static String formatArg(String arg, Text value) {
        if (value == null) {
            return "";
        } else {
            return " " + arg + " " + value;
        }
    }

    /**
     * Returns a list of pairs consisting of the extents of tablets that require compaction.
     *
     * @param context
     *         the context to use when connecting to Accumulo
     * @param opts
     *         the Opts object providing options to configure the tool
     * @return the list of tablet boundaries recommended for compaction
     * @throws AccumuloException
     *             if an error occurs while connecting to Accumulo
     * @throws TableNotFoundException
     *             if the specified table does not exist
     * @throws IOException
     *             if an error occurs while reading the client properties file
     */
    static List<Pair<Text,Text>> findCompactableTablets(ClientContext context, Opts opts) throws AccumuloException, TableNotFoundException, IOException {
        List<Pair<Text,Text>> compactionExtents = new ArrayList<>();

        TableId tableId = getTableId(context, opts.tableName);
        // Fetch the metadata for all tablets in the given table whose extents overlap with the user provided range of tablets to scan.
        try (TabletsMetadata tablets = context.getAmple().readTablets().forTable(tableId).overlapping(opts.beginRow, false, opts.endRow)
                        .fetch(TabletMetadata.ColumnType.PREV_ROW, TabletMetadata.ColumnType.FILES).build()) {

            // Tracks compaction ranges if we are merging extents.
            boolean foundCompactableTablet = false;
            Text compactionStart = null;
            Text compactionEnd = null;

            // Iterate over each tablet.
            for (TabletMetadata tablet : tablets) {
                // Determine whether the tablet needs compaction.
                boolean tabletRequiresCompaction = tabletRequiresCompaction(context, opts.tableName, tablet, opts.debug);
                KeyExtent extent = tablet.getExtent();

                // The current tablet requires compaction.
                if (tabletRequiresCompaction) {
                    // If we are merging extents, update the current compaction start and end based on whether the previous tablet also required compaction.
                    if (opts.mergeExtents) {
                        // The previous tablet did not require compaction. Update the compaction range to reflect the current tablet.
                        if (!foundCompactableTablet) {
                            compactionStart = extent.prevEndRow();
                            compactionEnd = extent.endRow();
                            foundCompactableTablet = true;
                        } else {
                            // The previous tablet needs compaction, along with the current tablet. Update the end row.
                            compactionEnd = extent.endRow();
                        }
                    } else {
                        compactionExtents.add(Pair.of(extent.prevEndRow(), extent.endRow()));
                    }
                    // The current tablet does not require compaction.
                } else {
                    // We are merging tablet extents, and the previous tablet requires compaction.
                    if (opts.mergeExtents && foundCompactableTablet) {
                        // Add a new recommended compaction range and reset our compaction boundaries.
                        compactionExtents.add(Pair.of(compactionStart, compactionEnd));
                        compactionStart = null;
                        compactionEnd = null;
                        foundCompactableTablet = false;
                    }
                }
            }
            // Handle case where last tablet needs compaction when we are merging extents.
            if (opts.mergeExtents && foundCompactableTablet) {
                compactionExtents.add(Pair.of(compactionStart, compactionEnd));
            }
        }
        return compactionExtents;
    }

    /**
     * Return the table ID for the given table
     *
     * @param context
     *            the context to use when connecting to Accumulo
     * @param tableName
     *            the table name
     * @return the table ID
     */
    private static TableId getTableId(ClientContext context, String tableName) {
        TableOperations tableOperations = context.tableOperations();
        if (tableOperations.exists(tableName)) {
            return TableId.of(tableOperations.tableIdMap().get(tableName));
        } else {
            throw new IllegalArgumentException("Table " + tableName + " does not exist");
        }
    }

    /**
     * Return whether the given tablet requires compaction. This method represents a merging of the functions
     * {@link org.apache.accumulo.tserver.tablet.CompactableUtils#getFirstAndLastKeys(Tablet, Set)} and
     * {@link org.apache.accumulo.tserver.tablet.CompactableUtils#findChopFiles(KeyExtent, Map, Collection)} that is designed to return true as soon as we find
     * an RFile that is empty, or whose first or last keys fall outside the tablet's extent.
     *
     * @param context
     *            the context to use when connecting to Accumulo
     * @param tableName
     *            the tablet name
     * @param tablet
     *            the tablet metadata
     * @param debug
     *            the flag to control logging of this tool
     * @return true if the tablet requires compaction, or false
     */
    private static boolean tabletRequiresCompaction(ClientContext context, String tableName, TabletMetadata tablet, boolean debug)
                    throws AccumuloException, TableNotFoundException, IOException {
        // Fetch the list of RFiles for the tablet.
        ConfigurationCopy tableConf = new ConfigurationCopy(context.tableOperations().getConfiguration(tableName));
        KeyExtent extent = tablet.getExtent();
        Set<StoredTabletFile> allFiles = new HashSet<>(tablet.getFiles());
        final FileOperations fileFactory = FileOperations.getInstance();

        // Examine each file and determine whether any of them would be cleaned up/optimized by a compaction.
        for (StoredTabletFile file : allFiles) {
            FileSystem ns = FileSystem.get(file.getPath().toUri(), context.getHadoopConf());
            try (FileSKVIterator openReader = fileFactory.newReaderBuilder().forFile(file.getPathStr(), ns, ns.getConf(), NoCryptoServiceFactory.NONE)
                            .withTableConfiguration(tableConf).seekToBeginning().build()) {
                Key first = openReader.getFirstKey();
                Key last = openReader.getLastKey();

                if (debug) {
                    System.out.println("Extent: " + extent);
                    System.out.println("First key: " + first);
                    System.out.println("Last key: " + last);
                }

                // A tablet requires a compaction if any of the following are true:
                // - The first key is outside the tablet's extent.
                // - The last key is outside the tablet's extent.

                if ((first != null && !extent.contains(first.getRow())) || (last != null && !extent.contains(last.getRow()))) {
                    return true;
                }
            }
        }
        return false;
    }
}
