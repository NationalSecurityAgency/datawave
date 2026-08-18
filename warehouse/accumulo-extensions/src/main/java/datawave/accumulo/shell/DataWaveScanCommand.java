package datawave.accumulo.shell;

import java.io.UnsupportedEncodingException;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.shell.commands.OptUtil;
import org.apache.accumulo.shell.commands.ScanCommand;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;

/**
 * A scan command that restricts the scan range itself to a column family and column qualifier rather than filtering columns out of a whole-row scan.
 * <p>
 * Row and column values are decoded by {@link KeyEscapes} so that null-delimited DataWave key components can be typed, as in {@code -cf datatype\0uid}. The
 * last component named may be partial, and bounds the range the way a partial key does when building a {@link Range} by hand: the scan starts at that key and
 * runs through every key sorting under it, so {@code -cf tf -cq datatype\0uid} reaches {@code tf:datatype\0uid\0FIELD\0value}. Naming a qualifier therefore
 * makes the column family fully qualified, since only one component can carry the bound. Pass {@code -ee} to stop at the end key literally instead. Scan
 * interpreters, which the stock command applies, are deprecated in Accumulo 2.1 and are not applied here.
 */
public class DataWaveScanCommand extends ScanCommand {

    private static final String ROW_OPT = "r";
    private static final String COLUMNS_OPT = "c";
    private static final String COLUMN_FAMILY_OPT = "cf";
    private static final String COLUMN_QUALIFIER_OPT = "cq";
    private static final String BEGIN_EXCLUSIVE_OPT = "be";
    private static final String END_EXCLUSIVE_OPT = "ee";

    private static final String BEGIN_KEY_CF_OPT = "bkcf";
    private static final String BEGIN_KEY_CQ_OPT = "bkcq";
    private static final String BEGIN_KEY_TS_OPT = "bkts";
    private static final String END_KEY_CF_OPT = "ekcf";
    private static final String END_KEY_CQ_OPT = "ekcq";
    private static final String END_KEY_TS_OPT = "ekts";
    private static final String NO_ESCAPES_OPT = "no-escapes";

    private static final String[] BEGIN_KEY_OPTS = {BEGIN_KEY_CF_OPT, BEGIN_KEY_CQ_OPT, BEGIN_KEY_TS_OPT};
    private static final String[] END_KEY_OPTS = {END_KEY_CF_OPT, END_KEY_CQ_OPT, END_KEY_TS_OPT};

    private static final Text EMPTY = new Text();

    /**
     * One end of the scan range below the row, as requested on the command line.
     */
    private static class ColumnBound {
        private Text cf;
        private Text cq;
        private Long ts;

        private boolean isEmpty() {
            return cf == null && cq == null && ts == null;
        }
    }

    @Override
    public String getName() {
        return "scan";
    }

    @Override
    public String description() {
        return "scans a table, restricting the scan range to the requested column family and column qualifier";
    }

    @Override
    public Options getOptions() {
        Options o = super.getOptions();

        o.addOption(argOption(BEGIN_KEY_CF_OPT, "begin-key-cf", "column family the scan range starts at"));
        o.addOption(argOption(BEGIN_KEY_CQ_OPT, "begin-key-cq", "column qualifier the scan range starts at"));
        o.addOption(argOption(BEGIN_KEY_TS_OPT, "begin-key-ts", "timestamp the scan range starts at"));
        o.addOption(argOption(END_KEY_CF_OPT, "end-key-cf", "column family the scan range ends at"));
        o.addOption(argOption(END_KEY_CQ_OPT, "end-key-cq", "column qualifier the scan range ends at"));
        o.addOption(argOption(END_KEY_TS_OPT, "end-key-ts", "timestamp the scan range ends at"));
        o.addOption(new Option(null, NO_ESCAPES_OPT, false, "take row and column values literally instead of decoding \\0, \\xHH, \\n, \\r, \\t and \\\\"));

        // the superclass registers these only for the commands it ships, and this command inherits its execute() unchanged
        o.addOption(argOption("o", "output", "local file to write the scan output to"));
        o.addOption(showFewOpt);

        return o;
    }

    @Override
    protected Range getRange(final CommandLine cl, @SuppressWarnings("deprecation") final org.apache.accumulo.core.util.interpret.ScanInterpreter interpreter)
                    throws UnsupportedEncodingException {
        if (cl.hasOption(ROW_OPT) && (cl.hasOption(OptUtil.START_ROW_OPT) || cl.hasOption(OptUtil.END_ROW_OPT))) {
            throw new IllegalArgumentException(
                            "Options -" + ROW_OPT + " AND (-" + OptUtil.START_ROW_OPT + " OR -" + OptUtil.END_ROW_OPT + ") are mutually exclusive ");
        }
        validate(cl);

        Text row = decode(cl, ROW_OPT);
        Text beginRow = (row != null) ? row : decode(cl, OptUtil.START_ROW_OPT);
        Text endRow = (row != null) ? row : decode(cl, OptUtil.END_ROW_OPT);

        boolean explicitBounds = hasAny(cl, BEGIN_KEY_OPTS) || hasAny(cl, END_KEY_OPTS);
        ColumnBound begin = bound(cl, BEGIN_KEY_OPTS, row, explicitBounds);
        ColumnBound end = bound(cl, END_KEY_OPTS, row, explicitBounds);
        if ((!begin.isEmpty() && beginRow == null) || (!end.isEmpty() && endRow == null)) {
            throw new IllegalArgumentException(
                            "Scoping a scan to a column requires a row, specify -" + ROW_OPT + ", -" + OptUtil.START_ROW_OPT + " or -" + OptUtil.END_ROW_OPT);
        }

        // -be and -ee exclude an endpoint, and the stock command ignores them when -r pins the scan to one row
        boolean beginInclusive = !cl.hasOption(BEGIN_EXCLUSIVE_OPT) || (row != null && begin.isEmpty());
        boolean endInclusive = !cl.hasOption(END_EXCLUSIVE_OPT) || (row != null && end.isEmpty());

        Key startKey = null;
        if (beginRow != null) {
            if (begin.isEmpty()) {
                startKey = beginInclusive ? new Key(beginRow) : new Key(beginRow).followingKey(PartialKey.ROW);
                beginInclusive = true;
            } else {
                startKey = new Key(beginRow, orEmpty(begin.cf), orEmpty(begin.cq), (begin.ts == null) ? Long.MAX_VALUE : begin.ts);
            }
        }

        Key endKey = null;
        if (endRow != null) {
            endKey = endKey(endRow, end, endInclusive);
            endInclusive = false;
        }

        return new Range(startKey, beginInclusive, endKey, endInclusive);
    }

    @Override
    protected void fetchColumns(final CommandLine cl, final ScannerBase scanner,
                    @SuppressWarnings("deprecation") final org.apache.accumulo.core.util.interpret.ScanInterpreter interpreter)
                    throws UnsupportedEncodingException {
        if (cl.hasOption(COLUMN_FAMILY_OPT) || cl.hasOption(COLUMN_QUALIFIER_OPT)) {
            if (rangeIsScopedToColumn(cl)) {
                // the range covers the requested column; fetching it as well would match the value exactly and drop the longer keys sorting under it
                return;
            }
            Text cf = decode(cl, COLUMN_FAMILY_OPT);
            Text cq = decode(cl, COLUMN_QUALIFIER_OPT);
            if (cq == null) {
                scanner.fetchColumnFamily(cf);
            } else {
                scanner.fetchColumn(cf, cq);
            }
        } else if (cl.hasOption(COLUMNS_OPT)) {
            for (String column : cl.getOptionValue(COLUMNS_OPT).split(",")) {
                String[] parts = column.split(":", 2);
                if (parts.length == 1) {
                    scanner.fetchColumnFamily(decodeValue(cl, parts[0]));
                } else {
                    scanner.fetchColumn(decodeValue(cl, parts[0]), decodeValue(cl, parts[1]));
                }
            }
        }
    }

    /**
     * Reports whether {@link #getRange(CommandLine, org.apache.accumulo.core.util.interpret.ScanInterpreter)} scoped the range to the column named by -cf and
     * -cq, which is the case when a single row was given and no explicit endpoint overrode it.
     *
     * @param cl
     *            the parsed command line
     * @return true if the range already restricts the scan to the requested column
     */
    protected boolean rangeIsScopedToColumn(final CommandLine cl) {
        return cl.hasOption(ROW_OPT) && cl.hasOption(COLUMN_FAMILY_OPT) && !hasAny(cl, BEGIN_KEY_OPTS) && !hasAny(cl, END_KEY_OPTS);
    }

    private void validate(final CommandLine cl) {
        if (cl.hasOption(COLUMNS_OPT) && (cl.hasOption(COLUMN_FAMILY_OPT) || cl.hasOption(COLUMN_QUALIFIER_OPT))) {
            throw new IllegalArgumentException(
                            "Option -" + COLUMNS_OPT + " is mutually exclusive with options -" + COLUMN_FAMILY_OPT + " and -" + COLUMN_QUALIFIER_OPT + ".");
        }
        if (cl.hasOption(COLUMN_QUALIFIER_OPT) && !cl.hasOption(COLUMN_FAMILY_OPT)) {
            throw new IllegalArgumentException("Option -" + COLUMN_FAMILY_OPT + " is required when using -" + COLUMN_QUALIFIER_OPT + ".");
        }
        if (cl.hasOption(BEGIN_KEY_CQ_OPT) && !cl.hasOption(BEGIN_KEY_CF_OPT)) {
            throw new IllegalArgumentException("Option -" + BEGIN_KEY_CF_OPT + " is required when using -" + BEGIN_KEY_CQ_OPT + ".");
        }
        if (cl.hasOption(END_KEY_CQ_OPT) && !cl.hasOption(END_KEY_CF_OPT)) {
            throw new IllegalArgumentException("Option -" + END_KEY_CF_OPT + " is required when using -" + END_KEY_CQ_OPT + ".");
        }
    }

    /**
     * Builds one end of the range below the row. Explicit begin or end key options win; otherwise a column named alongside a single row scopes both ends alike.
     *
     * @param cl
     *            the parsed command line
     * @param keyOpts
     *            the begin or end key options to read the endpoint from
     * @param row
     *            the single row being scanned, or null if a row range was given
     * @param explicitBounds
     *            whether either end of the range was set with the begin or end key options, in which case -cf and -cq only filter columns
     * @return the requested endpoint, empty if the scan is not scoped below the row on this side
     */
    private ColumnBound bound(final CommandLine cl, final String[] keyOpts, final Text row, final boolean explicitBounds) {
        ColumnBound bound = new ColumnBound();
        if (hasAny(cl, keyOpts)) {
            bound.cf = decode(cl, keyOpts[0]);
            bound.cq = decode(cl, keyOpts[1]);
            bound.ts = timestamp(cl, keyOpts[2]);
        } else if (explicitBounds) {
            return bound;
        } else if (row != null && cl.hasOption(COLUMN_FAMILY_OPT)) {
            bound.cf = decode(cl, COLUMN_FAMILY_OPT);
            bound.cq = decode(cl, COLUMN_QUALIFIER_OPT);
        }
        return bound;
    }

    /**
     * Computes the exclusive key the scan stops at. The last component named may be a partial value, so an inclusive endpoint runs past everything sorting
     * under it rather than stopping at the value itself, which is what lets a partial qualifier such as {@code value} reach {@code value\0datatype\0uid}. Only
     * that last component is extended, so naming a qualifier holds the column family exact.
     *
     * @param endRow
     *            the last row of the scan
     * @param end
     *            the requested endpoint below the row
     * @param inclusive
     *            whether keys sorting under the endpoint should be included
     * @return the exclusive end key
     */
    private Key endKey(final Text endRow, final ColumnBound end, final boolean inclusive) {
        if (end.isEmpty()) {
            return inclusive ? new Key(endRow).followingKey(PartialKey.ROW) : new Key(endRow);
        }

        Key key = new Key(endRow, orEmpty(end.cf), orEmpty(end.cq), (end.ts == null) ? Long.MAX_VALUE : end.ts);
        if (!inclusive) {
            return key;
        }
        if (end.ts != null) {
            // a timestamp is a fixed-width long rather than a partial value, and timestamps sort descending
            return key.followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME);
        }
        if (end.cq != null) {
            Text following = Range.followingPrefix(end.cq);
            return (following == null) ? key.followingKey(PartialKey.ROW_COLFAM) : new Key(endRow, orEmpty(end.cf), following);
        }
        Text following = Range.followingPrefix(end.cf);
        return (following == null) ? new Key(endRow).followingKey(PartialKey.ROW) : new Key(endRow, following);
    }

    private boolean hasAny(final CommandLine cl, final String[] opts) {
        for (String opt : opts) {
            if (cl.hasOption(opt)) {
                return true;
            }
        }
        return false;
    }

    private Long timestamp(final CommandLine cl, final String opt) {
        String value = cl.getOptionValue(opt);
        if (value == null) {
            return null;
        }
        try {
            return Long.valueOf(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Option -" + opt + " requires a timestamp in milliseconds, got: " + value, e);
        }
    }

    private Text decode(final CommandLine cl, final String opt) {
        String value = cl.getOptionValue(opt);
        return (value == null) ? null : decodeValue(cl, value);
    }

    private Text decodeValue(final CommandLine cl, final String value) {
        return KeyEscapes.decode(value, !cl.hasOption(NO_ESCAPES_OPT));
    }

    private Text orEmpty(final Text value) {
        return (value == null) ? EMPTY : value;
    }

    private Option argOption(final String opt, final String longOpt, final String description) {
        Option option = new Option(opt, longOpt, true, description);
        option.setArgName(longOpt);
        return option;
    }
}
