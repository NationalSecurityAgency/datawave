package datawave.next;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.query.Constants;
import datawave.query.parser.JavaRegexAnalyzer;

/**
 * A document id iterator that supports regex terms
 */
public class RegexDocIdIterator extends DocIdIterator {

    private static final Logger log = LoggerFactory.getLogger(RegexDocIdIterator.class);

    private String literal = null;
    private Pattern pattern;
    private Matcher matcher;

    private Key stopKey;

    /**
     * Constructor that accepts an {@link ASTERNode} node
     *
     * @param source
     *            the source
     * @param row
     *            the row
     * @param node
     *            the JexlNode
     */
    public RegexDocIdIterator(SortedKeyValueIterator<Key,Value> source, String row, JexlNode node) {
        super(source, row, node);
    }

    @Override
    public boolean hasNext() {
        if (!seenSeek) {
            handleFirstSeek();
        }

        while (tk == null && source.hasTop()) {
            Key top = source.getTopKey();
            parser.parse(top);

            // check for min/max datatype and uid
            if (!matchesMinMaxDatatypeUid()) {
                handleMinMaxDatatypeUidMiss();
                continue;
            }

            // apply datatype filter
            if (!acceptDatatype(parser.getDatatype())) {
                handleDatatypeMiss();
                continue;
            }

            // ensure key is within the requested date range
            if (timeFilter != null && !timeFilter.contains(top.getTimestamp())) {
                stats.incrementTimeFilterMiss();
                safeNext();
                continue;
            }

            if (suffix != null && !suffix.endsWith(parser.getUid())) {
                // TODO: this could be a seek to the dt\0uid, or a rollover seek to the next value
                // for now just advance the iterator
                safeNext();
            }

            // apply regex
            if (matcher == null) {
                matcher = pattern.matcher(parser.getValue());
            } else {
                matcher.reset(parser.getValue());
            }

            if (!matcher.matches()) {
                stats.incrementRegexMiss();
                safeNext();
                continue;
            }

            tk = fiToEvent(top);

            safeNext();
        }

        if (tk == null) {
            // iterator exhausted, log stats
            log.info("stats for term: [{}]: {}", getNode(), stats);
        }

        return tk != null;
    }

    /**
     * Regex range is built without the datatype filter
     *
     * @return the scan range
     */
    protected Range createScanRange() {
        try {
            JavaRegexAnalyzer analyzer = new JavaRegexAnalyzer(value);
            pattern = Pattern.compile(analyzer.getRegex());

            if (analyzer.isLeadingLiteral()) {
                // build a range restricted by the literal
                literal = analyzer.getLeadingLiteral();
                Key start = new Key(row, prefix, literal);
                stopKey = new Key(row, prefix, literal + '\uffff');
                return new Range(start, true, stopKey, false);
            } else {
                // otherwise this is a leading regex, and we must scan the entire fi\0FIELD column
                Key start = new Key(row, prefix);
                stopKey = new Key(row, prefix, Constants.MAX_UNICODE_STRING);
                return new Range(start, true, stopKey, false);
            }

        } catch (JavaRegexAnalyzer.JavaRegexParseException e) {
            throw new RuntimeException("Failed to analyze regex: " + value, e);
        }
    }

    protected boolean matchesMinMaxDatatypeUid() {
        if (minDatatypeUid != null && maxDatatypeUid != null) {
            String dtUid = parser.getDatatype() + '\u0000' + parser.getUid();
            return dtUid.compareTo(minDatatypeUid) >= 0 && dtUid.compareTo(maxDatatypeUid) <= 0;
        }
        return true;
    }

    protected void handleMinMaxDatatypeUidMiss() {
        // this is a miss on a min/max bound, but consider this a datatype miss for stat tracking
        stats.incrementDatatypeFilterMiss();
        String dtUid = parser.getDatatype() + '\u0000' + parser.getUid();
        if (dtUid.compareTo(minDatatypeUid) < 0) {
            seekToMinDatatypeUid();
        } else if (dtUid.compareTo(maxDatatypeUid) > 0) {
            seekToNextValue();
        } else {
            throw new IllegalStateException("Error");
        }
    }

    @Override
    protected void handleDatatypeFilterMiss(String datatype) {
        if (literal == null) {
            handleDatatypeFilterMissForLeadingRegex(datatype);
        } else {
            handleDatatypeFilterMissForTrailingRegex(datatype);
        }
    }

    protected void handleDatatypeFilterMissForLeadingRegex(String datatype) {
        log.info("datatype filter miss for leading regex");
        String nextDatatype = datatypeFilter.higher(datatype);
        if (nextDatatype == null) {
            seekToNextValue();
        }

        // advance to next highest datatype
        Key start = new Key(row, prefix, parser.getValue() + '\u0000' + nextDatatype);
        Range range = new Range(start, false, stopKey, true);
        safeSeek(range, true);
    }

    protected void handleDatatypeFilterMissForTrailingRegex(String datatype) {
        log.info("datatype filter miss for trailing regex");
        String nextDatatype = datatypeFilter.higher(datatype);
        if (nextDatatype == null) {
            seekToNextValue();
        }

        // advance to next highest datatype
        Key start = new Key(row, prefix, parser.getValue() + '\u0000' + nextDatatype);
        Range range = new Range(start, false, stopKey, true);
        safeSeek(range, true);
    }

    @Override
    protected void handleMinMaxDatatypeFilterMiss(String datatype) {
        if (literal == null) {
            handleMinMaxDatatypeFilterMissForLeadingRegex(datatype);
        } else if (minDatatype != null && maxDatatype != null) {
            handleMinMaxDatatypeFilterMissForTrailingRegex(datatype);
        } else {
            // advance via next on datatype miss
            safeNext();
        }
    }

    /**
     * Min and max datatype, leading regex means no literal is available
     *
     * @param datatype
     *            the datatype
     */
    protected void handleMinMaxDatatypeFilterMissForLeadingRegex(String datatype) {
        log.info("min/max datatype filter miss for leading regex");
        if (datatype.compareTo(minDatatype) < 0) {
            seekToMostSelectiveMinimum();
        } else if (datatype.compareTo(maxDatatype) > 0) {
            seekToNextValue();
        }
    }

    protected void handleMinMaxDatatypeFilterMissForTrailingRegex(String datatype) {
        log.info("min/max datatype filter miss for trailing regex");
        if (datatype.compareTo(minDatatype) < 0) {
            seekToMostSelectiveMinimum();
        } else if (datatype.compareTo(maxDatatype) > 0) {
            seekToNextValue();
        }
    }

    protected void seekToMostSelectiveMinimum() {
        if (minDatatypeUid != null) {
            seekToMinDatatypeUid();
        } else if (minDatatype != null) {
            seekToMinDatatype();
        } else {
            throw new IllegalStateException("Cannot seek to most selective minimum");
        }
    }

    protected void seekToMinDatatype() {
        Key start = new Key(row, prefix, parser.getValue() + '\u0000' + minDatatype);
        Range range = new Range(start, false, stopKey, true);
        safeSeek(range, true);
    }

    /**
     * Seek to the minimum datatype and uid (lower bound)
     */
    protected void seekToMinDatatypeUid() {
        // advance to next highest datatype
        Key start = new Key(row, prefix, parser.getValue() + '\u0000' + minDatatypeUid);
        Range range = new Range(start, true, stopKey, true);
        safeSeek(range, true);
    }

    /**
     * Seek to the next value
     */
    protected void seekToNextValue() {
        Key start = new Key(row, prefix, parser.getValue() + '\u0000' + '\uffff');
        Range range = new Range(start, false, stopKey, true);
        safeSeek(range, true);
    }
}
