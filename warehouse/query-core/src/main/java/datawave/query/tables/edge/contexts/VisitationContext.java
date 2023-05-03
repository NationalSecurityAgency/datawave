package datawave.query.tables.edge.contexts;

import com.google.common.collect.HashMultimap;
import datawave.edge.model.EdgeModelAware;
import datawave.edge.util.EdgeKeyUtil;
import datawave.query.parser.JavaRegexAnalyzer;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class is used to store and build 3 things for an edge query. 1) The normalized edge relationship query 2) The normalized stats edge query 3) The
 * Accumulo ranges
 * <p>
 * Since relationship edges and stats edges are structured differently its difficult to write a single query that correctly evaluates against both. (Ex stats
 * edges don't have SINKs, they only have 1 EDGE_RELATIONSHIP and 1 EDGE_ATTRIBUTE1) So two query strings are created from the original query, one to be used
 * when evaluating relationship edges and one to be used when evaluating stats edges.
 * <p>
 * Further more the original query must be run through a set of normalizers so that the query can correctly get results from the table without having to worry
 * about capitalization or other formatting details.
 */
public class VisitationContext implements EdgeModelAware, EdgeContext {
    private static final Logger log = Logger.getLogger(VisitationContext.class);

    StringBuilder normalizedQuery;
    StringBuilder normalizedStatsQuery;
    protected Set<Range> ranges;
    protected List<Text> columnFamilies;
    protected boolean hasAllCompleteColumnFamilies = false;

    private HashMultimap<String, String> preFilterValues = HashMultimap.create();
    long termCount = 0;

    protected boolean includeStats;

    private static final String OR = " || ";
    private static final String AND = " && ";

    public VisitationContext() {
        normalizedQuery = new StringBuilder();
        normalizedStatsQuery = new StringBuilder();
        ranges = new HashSet<>();

        this.includeStats = true;
    }

    public VisitationContext(boolean includeStats) {

        normalizedQuery = new StringBuilder();
        normalizedStatsQuery = new StringBuilder();
        ranges = new HashSet<>();

        this.includeStats = includeStats;

    }

    public void buildColumnFamilyList(QueryContext qContext, boolean includeStats) {

        if (hasAllCompleteColumnFamilies) {
            if (columnFamilies == null) {
                columnFamilies = new ArrayList<>();
            }
            columnFamilies.addAll(qContext.getColumnFamilies(includeStats));
        } else {
            throw new RuntimeException("Query does not have all complete column families, cannot use this method");
        }
    }

    /**
     * Builds two query strings for relationship and stats edges Only include SOURCE if there is a regex SOURCE Only include SINKs if there is a regex SOURCE or
     * SINK Always include everything else Edge type, relationship ,...
     * <p>
     * includingSources and includingSinks are used to remember if a previous qContext had a regex for SOURCE and SINK, if so then we have to include SOURCEs
     * and SINKs for every Query context regardless of weather or not they all have regex's.
     *
     * @param qContext                query context
     * @param includColumnFamilyTerms whether to include column family terms
     * @param includeSinks            flag to include the sinks
     * @param includeSources          flag to include sources
     * @param updateAllowlist         flag to update allowlist
     */
    public void updateQueryStrings(QueryContext qContext, boolean includeSources, boolean includeSinks, boolean includColumnFamilyTerms, boolean updateAllowlist) {
        StringBuilder trimmedQuery = new StringBuilder();
        StringBuilder trimmedStatsQuery = new StringBuilder();
        trimmedQuery.append("(");
        if (includeStats) {
            trimmedStatsQuery.append("(");
        }

        qContext.buildStrings(trimmedQuery, trimmedStatsQuery, includeStats, includeSources, includeSinks, preFilterValues, includColumnFamilyTerms,
                updateAllowlist);
        trimmedQuery.append(")");
        if (includeStats) {
            trimmedStatsQuery.append(")");
        }

        if (trimmedQuery.length() > 7) {
            if (this.getNormalizedQuery().length() > 7) {
                this.getNormalizedQuery().append(OR);
            }
            this.getNormalizedQuery().append(trimmedQuery);
        }

        if (includeStats && trimmedStatsQuery.length() > 7) {
            if (this.getNormalizedStatsQuery().length() > 7) {
                this.getNormalizedStatsQuery().append(OR);
            }
            this.getNormalizedStatsQuery().append(trimmedStatsQuery);
        }

    }

    /**
     * Takes a list of IdentityContext's containing all sources and another containing all sinks as input and returns a set of ranges Loops through each
     * provided SOURCE IdentityContext. If the SOURCE is not a regex and there are SINKs then build the range using the SOURCE and SINK else build the range
     * with just the SOURCE.
     *
     * @param sinks   list of sinks
     * @param sources list of sources
     * @return set of ranges
     */
    private Set<Range> buildRanges(List<IdentityContext> sources, List<IdentityContext> sinks) {
        Set<Range> ranges = new HashSet<>();
        if (sources == null || sources.isEmpty()) {
            throw new RuntimeException("Can't build ranges for given query. There must be a SOURCE.");
        }
        for (IdentityContext source : sources) {
            if (source.isEquivalence()) {
                if (sinks != null && !source.getOperation().equals(EQUALS_REGEX)) {
                    for (IdentityContext sink : sinks) {
                        if (sink.isEquivalence()) {
                            ranges.addAll(buildRange(source, sink));
                        }

                    }
                } else {
                    ranges.add(buildRange(source));
                }
            } else {
                throw new IllegalStateException("Unknown state trying to build ranges with sources that are not using equivalence operations");
            }

        }

        return ranges;
    }

    /**
     * Builds an Accumulo edge table range for a given SOURCE. Gets the leading string literal and makes a range between the leading string literal and the Max
     * Unicode String Constant.
     * <p>
     * Note: if not including stats edges then we can speed things up by setting the start key equal to the SOURCE + the null char, ONLY IF IT IS NOT A REGEX
     * EXPRESSION Also Note: with regex expressions we need to have the end of the end key set to the max unicode value but if it is just an equals expression
     * then the end key should end in unicode 1 that way we don't pick up extra sources
     * <p>
     * Use this method when there is either no sink or the source is a regex expression
     *
     * @param source a source
     * @return a range
     */
    private Range buildRange(IdentityContext source) {
        String rowID = getLeadingLiteral(source, false);

        boolean isRegexRange = source.getOperation().equals(EQUALS_REGEX);
        return EdgeKeyUtil.createEscapedRange(rowID, isRegexRange, includeStats, true);

    }

    /**
     * Builds an Accumulo edge table range for a given SOURCE and SINK. The SOURCE is assumed to not be a regex expression.
     * <p>
     * If including stats edges then only build range for the SOURCE and the SOURCE+SINK, no stats edges for SINK will be returned.
     *
     * @param source a source
     * @param sink   a sink
     * @return a set of ranges
     */
    private Set<Range> buildRange(IdentityContext source, IdentityContext sink) {
        String rowSource = getLeadingLiteral(source, false);
        String rowSink = getLeadingLiteral(sink, true);
        Set<Range> rangeSet = new HashSet<>(2);

        if (includeStats) {
            rangeSet.add(EdgeKeyUtil.createEscapedRange(rowSource, false, includeStats, false));
        }

        boolean isSinkRegex = sink.getOperation().equals(EQUALS_REGEX);
        rangeSet.add(EdgeKeyUtil.createEscapedRange(rowSource, rowSink, isSinkRegex));

        return rangeSet;
    }

    /**
     * Gets the leading literal from the search term stored in an IdentityContext. The leading literal is set of characters that are not a regex expression (.*
     * /d /w /s ...)
     * <p>
     * If the whole search term is a literal (eg like with an == expression) then the whole string is returned
     * <p>
     * If there is no leading literal then the empty String is returned
     * <p>
     * The leading wildCardAllowed parameter says if leading wild cards are allowed in the search term (leading wild cards are not allowed with SOURCE
     * expressions but allowed with every thing else)
     *
     * @param leadingWildCardAllowed leadingWildCardAllowed
     * @param term                   a term
     * @return the leading literal
     */
    private String getLeadingLiteral(IdentityContext term, boolean leadingWildCardAllowed) {
        String leadingLiteral = "";
        if (term.getOperation().equals(EQUALS_REGEX)) {
            try {
                JavaRegexAnalyzer regexAnalyzer = new JavaRegexAnalyzer(term.getLiteral());

                if (leadingWildCardAllowed == false && regexAnalyzer.isLeadingRegex()) {
                    log.error("Identifier had leading wildcard expression");
                    throw new IllegalArgumentException("Can't have leading wildcards on SOURCE");
                }
                leadingLiteral = regexAnalyzer.getLeadingLiteral();
            } catch (JavaRegexAnalyzer.JavaRegexParseException e) {
                log.error("Error parsing regex expression: " + term.getLiteral());
                throw new IllegalArgumentException("Error parsing regex expression: " + term.getLiteral());
            }
        } else {
            leadingLiteral = term.getLiteral();
        }

        // Since the result of this method will be appended to the start/end keys returning null is a problem since the
        // string class will literally append the word 'null' instead of nothing
        if (leadingLiteral == null) {
            leadingLiteral = "";
        }

        return leadingLiteral;
    }

    /*
     * Calls build ranges and adds the results of the call to the list of total ranges
     *
     * This method also looks for the location of the sink in the query context which can appear in either the row context or the row contexts of its
     * 'otherContexts' list (but not both).
     *
     * If the sinks do appear in the otherContexts list then the sinks need to be included in the query string sent to the edge filter iterator. If the sinks
     * appear in some but not all of the other query contexts then they cannot be used in building the ranges
     *
     * ex/ (Source=a1) && ((Sink=k1 && ET=e1) || (Sink=k2 && ET=e2)) -ranges built with sink and sink included in query string (Source=a1) && ((Sink=k1 &&
     * ET=e1) || (ET=e2)) -ranges built without sink and sink included in query string (Source=a1 && Sink=k1) && (ET=e1 || ET=e2) -ranges built with sink and
     * sink not included in query string (Source=a1 && Sink=k1) && ((Sink=k2 && ET=e1) || (Sink=k3 && ET=e2)) -error should not occur/not allowed -Rather by the
     * time it gets here the above query should look like: (Source=a1) && ((Sink=k1) || (Sink=k2 && ET=e1) || (Sink=k3 && ET=e2))
     */
    public boolean updateQueryRanges(QueryContext qContext) {
        boolean needToIncludeSinksInQuery = false;

        // get the sinks from the column context
        List<IdentityContext> sinks = qContext.getRowContext().getSinks();

        List<IdentityContext> otherSinks = null;
        boolean otherTSComplete = true;

        // get the sinks from the otherContexts
        if (qContext.getOtherContexts() != null) {
            for (QueryContext oContext : qContext.getOtherContexts()) {
                // if an other context was found without a sink list then can't use sinks when building ranges
                if (oContext.getRowContext() != null && oContext.getRowContext().getSinks() != null) {
                    if (otherSinks == null) {
                        otherSinks = new ArrayList<>();
                    }
                    otherSinks.addAll(oContext.getRowContext().getSinks());
                } else {
                    otherTSComplete = false;
                }
            }
        }
        // now evaluate what to do after finding all sinks
        if (sinks != null && otherSinks != null) {
            throw new IllegalStateException("Can't build ranges for the provide sinks");
        } else if (otherSinks != null) {
            // sinks only in the other contexts
            needToIncludeSinksInQuery = true;
            if (otherTSComplete) {
                // build ranges with sinks
                this.getRanges().addAll(buildRanges(qContext.getRowContext().getSources(), otherSinks));
            } else {
                // build ranges without sinks
                this.getRanges().addAll(buildRanges(qContext.getRowContext().getSources(), null));
            }

        } else if (sinks != null) {
            // sinks only in the row context
            this.getRanges().addAll(buildRanges(qContext.getRowContext().getSources(), sinks));
        } else {
            // no sinks found
            this.getRanges().addAll(buildRanges(qContext.getRowContext().getSources(), null));
        }

        return needToIncludeSinksInQuery;

    }

    public StringBuilder getNormalizedQuery() {
        return normalizedQuery;
    }

    public StringBuilder getNormalizedStatsQuery() {
        return normalizedStatsQuery;
    }

    public Set<Range> getRanges() {
        return ranges;
    }

    public HashMultimap<String, String> getPreFilterValues() {
        return preFilterValues;
    }

    public long getTermCount() {
        return termCount;
    }

    public void setTermCount(long termCount) {
        this.termCount = termCount;
    }

    public boolean isHasAllCompleteColumnFamilies() {
        return hasAllCompleteColumnFamilies;
    }

    public void setHasAllCompleteColumnFamilies(boolean hasAllCompleteColumnFamilies) {
        this.hasAllCompleteColumnFamilies = hasAllCompleteColumnFamilies;
    }

    public List<Text> getColumnFamilies() {
        return columnFamilies;
    }
}
