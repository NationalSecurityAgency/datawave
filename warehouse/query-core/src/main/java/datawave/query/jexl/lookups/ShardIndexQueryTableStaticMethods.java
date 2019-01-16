package datawave.query.jexl.lookups;

import com.google.common.collect.Sets;
import datawave.core.iterators.ColumnQualifierRangeIterator;
import datawave.core.iterators.GlobalIndexTermMatchingIterator;
import datawave.core.iterators.filter.GlobalIndexDataTypeFilter;
import datawave.core.iterators.filter.GlobalIndexDateRangeFilter;
import datawave.core.iterators.filter.GlobalIndexTermMatchingFilter;
import datawave.data.type.Type;
import datawave.query.Constants;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DoNotPerformOptimizedQueryException;
import datawave.query.exceptions.IllegalRangeArgumentException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.LiteralRange;
import datawave.query.parser.JavaRegexAnalyzer;
import datawave.query.tables.AnyFieldScanner;
import datawave.query.tables.ScannerFactory;
import datawave.query.tables.ScannerSession;
import datawave.query.tables.SessionOptions;
import datawave.query.util.MetadataHelper;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.PreConditionFailedQueryException;
import datawave.webservice.query.exception.QueryException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.LongRange;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Temporary location for static methods in ShardIndexQueryTable
 *
 */
public class ShardIndexQueryTableStaticMethods {
    
    private static final Logger log = Logger.getLogger(ShardIndexQueryTableStaticMethods.class);
    
    private static FastDateFormat formatter = FastDateFormat.getInstance("yyyyMMdd");
    
    /**
     * Create an IndexLookup task to find field names give a JexlNode and a set of Types for that node
     *
     * @param node
     * @param dataTypes
     * @return
     */
    public static IndexLookup normalizeQueryTerm(JexlNode node, Set<String> expansionFields, Set<Type<?>> dataTypes, MetadataHelper helperRef) {
        if (node instanceof ASTEQNode) {
            return normalizeQueryTerm((ASTEQNode) node, expansionFields, dataTypes);
        } else if (node instanceof ASTNENode) {
            return normalizeQueryTerm((ASTNENode) node, expansionFields, dataTypes);
        } else if (node instanceof ASTERNode) {
            return expandRegexFieldName((ASTERNode) node, expansionFields, dataTypes, helperRef);
        } else if (node instanceof ASTNRNode) {
            return expandRegexFieldName((ASTNRNode) node, expansionFields, dataTypes, helperRef);
        } else if (node instanceof ASTLENode) {
            throw new UnsupportedOperationException("Cannot expand an unbounded range");
        } else if (node instanceof ASTLTNode) {
            throw new UnsupportedOperationException("Cannot expand an unbounded range");
        } else if (node instanceof ASTGENode) {
            throw new UnsupportedOperationException("Cannot expand an unbounded range");
        } else if (node instanceof ASTGTNode) {
            throw new UnsupportedOperationException("Cannot expand an unbounded range");
        } else {
            return new EmptyIndexLookup();
        }
    }
    
    public static IndexLookup normalizeQueryTerm(String literal, Set<String> expansionFields, Set<Type<?>> dataTypes) {
        Set<String> terms = Sets.newHashSet(literal);
        
        for (Type<?> normalizer : dataTypes) {
            try {
                String normalizedValue = normalizer.normalize(literal);
                if (!StringUtils.isBlank(normalizedValue)) {
                    terms.add(normalizedValue);
                }
            } catch (Exception e) {
                if (log.isTraceEnabled()) {
                    log.trace("Could not apply " + normalizer.getClass().getName() + " to " + literal);
                }
            }
        }
        
        return new FieldNameLookup(expansionFields, terms);
    }
    
    /**
     * Build up a task to run against the inverted index tables
     *
     * @param node
     * @param dataTypes
     * @return
     */
    public static IndexLookup normalizeQueryTerm(ASTEQNode node, Set<String> expansionFields, Set<Type<?>> dataTypes) {
        return _normalizeQueryTerm(node, expansionFields, dataTypes);
    }
    
    /**
     * Build up a task to run against the inverted index tables
     *
     * @param node
     * @param dataTypes
     * @return
     */
    public static IndexLookup normalizeQueryTerm(ASTNENode node, Set<String> expansionFields, Set<Type<?>> dataTypes) {
        return _normalizeQueryTerm(node, expansionFields, dataTypes);
    }
    
    protected static IndexLookup _normalizeQueryTerm(JexlNode node, Set<String> expansionFields, Set<Type<?>> dataTypes) {
        Object literal = JexlASTHelper.getLiteralValue(node);
        
        if (literal instanceof String) {
            return normalizeQueryTerm((String) literal, expansionFields, dataTypes);
        } else if (literal instanceof Number) {
            return normalizeQueryTerm(((Number) literal).toString(), expansionFields, dataTypes);
        } else {
            log.error("Encountered literal that was not a String nor a Number: " + literal.getClass().getName() + ", " + literal);
            throw new IllegalArgumentException("Encountered literal that was not a String nor a Number: " + literal.getClass().getName() + ", " + literal);
        }
    }
    
    /**
     * Build up a task to run against the inverted index tables
     *
     * @param node
     * @param dataTypes
     * @return
     */
    public static IndexLookup expandRegexFieldName(ASTERNode node, Set<String> expansionFields, Set<Type<?>> dataTypes, MetadataHelper helperRef) {
        return _expandRegexFieldName(node, expansionFields, dataTypes, helperRef);
    }
    
    /**
     * Build up a task to run against the inverted index tables
     *
     * @param node
     * @param dataTypes
     * @return
     */
    public static IndexLookup expandRegexFieldName(ASTNRNode node, Set<String> expansionFields, Set<Type<?>> dataTypes, MetadataHelper helperRef) {
        return _expandRegexFieldName(node, expansionFields, dataTypes, helperRef);
    }
    
    /**
     * A non-public method that implements the expandRegexFieldName to force clients to actually provide an ASTERNode or ASTNRNode
     *
     * @param node
     * @param dataTypes
     * @return
     */
    protected static IndexLookup _expandRegexFieldName(JexlNode node, Set<String> expansionFields, Set<Type<?>> dataTypes, MetadataHelper helperRef) {
        Set<String> patterns = Sets.newHashSet();
        
        Object literal = JexlASTHelper.getLiteralValue(node);
        
        if (literal instanceof String) {
            patterns.add((String) literal);
        } else if (literal instanceof Number) {
            patterns.add(literal.toString());
        }
        
        // TODO: Add proper support for regex against overloaded composite fields
        for (Type<?> normalizer : dataTypes) {
            if (literal instanceof String) {
                try {
                    patterns.add(normalizer.normalizeRegex((String) literal));
                } catch (Exception e) {
                    if (log.isTraceEnabled()) {
                        log.trace("Could not apply " + normalizer.getClass().getName() + " to " + literal);
                    }
                }
            } else if (literal instanceof Number) {
                try {
                    patterns.add(normalizer.normalizeRegex(literal.toString()));
                } catch (Exception e) {
                    if (log.isTraceEnabled()) {
                        log.trace("Could not apply " + normalizer.getClass().getName() + " to " + literal);
                    }
                }
            } else {
                log.warn("Encountered literal that was not a String nor a Number: " + literal.getClass().getName() + ", " + literal);
            }
        }
        
        return (IndexLookup) new LookupTermsFromRegex(expansionFields, patterns, helperRef);
    }
    
    public static IndexLookup expandRegexTerms(ASTERNode node, String fieldName, Set<Type<?>> dataTypes, MetadataHelper helperRef) {
        return expandRegexTerms(node, fieldName, dataTypes, null, helperRef);
    }
    
    /**
     * Build up a task to run against the inverted index tables
     *
     * @param node
     * @param fieldName
     * @param dataTypes
     * @param datatypeFilter
     * @return
     */
    public static IndexLookup expandRegexTerms(ASTERNode node, String fieldName, Collection<Type<?>> dataTypes, Set<String> datatypeFilter,
                    MetadataHelper helperRef) {
        Set<String> patterns = Sets.newHashSet();
        
        Object literal = JexlASTHelper.getLiteralValue(node);
        
        for (Type<?> type : dataTypes) {
            if (literal instanceof String) {
                try {
                    patterns.add(type.normalizeRegex((String) literal));
                } catch (Exception e) {
                    if (log.isTraceEnabled()) {
                        log.trace("Could not apply " + type.getClass().getName() + " to " + literal);
                    }
                }
            } else if (literal instanceof Number) {
                try {
                    patterns.add(type.normalizeRegex(literal.toString()));
                } catch (Exception e) {
                    if (log.isTraceEnabled()) {
                        log.trace("Could not apply " + type.getClass().getName() + " to " + literal);
                    }
                }
            } else {
                log.warn("Encountered literal that was not a String nor a Number: " + literal.getClass().getName() + ", " + literal);
            }
        }
        
        return (IndexLookup) new LookupTermsFromRegex(fieldName, patterns, datatypeFilter, helperRef);
    }
    
    public static IndexLookup expandRange(LiteralRange<?> range) {
        
        return new LookupBoundedRangeForTerms(range);
    }
    
    /**
     * Get a range description for a specified query term which is a literal.
     *
     * @param normalizedQueryTerm
     * @return
     */
    public static Range getLiteralRange(String normalizedQueryTerm) {
        return getLiteralRange(null, normalizedQueryTerm);
    }
    
    public static Range getLiteralRange(Map.Entry<String,String> entry) {
        return getLiteralRange(entry.getKey(), entry.getValue());
    }
    
    public static Range getLiteralRange(String fieldName, String normalizedQueryTerm) {
        if (null == fieldName) {
            return new Range(new Text(normalizedQueryTerm));
        }
        
        Key startKey = new Key(normalizedQueryTerm, fieldName, "");
        
        return new Range(startKey, false, startKey.followingKey(PartialKey.ROW_COLFAM), false);
    }
    
    /**
     * We only need to concern ourselves with looking for field names.
     * 
     * @param config
     * @param scannerFactory
     * @param tableName
     * @param ranges
     * @param literals
     * @param patterns
     * @param reverseIndex
     * @param limitToUniqueTerms
     * @return
     * @throws Exception
     */
    public static ScannerSession configureTermMatchOnly(ShardQueryConfiguration config, ScannerFactory scannerFactory, String tableName,
                    Collection<Range> ranges, Collection<String> literals, Collection<String> patterns, boolean reverseIndex, boolean limitToUniqueTerms)
                    throws Exception {
        
        // if we have no ranges, then nothing to scan
        if (ranges.isEmpty()) {
            return null;
        }
        
        ScannerSession bs = scannerFactory.newLimitedScanner(AnyFieldScanner.class, tableName, config.getAuthorizations(), config.getQuery());
        
        bs.setRanges(ranges);
        
        SessionOptions options = new SessionOptions();
        
        IteratorSetting setting = configureDateRangeIterator(config);
        options.addScanIterator(setting);
        
        setting = configureGlobalIndexTermMatchingIterator(config, literals, patterns, reverseIndex, limitToUniqueTerms);
        if (setting != null) {
            options.addScanIterator(setting);
        }
        
        bs.setOptions(options);
        
        return bs;
    }
    
    public static ScannerSession configureLimitedDiscovery(ShardQueryConfiguration config, ScannerFactory scannerFactory, String tableName,
                    Collection<Range> ranges, Collection<String> literals, Collection<String> patterns, boolean reverseIndex, boolean limitToUniqueTerms)
                    throws Exception {
        
        // if we have no ranges, then nothing to scan
        if (ranges.isEmpty()) {
            return null;
        }
        
        ScannerSession bs = scannerFactory.newLimitedScanner(AnyFieldScanner.class, tableName, config.getAuthorizations(), config.getQuery());
        
        bs.setRanges(ranges);
        
        // The begin date from the query may be down to the second, for doing lookups in the index we want to use the day because
        // the times in the index table have been truncated to the day.
        Date begin = DateUtils.truncate(config.getBeginDate(), Calendar.DAY_OF_MONTH);
        // we don't need to bump up the end date any more because it's not apart of the range set on the scanner
        Date end = config.getEndDate();
        
        LongRange dateRange = new LongRange(begin.getTime(), end.getTime());
        SessionOptions options = new SessionOptions();
        // options.addScanIterator(configureGlobalIndexDateRangeFilter(config, dateRange));
        options.addScanIterator(configureDateRangeIterator(config));
        IteratorSetting setting = configureGlobalIndexDataTypeFilter(config, config.getDatatypeFilter());
        if (setting != null) {
            options.addScanIterator(setting);
        }
        setting = configureGlobalIndexTermMatchingIterator(config, literals, patterns, reverseIndex, limitToUniqueTerms);
        if (setting != null) {
            options.addScanIterator(setting);
        }
        
        bs.setOptions(options);
        
        return bs;
    }
    
    public static final void configureGlobalIndexDateRangeFilter(ShardQueryConfiguration config, ScannerBase bs, LongRange dateRange) {
        // Setup the GlobalIndexDateRangeFilter
        
        if (log.isTraceEnabled()) {
            log.trace("Configuring GlobalIndexDateRangeFilter with " + dateRange);
        }
        IteratorSetting cfg = configureGlobalIndexDateRangeFilter(config, dateRange);
        bs.addScanIterator(cfg);
    }
    
    public static final IteratorSetting configureGlobalIndexDateRangeFilter(ShardQueryConfiguration config, LongRange dateRange) {
        // Setup the GlobalIndexDateRangeFilter
        if (log.isTraceEnabled()) {
            log.trace("Configuring GlobalIndexDateRangeFilter with " + dateRange);
        }
        IteratorSetting cfg = new IteratorSetting(config.getBaseIteratorPriority() + 21, "dateFilter", GlobalIndexDateRangeFilter.class);
        cfg.addOption(Constants.START_DATE, Long.toString(dateRange.getMinimumLong()));
        cfg.addOption(Constants.END_DATE, Long.toString(dateRange.getMaximumLong()));
        return cfg;
    }
    
    public static final IteratorSetting configureDateRangeIterator(ShardQueryConfiguration config) throws IOException {
        // Setup the GlobalIndexDateRangeFilter
        if (log.isTraceEnabled()) {
            log.trace("Configuring configureDateRangeIterator ");
        }
        IteratorSetting cfg = new IteratorSetting(config.getBaseIteratorPriority() + 21, "dateFilter", ColumnQualifierRangeIterator.class);
        String begin = formatter.format(config.getBeginDate());
        String end = formatter.format(config.getEndDate()) + Constants.MAX_UNICODE_STRING;
        cfg.addOption(ColumnQualifierRangeIterator.RANGE_NAME, ColumnQualifierRangeIterator.encodeRange(new Range(begin, end)));
        return cfg;
    }
    
    public static final void configureGlobalIndexDataTypeFilter(ShardQueryConfiguration config, ScannerBase bs, Collection<String> dataTypes) {
        if (dataTypes == null || dataTypes.isEmpty()) {
            return;
        }
        if (log.isTraceEnabled()) {
            log.trace("Configuring GlobalIndexDataTypeFilter with " + dataTypes);
        }
        
        IteratorSetting cfg = configureGlobalIndexDataTypeFilter(config, dataTypes);
        if (cfg == null) {
            return;
        }
        
        bs.addScanIterator(cfg);
    }
    
    public static IteratorSetting configureGlobalIndexDataTypeFilter(ShardQueryConfiguration config, Collection<String> dataTypes) {
        
        if (log.isTraceEnabled()) {
            log.trace("Configuring GlobalIndexDataTypeFilter with " + dataTypes);
        }
        
        IteratorSetting cfg = new IteratorSetting(config.getBaseIteratorPriority() + 22, "dataTypeFilter", GlobalIndexDataTypeFilter.class);
        int i = 1;
        for (String dataType : dataTypes) {
            cfg.addOption(GlobalIndexDataTypeFilter.DATA_TYPE + i, dataType);
            i++;
        }
        return cfg;
    }
    
    public static final void configureGlobalIndexTermMatchingIterator(ShardQueryConfiguration config, ScannerBase bs, Collection<String> literals,
                    Collection<String> patterns, boolean reverseIndex, boolean limitToUniqueTerms) {
        if ((literals == null || literals.isEmpty()) && (patterns == null || patterns.isEmpty())) {
            return;
        }
        if (log.isTraceEnabled()) {
            log.trace("Configuring GlobalIndexTermMatchingIterator with " + literals + " and " + patterns);
        }
        
        IteratorSetting cfg = configureGlobalIndexTermMatchingIterator(config, literals, patterns, reverseIndex, limitToUniqueTerms);
        
        bs.addScanIterator(cfg);
        
    }
    
    public static final IteratorSetting configureGlobalIndexTermMatchingIterator(ShardQueryConfiguration config, Collection<String> literals,
                    Collection<String> patterns, boolean reverseIndex, boolean limitToUniqueTerms) {
        if ((literals == null || literals.isEmpty()) && (patterns == null || patterns.isEmpty())) {
            return null;
        }
        if (log.isTraceEnabled()) {
            log.trace("Configuring GlobalIndexTermMatchingIterator with " + literals + " and " + patterns);
        }
        
        IteratorSetting cfg = new IteratorSetting(config.getBaseIteratorPriority() + 24, "termMatcher", GlobalIndexTermMatchingIterator.class);
        int i = 1;
        if (patterns != null) {
            for (String pattern : patterns) {
                cfg.addOption(GlobalIndexTermMatchingFilter.PATTERN + i, pattern);
                i++;
            }
        }
        i = 1;
        if (literals != null) {
            for (String literal : literals) {
                cfg.addOption(GlobalIndexTermMatchingFilter.LITERAL + i, literal);
                i++;
            }
        }
        
        cfg.addOption(GlobalIndexTermMatchingFilter.REVERSE_INDEX, Boolean.toString(reverseIndex));
        if (limitToUniqueTerms) {
            cfg.addOption(GlobalIndexTermMatchingIterator.UNIQE_TERMS_IN_FIELD, Boolean.toString(limitToUniqueTerms));
        }
        
        return cfg;
    }
    
    /**
     * Get a range description for a specified query term, which could be a regex. Note that it is assumed that the column family set will include the
     * fieldname.
     *
     * @param fieldName
     * @param normalizedQueryTerm
     * @param fullTableScanEnabled
     * @param config
     * @return
     * @throws datawave.query.parser.JavaRegexAnalyzer.JavaRegexParseException
     * @throws org.apache.accumulo.core.client.TableNotFoundException
     * @throws java.util.concurrent.ExecutionException
     */
    public static RefactoredRangeDescription getRegexRange(String fieldName, String normalizedQueryTerm, boolean fullTableScanEnabled,
                    MetadataHelper metadataHelper, ShardQueryConfiguration config) throws JavaRegexAnalyzer.JavaRegexParseException, TableNotFoundException,
                    ExecutionException {
        if (log.isDebugEnabled()) {
            log.debug("getRegexRange: " + normalizedQueryTerm);
        }
        
        RefactoredRangeDescription rangeDesc = new RefactoredRangeDescription();
        
        JavaRegexAnalyzer regex = new JavaRegexAnalyzer(normalizedQueryTerm);
        
        if (regex.isNgram()) {
            // If we have wildcards on both sides, then do a full table scan if allowed
            
            // if we require a full table scan but it is disabled, then bail
            if (!fullTableScanEnabled) {
                PreConditionFailedQueryException qe = new PreConditionFailedQueryException(DatawaveErrorCode.WILDCARDS_BOTH_SIDES, MessageFormat.format(
                                "Term: {0}", normalizedQueryTerm));
                log.error(qe);
                throw new DoNotPerformOptimizedQueryException(qe);
            }
            
            rangeDesc.range = new Range();
            
        } else if (shouldUseReverseIndex(regex, fieldName, metadataHelper, config)) {
            // If we have a leading wildcard, reverse the term and use the global reverse index.
            
            StringBuilder buf = new StringBuilder(regex.getTrailingLiteral());
            String reversedQueryTerm = buf.reverse().toString();
            
            if (log.isTraceEnabled()) {
                StringBuilder sb = new StringBuilder(256);
                
                sb.append("For '").append(normalizedQueryTerm).append("'");
                
                if (null != fieldName) {
                    sb.append(" in the field ").append(fieldName);
                }
                
                sb.append(", using the reverse index with the literal: '").append(reversedQueryTerm).append("'");
                
                log.trace(sb.toString());
            }
            
            Key startKey = new Key(reversedQueryTerm);
            Key endKey = new Key(reversedQueryTerm + Constants.MAX_UNICODE_STRING);
            
            // set the upper and lower bounds
            rangeDesc.range = new Range(startKey, false, endKey, false);
            rangeDesc.isForReverseIndex = true;
            
        } else {
            String queryTerm = regex.getLeadingLiteral();
            
            if (log.isTraceEnabled()) {
                StringBuilder sb = new StringBuilder(256);
                
                sb.append("For '").append(normalizedQueryTerm).append("'");
                
                if (null != fieldName) {
                    sb.append(" in the field ").append(fieldName);
                }
                
                sb.append(", using the forward index with the literal: '").append(queryTerm).append("'");
                
                log.trace(sb.toString());
            }
            
            Key startKey = new Key(queryTerm);
            Key endKey = new Key(queryTerm + Constants.MAX_UNICODE_STRING);
            
            // either middle or trailing wildcard, truncate the field value at the wildcard location
            // for upper bound, tack on the upper bound UTF character
            rangeDesc.range = new Range(startKey, false, endKey, false);
        }
        
        return rangeDesc;
        
    }
    
    public static class RefactoredRangeDescription {
        
        public Range range;
        public boolean isForReverseIndex = false;
        
        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder(range.toString());
            return builder.append(" is Reverse Indexed ").append(isForReverseIndex).toString();
        }
    }
    
    public static RefactoredRangeDescription getRegexRange(Map.Entry<String,String> entry, boolean fullTableScanEnabled, MetadataHelper metadataHelper,
                    ShardQueryConfiguration config) throws JavaRegexAnalyzer.JavaRegexParseException, TableNotFoundException, ExecutionException {
        return getRegexRange(entry.getKey(), entry.getValue(), fullTableScanEnabled, metadataHelper, config);
    }
    
    public static boolean shouldUseReverseIndex(JavaRegexAnalyzer analyzer, String fieldName, MetadataHelper metadataHelper, ShardQueryConfiguration config)
                    throws TableNotFoundException, ExecutionException {
        
        String leadingLiteral = analyzer.getLeadingLiteral(), trailingLiteral = analyzer.getTrailingLiteral();
        
        Set<String> datatypeFilter = config.getDatatypeFilter();
        
        // Normal case, we have a prefix, use the forward index
        if (null == trailingLiteral && null != leadingLiteral) {
            return false;
        } else if (null == leadingLiteral && null != trailingLiteral) {
            // If we have no prefix, we have no option but to use the reverse index (regardless of indexed or not)
            return true;
        } else {
            // TODO Magical handling of a "null" fieldName
            boolean isReverseIndexed = (null != fieldName) ? reverseIndexedInDatatype(fieldName, datatypeFilter, metadataHelper) : true;
            
            // We have no leading literal, only the trailing, we have to use the reverse index
            if (null != trailingLiteral && null == leadingLiteral && isReverseIndexed) {
                return true;
            }
            
            // TODO Magical handling of a "null" fieldName
            boolean isForwardIndexed = (null != fieldName) ? indexedInDatatype(fieldName, datatypeFilter, metadataHelper) : true;
            
            // If we're only reverse indexed, gotta use the reverse
            if (!isForwardIndexed && isReverseIndexed) {
                return true;
            }
            
            // In the case where we have the option
            if (leadingLiteral != null && isForwardIndexed && isReverseIndexed) {
                String p = trimRealmFromLiteral(trailingLiteral, config);
                // Use the reverse if we have a longer 'piece' to search over
                if (leadingLiteral.length() < p.length()) {
                    return true;
                }
            }
        }
        
        // Use the forward in the end
        return false;
    }
    
    /**
     * Get the accumulo range for a literal query range. Note that it is assumed that the column family set will include the fieldname.
     * 
     * @param fieldName
     * @param literalRange
     * @return
     * @throws IllegalRangeArgumentException
     */
    public static Range getBoundedRangeRange(String fieldName, LiteralRange<?> literalRange) throws IllegalRangeArgumentException {
        String lower = literalRange.getLower().toString(), upper = literalRange.getUpper().toString();
        
        Key startKey = new Key(new Text(lower));
        Key endKey = new Key(new Text(literalRange.isUpperInclusive() ? upper + Constants.MAX_UNICODE_STRING : upper));
        
        Range range = null;
        try {
            range = new Range(startKey, literalRange.isLowerInclusive(), endKey, literalRange.isUpperInclusive());
        } catch (IllegalArgumentException e) {
            QueryException qe = new QueryException(DatawaveErrorCode.RANGE_CREATE_ERROR, e, MessageFormat.format("{0}", literalRange));
            log.debug(qe);
            throw new IllegalRangeArgumentException(qe);
        }
        return range;
    }
    
    public static boolean indexedInDatatype(String fieldName, Set<String> datatypeFilter, MetadataHelper helper) throws TableNotFoundException {
        return helper.isIndexed(fieldName, datatypeFilter);
    }
    
    public static boolean reverseIndexedInDatatype(String fieldName, Set<String> datatypeFilter, MetadataHelper helper) throws TableNotFoundException {
        return helper.isReverseIndexed(fieldName, datatypeFilter);
    }
    
    /**
     * Method to remove realm from the trialing literal to discourage usage of the reverse index when the trailing literal is only a realm.
     *
     * @param literal
     *            The literal to trim.
     * @return A literal with the realm information removed.
     */
    private static String trimRealmFromLiteral(String literal, ShardQueryConfiguration config) {
        String retVal = null;
        
        List<String> exclusions = config.getRealmSuffixExclusionPatterns();
        
        if (null != exclusions) {
            for (String exclusion : exclusions) {
                java.util.regex.Pattern exclPattern = java.util.regex.Pattern.compile("(.*)" + exclusion);
                java.util.regex.Matcher m = exclPattern.matcher(literal);
                if (m.matches()) {
                    retVal = m.group(1);
                    break; // do we want to continue? should only ever be one realm...
                }
            }
            if (null == retVal) {
                retVal = literal;
            }
        } else {
            log.warn("Realm exclusion patterns were null.");
            retVal = literal;
        }
        
        return retVal;
    }
}
