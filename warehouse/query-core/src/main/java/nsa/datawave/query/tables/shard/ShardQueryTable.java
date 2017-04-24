package nsa.datawave.query.tables.shard;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;

import nsa.datawave.data.type.Type;
import nsa.datawave.query.config.GenericShardQueryConfiguration;
import nsa.datawave.query.config.ShardQueryConfiguration;
import nsa.datawave.query.parser.DatawaveQueryAnalyzer;
import nsa.datawave.query.parser.DatawaveTreeNode;
import nsa.datawave.query.parser.JavaRegexAnalyzer.JavaRegexParseException;
import nsa.datawave.query.parser.RangeCalculator;
import nsa.datawave.query.parser.RangeCalculator.RangeExpansionException;
import nsa.datawave.query.rewrite.exceptions.TooManyTermsException;
import nsa.datawave.query.tables.ScannerFactory;
import nsa.datawave.query.tables.ShardQueryLogic;
import nsa.datawave.query.tables.shard.ShardIndexQueryTable.RangeDescription;
import nsa.datawave.query.transformer.EventQueryDataDecoratorTransformer;
import nsa.datawave.query.transformer.EventQueryTransformer;
import nsa.datawave.webservice.query.Query;
import nsa.datawave.webservice.query.configuration.GenericQueryConfiguration;
import nsa.datawave.webservice.query.configuration.QueryData;
import nsa.datawave.webservice.query.logic.QueryLogicTransformer;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.time.DateUtils;
import org.apache.log4j.Logger;

import com.google.common.collect.Multimap;

/**
 * <h1>Overview</h1> QueryTable implementation that works with the JEXL grammar. This QueryTable uses the DATAWAVE metadata, global index, and sharded event
 * table to return results based on the query. The runServerQuery method is the main method that is called from the web service, and it contains the logic used
 * to run the queries against ACCUMULO. Example queries:
 *
 * <pre>
 *  <b>Single Term Query</b>
 *  'foo' - looks in global index for foo, and if any entries are found, then the query
 *          is rewritten to be field1 == 'foo' or field2 == 'foo', etc. This is then passed
 *          down the optimized query path which uses the intersecting iterators on the shard
 *          table.
 * 
 *  <b>Boolean expression</b>
 *  field == 'foo' - For fielded queries, those that contain a field, an operator, and a literal (string or number),
 *                   the query is parsed and the set of eventFields in the query that are indexed is determined by
 *                   querying the metadata table. Depending on the conjunctions in the query (or, and, not) and the
 *                   eventFields that are indexed, the query may be sent down the optimized path or the full scan path.
 * </pre>
 *
 * We are not supporting all of the operators that JEXL supports at this time. We are supporting the following operators:
 * 
 * <pre>
 *  ==, !=, &gt;, &ge;, &lt;, &le;, =~, and !~
 * </pre>
 *
 * Custom functions can be created and registered with the Jexl engine. The functions can be used in the queries in conjunction with other supported operators.
 * A sample function has been created, called between, and is bound to the 'f' namespace. An example using this function is : "f:between(LATITUDE,60.0, 70.0)"
 * 
 * <h1>Constraints on Query Structure</h1> Queries that are sent to this class need to be formatted such that there is a space on either side of the operator.
 * We are rewriting the query in some cases and the current implementation is expecting a space on either side of the operator. Users should also be aware that
 * the literals used in the query need to match the data in the table. For example, '123', 123, and 123.0 are treated differently. JEXL looks at the literal
 * used in the query and will try to coerce the field value to the appropriate type. If an error occurs in the evaluation we are skipping the event.
 * 
 * <h1>Notes on Optimization</h1> Queries that meet any of the following criteria will perform a full scan of the events in the sharded event table:
 * <ol>
 * <li>An 'or' conjunction exists in the query but not all of the terms are indexed.</li>
 * <li>No indexed terms exist in the query</li>
 * <li>An unsupported operator exists in the query</li>
 * </ol>
 *
 *
 * @deprecated - see nsa.datawave.query.rewrite.tables.RefactoredShardQueryLogic
 */
@Deprecated
public class ShardQueryTable extends ShardQueryLogic {
    
    protected static final Logger log = Logger.getLogger(ShardQueryTable.class);
    
    protected SimpleDateFormat shardDateFormatter = new SimpleDateFormat("yyyyMMdd");
    
    public static Class<? extends GenericShardQueryConfiguration> tableConfigurationType = ShardQueryConfiguration.class;
    
    protected List<String> taskingDatatypes = Arrays.asList("octave", "utt");
    protected EventQueryDataDecoratorTransformer eventQueryDataDecoratorTransformer = null;
    protected List<String> contentFieldNames = Collections.emptyList();
    
    public ShardQueryTable() {
        super();
        this.shardDateFormatter.setTimeZone(TimeZone.getTimeZone("GMT"));
    }
    
    public ShardQueryTable(ShardQueryTable other) {
        super(other);
        this.shardDateFormatter.setTimeZone(TimeZone.getTimeZone("GMT"));
        // shallow copy the utt list, we should be able to get away with a shallow copy because it is comprised of strings.
        List<String> copyTaskingDatatypes = new ArrayList<>(other.taskingDatatypes);
        this.setTaskingDatatypes(copyTaskingDatatypes);
        
        if (other.dataTypes != null) {
            List<Type<?>> copyDataTypes = new ArrayList<>(other.dataTypes);
            this.setDataTypes(copyDataTypes);
        }
        
        if (other.eventQueryDataDecoratorTransformer != null) {
            this.eventQueryDataDecoratorTransformer = new EventQueryDataDecoratorTransformer(other.eventQueryDataDecoratorTransformer);
        }
        this.contentFieldNames = other.contentFieldNames;
    }
    
    public List<String> getContentFieldNames() {
        return contentFieldNames;
    }
    
    public void setContentFieldNames(List<String> contentFieldNames) {
        this.contentFieldNames = contentFieldNames;
    }
    
    @Override
    protected RangeCalculator getTermIndexInformation(GenericShardQueryConfiguration config, Set<String> indexedFieldNames, DatawaveTreeNode root)
                    throws TableNotFoundException, org.apache.commons.jexl2.parser.ParseException, RangeExpansionException, TooManyTermsException,
                    JavaRegexParseException {
        RangeCalculator calc = new RangeCalculator();
        calc.execute(config, indexedFieldNames, root, this.scannerFactory);
        return calc;
    }
    
    public List<String> getTaskingDatatypes() {
        return taskingDatatypes;
    }
    
    public void setTaskingDatatypes(List<String> taskingDatatypes) {
        this.taskingDatatypes = taskingDatatypes;
    }
    
    @Override
    protected Collection<Range> getFullScanRange(GenericShardQueryConfiguration config, Multimap<String,DatawaveTreeNode> terms) {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        cal.setTime(config.getBeginDate());
        
        // Truncate the time on the startKey
        String startKey = config.getShardDateFormatter().format(DateUtils.truncate(cal, Calendar.DAY_OF_MONTH).getTime());
        
        // Truncate and bump the time on the endKey
        String endKey = config.getShardDateFormatter().format(RangeCalculator.getEndDateForIndexLookup(config.getEndDate()));
        
        Range r = new Range(startKey, true, endKey, false);
        
        return Collections.singletonList(r);
    }
    
    @Override
    public SimpleDateFormat getDateFormatter() {
        return shardDateFormatter;
    }
    
    @Override
    protected Set<String> getUnfieldedTermIndexInformation(GenericShardQueryConfiguration config, DatawaveTreeNode node) throws TableNotFoundException,
                    IOException, InstantiationException, IllegalAccessException, JavaRegexParseException {
        
        Set<String> mappings = new HashSet<>();
        
        // Calculate the set of ranges to use for this term.
        Set<String> literals = new HashSet<>();
        Set<String> patterns = new HashSet<>();
        Set<String> reversePatterns = new HashSet<>();
        Multimap<String,DatawaveTreeNode> fields = new DatawaveQueryAnalyzer().getFieldNameToNodeMap(node);
        ShardIndexQueryTable.normalizeQueryTerms(fields, config.getDataTypes(), literals, patterns);
        
        Set<Range> ranges = new HashSet<>();
        Set<Range> reverseRanges = new HashSet<>();
        
        for (String normalizedQueryTerm : literals) {
            ranges.add(ShardIndexQueryTable.getLiteralRange(normalizedQueryTerm));
        }
        for (String normalizedQueryTerm : patterns) {
            RangeDescription r = ShardIndexQueryTable.getRegexRange(normalizedQueryTerm, isFullTableScanEnabled());
            if (r.isForReverseIndex) {
                reverseRanges.add(r.range);
                reversePatterns.add(normalizedQueryTerm);
            } else {
                ranges.add(r.range);
            }
        }
        patterns.removeAll(reversePatterns);
        
        if (!literals.isEmpty() || !patterns.isEmpty()) {
            BatchScanner bs = ShardIndexQueryTable.configureBatchScannerAndReturnMatches(config, config.getIndexTableName(), ranges, literals, patterns, false,
                            super.scannerFactory);
            try {
                for (Entry<Key,Value> entry : bs) {
                    if (log.isTraceEnabled()) {
                        log.trace("Index entry: " + entry.getKey().toString());
                    }
                    // We are only returning a mapping of field value to field name, no need to
                    // determine cardinality and such at this point.
                    mappings.add(entry.getKey().getColumnFamily().toString());
                }
            } finally {
                bs.close();
            }
        }
        if (!reversePatterns.isEmpty()) {
            BatchScanner bs = ShardIndexQueryTable.configureBatchScannerAndReturnMatches(config, config.getReverseIndexTableName(), reverseRanges, null,
                            reversePatterns, true, super.scannerFactory);
            try {
                for (Entry<Key,Value> entry : bs) {
                    if (log.isTraceEnabled()) {
                        log.trace("Index entry: " + entry.getKey().toString());
                    }
                    // We are only returning a mapping of field value to field name, no need to
                    // determine cardinality and such at this point.
                    mappings.add(entry.getKey().getColumnFamily().toString());
                }
            } finally {
                bs.close();
            }
        }
        return mappings;
    }
    
    @Override
    public ShardQueryTable clone() {
        return new ShardQueryTable(this);
    }
    
    @Override
    public GenericQueryConfiguration initialize(Connector connection, Query settings, Set<Authorizations> auths) throws Exception {
        ShardQueryConfiguration config = new ShardQueryConfiguration(this, settings);
        
        scannerFactory = new ScannerFactory(connection);
        
        initialize(config, connection, settings, auths);
        
        return config;
    }
    
    @Override
    public void setupQuery(GenericQueryConfiguration genericConfig) throws Exception {
        if (!ShardQueryConfiguration.class.isAssignableFrom(genericConfig.getClass())) {
            throw new IllegalStateException("Did not receive a ShardQueryConfiguration instance!!");
        }
        
        ShardQueryConfiguration config = (ShardQueryConfiguration) genericConfig;
        
        // Ensure we have all of the information needed to run a query
        if (!config.canRunQuery()) {
            log.warn("The given query '" + config.getQueryString() + "' could not be run, most likely due to not matching any records in the global index.");
            
            // Stub out an iterator to correctly present "no results"
            this.iterator = new Iterator<Map.Entry<Key,Value>>() {
                @Override
                public boolean hasNext() {
                    return false;
                }
                
                @Override
                public Map.Entry<Key,Value> next() {
                    return null;
                }
                
                @Override
                public void remove() {}
            };
            
            this.scanner = null;
            
            return;
        }
        
        Iterator<QueryData> queries = config.getQueries();
        if (!queries.hasNext()) {
            throw new UnsupportedOperationException("Need to implement more than one query in a single pass");
        }
        
        QueryData qd = queries.next();
        
        try {
            BatchScanner bs = super.scannerFactory.newScanner(config.getShardTableName(), config.getAuthorizations(), config.getNumQueryThreads(),
                            config.getQuery());
            bs.setRanges(qd.getRanges());
            
            for (IteratorSetting itr : qd.getSettings()) {
                bs.addScanIterator(itr);
            }
            
            this.iterator = bs.iterator();
            this.scanner = bs;
        } catch (TableNotFoundException e) {
            log.error("The table '" + config.getShardTableName() + "' does not exist", e);
        }
    }
    
    @Override
    public QueryLogicTransformer getTransformer(Query settings) {
        EventQueryTransformer eventQueryTransformer = new EventQueryTransformer(this, settings, this.getMarkingFunctions(), this.getResponseObjectFactory());
        eventQueryTransformer.setEventQueryDataDecoratorTransformer(eventQueryDataDecoratorTransformer);
        eventQueryTransformer.setQm(this.queryModel);
        eventQueryTransformer.setContentFieldNames(getContentFieldNames());
        return eventQueryTransformer;
    }
    
    public EventQueryDataDecoratorTransformer getEventQueryDataDecoratorTransformer() {
        return eventQueryDataDecoratorTransformer;
    }
    
    public void setEventQueryDataDecoratorTransformer(EventQueryDataDecoratorTransformer eventQueryDataDecoratorTransformer) {
        this.eventQueryDataDecoratorTransformer = eventQueryDataDecoratorTransformer;
    }
    
    @Override
    public Set<String> getRequiredQueryParameters() {
        // TODO Auto-generated method stub
        return Collections.emptySet();
    }
    
    @Override
    public Set<String> getExampleQueries() {
        // TODO Auto-generated method stub
        return Collections.emptySet();
    }
    
}
