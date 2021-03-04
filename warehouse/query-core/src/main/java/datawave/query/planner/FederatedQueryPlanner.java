package datawave.query.planner;

import com.google.common.collect.Multimap;
import datawave.data.type.Type;
import datawave.query.CloseableIterable;
import datawave.query.config.FieldIndexHole;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.config.ValueIndexHole;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.exceptions.DatawaveQueryException;
import datawave.query.exceptions.NoResultsException;
import datawave.query.jexl.visitors.FetchDataTypesVisitor;
import datawave.query.planner.pushdown.rules.PushDownRule;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.IndexedDatesValue;
import datawave.query.util.MetadataHelper;
import datawave.query.util.YearMonthDay;
import datawave.util.time.DateHelper;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.configuration.QueryData;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.PreConditionFailedQueryException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.*;

public class FederatedQueryPlanner extends DefaultQueryPlanner {
    
    private static final Logger log = ThreadConfigurableLogger.getLogger(FederatedQueryPlanner.class);
    {
        log.setLevel(Level.DEBUG);
    }
    
    public FederatedQueryPlanner() {
        super();
    }
    
    public FederatedQueryDataIterable process2(GenericQueryConfiguration config, String query, Query settings, ScannerFactory scannerFactory)
                    throws DatawaveQueryException {
        
        FederatedQueryDataIterable returnQueryData = new FederatedQueryDataIterable();
        Date originalEndDate = config.getEndDate();
        Date originalStartDate = config.getBeginDate();
        TreeSet<YearMonthDay> holeDates;
        MetadataHelper metadataHelper = getMetadataHelper();

        final QueryData queryData = new QueryData();
        CloseableIterable<QueryData> results;

        if (config instanceof ShardQueryConfiguration) {
            ASTJexlScript queryTree = null;
            try {
                queryTree = updateQueryTree(scannerFactory, metadataHelper, dateIndexHelper, (ShardQueryConfiguration) config, query, queryData, settings);
            } catch (StackOverflowError e) {
                if (log.isTraceEnabled()) {
                    log.trace("Stack trace for overflow " + e);
                }
                PreConditionFailedQueryException qe = new PreConditionFailedQueryException(DatawaveErrorCode.QUERY_DEPTH_OR_TERM_THRESHOLD_EXCEEDED, e);
                log.warn(qe);
                throw new DatawaveFatalQueryException(qe);
            } catch (NoResultsException e) {
                if (log.isTraceEnabled()) {
                    log.trace("Definitively determined that no results exist from the indexes");
                }
                
            }
            
            Multimap<String,Type<?>> fieldToDatatypeMap = FetchDataTypesVisitor.fetchDataTypes(metadataHelper,
                            ((ShardQueryConfiguration) config).getDatatypeFilter(), queryTree, false);
            
            try {
                
                calculateFieldIndexHoles(metadataHelper, fieldToDatatypeMap, (ShardQueryConfiguration) config);
            } catch (TableNotFoundException e) {
                log.error("metadata table was not found " + e.getMessage());
            }
            
            List<FieldIndexHole> fieldIndexHoles = ((ShardQueryConfiguration) config).getFieldIndexHoles();
            List<ValueIndexHole> valueIndexHoles = ((ShardQueryConfiguration) config).getValueIndexHoles();
            if ((valueIndexHoles == null && fieldIndexHoles == null) || (valueIndexHoles.size() == 0 && fieldIndexHoles.size() == 0)) {
                returnQueryData.addDelegate(results);
                return returnQueryData;
            }
            
            holeDates = generateStartAndEndDates((ShardQueryConfiguration) config);
            Boolean firstIteration = true;
            Date startDate, endDate;
            
            for (Iterator<YearMonthDay> it = holeDates.iterator(); it.hasNext();) {
                if (firstIteration) {
                    firstIteration = false;
                    startDate = originalStartDate;
                    if (it.hasNext()) {
                        endDate = DateHelper.parse(it.next().getYyyymmdd());
                    } else
                        endDate = originalEndDate;
                } else {
                    startDate = DateHelper.parse(it.next().getYyyymmdd());
                    if (it.hasNext())
                        endDate = DateHelper.parse(it.next().getYyyymmdd());
                    else {
                        endDate = originalEndDate;
                    }
                }
                
                results = getQueryData((ShardQueryConfiguration) config, query, settings, scannerFactory, startDate, endDate);
                returnQueryData.addDelegate(results);
                
            }
            
        }else {
            results = super.process(config, query, settings, scannerFactory);
            returnQueryData.addDelegate(results);
        }
        
        
        return returnQueryData;
    }
    
    private CloseableIterable<QueryData> getQueryData(ShardQueryConfiguration config, String query, Query settings, ScannerFactory scannerFactory,
                    Date startDate, Date endDate) throws DatawaveQueryException {
        CloseableIterable<QueryData> queryData;
        ShardQueryConfiguration tempConfig = new ShardQueryConfiguration(config);
        tempConfig.setBeginDate(startDate);
        tempConfig.setEndDate(endDate);
        queryData = super.process(tempConfig, query, settings, scannerFactory);
        return queryData;
    }
    
    private TreeSet<YearMonthDay> generateStartAndEndDates(ShardQueryConfiguration configuration) {
        
        String startDate = DateHelper.format(configuration.getBeginDate().getTime());
        String endDate = DateHelper.format(configuration.getEndDate().getTime());
        
        YearMonthDay.Bounds bounds = new YearMonthDay.Bounds(startDate, true, endDate, true);
        
        TreeSet<YearMonthDay> queryDates = new TreeSet<>();
        for (ValueIndexHole valueIndexHole : configuration.getValueIndexHoles()) {
            addDatesToSet(bounds, queryDates, valueIndexHole.getStartDate());
            addDatesToSet(bounds, queryDates, valueIndexHole.getEndDate());
        }
        
        for (FieldIndexHole fieldIndexHole : configuration.getFieldIndexHoles()) {
            // TODO remove comparison below. calculateFieldHoles needs to be fixed.
            if (fieldIndexHole.getStartDate().compareTo(fieldIndexHole.getEndDate()) <= 0) {
                addDatesToSet(bounds, queryDates, fieldIndexHole.getStartDate());
                addDatesToSet(bounds, queryDates, fieldIndexHole.getEndDate());
            }
        }
        
        return queryDates;
    }
    
    private void addDatesToSet(YearMonthDay.Bounds bounds, TreeSet<YearMonthDay> queryDates, String strDate) {
        if (bounds.withinBounds(strDate))
            queryDates.add(new YearMonthDay(strDate));
    }
    
    @Override
    public FederatedQueryDataIterable process(GenericQueryConfiguration config, String query, Query settings, ScannerFactory scannerFactory)
                    throws DatawaveQueryException {
        
        return process2(config, query, settings, scannerFactory);
    }
    
    @Override
    public long maxRangesPerQueryPiece() {
        return super.maxRangesPerQueryPiece();
    }
    
    @Override
    public void close(GenericQueryConfiguration config, Query settings) {
        super.close(config, settings);
    }
    
    @Override
    public void setQueryIteratorClass(Class<? extends SortedKeyValueIterator<Key,Value>> clazz) {
        super.setQueryIteratorClass(clazz);
    }
    
    @Override
    public Class<? extends SortedKeyValueIterator<Key,Value>> getQueryIteratorClass() {
        return super.getQueryIteratorClass();
    }
    
    @Override
    public String getPlannedScript() {
        return super.getPlannedScript();
    }
    
    @Override
    public DefaultQueryPlanner clone() {
        return super.clone();
    }
    
    @Override
    public void setRules(Collection<PushDownRule> rules) {
        super.setRules(rules);
    }
    
    @Override
    public Collection<PushDownRule> getRules() {
        return super.getRules();
    }
    
    @Override
    public ASTJexlScript applyRules(ASTJexlScript queryTree, ScannerFactory scannerFactory, MetadataHelper metadataHelper, ShardQueryConfiguration config) {
        return super.applyRules(queryTree, scannerFactory, metadataHelper, config);
    }
    
    /**
     * Calculate the FieldIndexHoles and add them to the ShardedQueryConfiguaration
     *
     * @param metadataHelper
     * @param fieldToDatatypeMap
     * @param config
     */
    public void calculateFieldIndexHoles(MetadataHelper metadataHelper, Multimap<String,Type<?>> fieldToDatatypeMap, ShardQueryConfiguration config)
                    throws TableNotFoundException {
        
        IndexedDatesValue indexedDates;
        String startDate = DateHelper.format(config.getBeginDate().getTime());
        String endDate = DateHelper.format(config.getEndDate().getTime());
        String holeStart = startDate;
        String lastHoleEndate = null;
        YearMonthDay.Bounds bounds = new YearMonthDay.Bounds(startDate, true, endDate, true);
        FieldIndexHole newHole = null;
        boolean firstHole = true;
        boolean foundHolesInDateBounds = false;
        String previousDay, nextDay = null;
        log.debug("startDate is: " + startDate + " and endDate is " + endDate);
        
        for (String field : fieldToDatatypeMap.keySet()) {
            indexedDates = metadataHelper.getIndexDates(field, config.getDatatypeFilter());
            if (indexedDates != null && !(indexedDates.getIndexedDatesSet().size() == 0)) {
                for (YearMonthDay entry : indexedDates.getIndexedDatesSet()) {
                    // Only create a hole if the indexed field are within date bounds
                    if (bounds.withinBounds(entry)) {
                        foundHolesInDateBounds = true;
                        if (firstHole && holeStart.compareTo(entry.getYyyymmdd()) < 0) {
                            // create the FieldIndexHole for the dates the field was not indexed before the first
                            // time in the date range that it was indexed.
                            FieldIndexHole firstIndexHole = new FieldIndexHole(field, startDate);
                            previousDay = previousDay(entry.getYyyymmdd());
                            nextDay = nextDay(entry.getYyyymmdd());
                            log.debug("The date in the entry is: " + entry.getYyyymmdd());
                            log.debug("The previous day is: " + previousDay);
                            log.debug("The next day is: " + nextDay);
                            firstIndexHole.setEndDate(previousDay);
                            config.addFieldIndexHole(firstIndexHole);
                            holeStart = nextDay(entry.getYyyymmdd());
                            firstHole = false;
                        }
                        
                        // The end date of the last hole processed depends on the next date the field was indexed
                        if (newHole != null) {
                            lastHoleEndate = previousDay(entry.getYyyymmdd());
                            newHole.setEndDate(lastHoleEndate);
                        } else {
                            lastHoleEndate = nextDay(entry.getYyyymmdd());
                            if (lastHoleEndate.compareTo(endDate) > 0)
                                lastHoleEndate = endDate;
                        }
                        
                        /*
                         * If the start of the next potential hole is the same as the date indexed field there is no need to start creating and index hole
                         * starting on that date so increment the holeStart and find the next indexed date.
                         */
                        if (holeStart.equals(entry.getYyyymmdd())) {
                            holeStart = nextDay(entry.getYyyymmdd());
                            continue;
                        }
                        
                        // At this point, create a new FieldIndexHole
                        if (bounds.withinBounds(holeStart)) {
                            newHole = new FieldIndexHole();
                            newHole.setFieldName(field);
                            newHole.setStartDate(holeStart);
                            // you have to see next date the the field was indexed in the next iteration
                            // before you can set the end date. That may get done outside the loop on line 1698
                            // or on 1669 with the previous day of the next date the field was indexed
                            config.addFieldIndexHole(newHole);
                        }
                    }
                    holeStart = nextDay(entry.getYyyymmdd());
                }
            }
            
            if (newHole != null)
                newHole.setEndDate(lastHoleEndate);
            
            if (foundHolesInDateBounds && bounds.withinBounds(holeStart)) {
                FieldIndexHole trailingHole = new FieldIndexHole(field, new String[] {holeStart, endDate});
                config.addFieldIndexHole(trailingHole);
            }
            
            if (!foundHolesInDateBounds) {
                FieldIndexHole trailingHole = new FieldIndexHole(field, new String[] {startDate, endDate});
                config.addFieldIndexHole(trailingHole);
            }
            
            if (!config.getFieldIndexHoles().isEmpty()) {
                log.debug("Found Field index holes for field: " + field + " within date bounds");
                for (FieldIndexHole hole : config.getFieldIndexHoles()) {
                    log.debug(hole.toString());
                }
            } else {
                log.debug("No fieldHoles created.");
            }
            
        }
        
    }
    
    private static String previousDay(String day) {
        return YearMonthDay.previousDay(day).getYyyymmdd();
    }
    
    private static String nextDay(String day) {
        return YearMonthDay.nextDay(day).getYyyymmdd();
    }
    
}
