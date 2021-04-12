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
    
    public FederatedQueryPlanner() {
        super();
    }
    
    @Override
    public FederatedQueryDataIterable process(GenericQueryConfiguration config, String query, Query settings, ScannerFactory scannerFactory)
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
            // if (log.isDebugEnabled() && log.getLevel().equals(Level.DEBUG))
            checkForErrorsInFieldIndexHoles((ShardQueryConfiguration) config);
            List<ValueIndexHole> valueIndexHoles = ((ShardQueryConfiguration) config).getValueIndexHoles();
            holeDates = generateStartAndEndDates((ShardQueryConfiguration) config);
            if ((valueIndexHoles == null && fieldIndexHoles == null) || (valueIndexHoles.size() == 0 && fieldIndexHoles.size() == 0) || holeDates.size() == 0) {
                results = super.process(config, query, settings, scannerFactory);
                returnQueryData.addDelegate(results);
                return returnQueryData;
            }
            
            boolean firstIteration = true;
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
            
        }
        
        return returnQueryData;
    }
    
    /*
     * This function removes improperly calculate field index holes and will be removed as soon as all debugging in ticket #825 is complete. I need to see that
     * the test don't pass because of bad index holes.
     */
    
    private void checkForErrorsInFieldIndexHoles(ShardQueryConfiguration config) {
        ArrayList<FieldIndexHole> removalList = new ArrayList<>();
        for (FieldIndexHole fieldIndexHole : config.getFieldIndexHoles()) {
            if (fieldIndexHole.getStartDate().compareTo(fieldIndexHole.getEndDate()) > 0) {
                log.error("There was a problem calculating the FieldIndexHole " + fieldIndexHole);
                log.error("End date in feild Index hole can't come before start date.");
                removalList.add(fieldIndexHole);
                log.info("Invalid field index hole removed " + fieldIndexHole);
            }
        }
        config.getFieldIndexHoles().removeAll(removalList);
    }
    
    private CloseableIterable<QueryData> getQueryData(ShardQueryConfiguration config, String query, Query settings, ScannerFactory scannerFactory,
                    Date startDate, Date endDate) throws DatawaveQueryException {
        log.debug("getQueryData in the FederatedQueryPlanner is called ");
        CloseableIterable<QueryData> queryData;
        ShardQueryConfiguration tempConfig = new ShardQueryConfiguration(config);
        tempConfig.setBeginDate(startDate);
        tempConfig.setEndDate(endDate);
        // TODO: I think it is unnecessary to clone the DefaultQueryPlanner but I think it was requested.
        DefaultQueryPlanner tempPlanner = new DefaultQueryPlanner(this);
        queryData = tempPlanner.process(tempConfig, query, settings, scannerFactory);
        return queryData;
    }
    
    private TreeSet<YearMonthDay> generateStartAndEndDates(ShardQueryConfiguration configuration) {
        
        String startDate = DateHelper.format(configuration.getBeginDate().getTime());
        String endDate = DateHelper.format(configuration.getEndDate().getTime());
        
        YearMonthDay.Bounds bounds = new YearMonthDay.Bounds(startDate, false, endDate, false);
        
        TreeSet<YearMonthDay> queryDates = new TreeSet<>();
        for (ValueIndexHole valueIndexHole : configuration.getValueIndexHoles()) {
            addDatesToSet(bounds, queryDates, valueIndexHole.getStartDate());
            addDatesToSet(bounds, queryDates, valueIndexHole.getEndDate());
        }
        
        for (FieldIndexHole fieldIndexHole : configuration.getFieldIndexHoles()) {
            // TODO remove comparison below. This may be wrong.
            if (fieldIndexHole.getStartDate().compareTo(fieldIndexHole.getEndDate()) == 0) {
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
        String queryStartDate = DateHelper.format(config.getBeginDate().getTime());
        String queryEndDate = DateHelper.format(config.getEndDate().getTime());
        String holeStart = queryStartDate;
        YearMonthDay.Bounds bounds = new YearMonthDay.Bounds(queryStartDate, false, queryEndDate, false);
        long numDaysInQuery = YearMonthDay.getNumOfDaysBetween(new YearMonthDay(queryStartDate), new YearMonthDay(queryEndDate));
        long diffBetweenQueryStartAndIndexStart;
        int bitSetStartIndex;
        log.debug("startDate is: " + queryStartDate + " and endDate is " + queryEndDate);
        
        int dateComparison;
        
        /*
         * Here are ways the Dates represented in the indexedDates.bitset may overlap query range for a field. Case #1 (indexed before query start date only -
         * whole query range is a hole ) Case #2:(indexed partially into the query range - beginning after query start date) Case #3:(indexed partially inside
         * of query range - extending past query end date) Case #4 (indexed completely after the query range - whole query range is a hole) Case #5 (index
         * completely covers the query range - no holes) Case #6 no field indexed dates are in the metadata so the whole range is a hole.
         */
        
        for (String field : fieldToDatatypeMap.keySet()) {
            indexedDates = metadataHelper.getIndexDates(field, config.getDatatypeFilter());
            if (indexedDates != null && indexedDates.getIndexedDatesBitSet() != null) {
                if (indexedDates != null && indexedDates.getIndexedDatesBitSet().size() > 0) {
                    
                    // Check if the query end date came before the first indexed date.
                    // If the query end date comes before the first indexed date of the field
                    // then the field was not indexed within the query range and the entire
                    // query range is a "Field hole"
                    if (queryEndDate.compareTo(indexedDates.getStartDay().getYyyymmdd()) < 0) {
                        // CASE #4 in the above diagram has occurred.
                        FieldIndexHole queryRangeIsAHole = new FieldIndexHole(field, queryStartDate, queryEndDate);
                        config.getFieldIndexHoles().add(queryRangeIsAHole);
                        // Get the next field to calculate field index holes.
                        continue;
                    }
                    
                    // CASE #1 from above
                    if (queryStartDate.compareTo(indexedDates.getStartDay().getYyyymmdd()) > 0) {
                        long numDaysIndexedBeforeQuery = YearMonthDay.getNumOfDaysBetween(indexedDates.getStartDay(), new YearMonthDay(queryStartDate));
                        if (numDaysIndexedBeforeQuery >= indexedDates.getIndexedDatesBitSet().length()) {
                            FieldIndexHole queryRangeIsAHole = new FieldIndexHole(field, queryStartDate, queryEndDate);
                            config.getFieldIndexHoles().add(queryRangeIsAHole);
                            // Get the next field to calculate field index holes.
                            continue;
                        }
                    }
                    
                    // CASE #5
                    if (indexedDates.getStartDay().getYyyymmdd().compareTo(queryStartDate) < 0) {
                        long numDaysIndexedBeforeQuery = YearMonthDay.getNumOfDaysBetween(indexedDates.getStartDay(), new YearMonthDay(queryStartDate));
                        if ((numDaysInQuery + numDaysIndexedBeforeQuery) < numDaysIndexedBeforeQuery + indexedDates.getIndexedDatesBitSet().length())
                            continue;
                        
                    }
                    
                    // If the field does have dates that are indexed in the query
                    dateComparison = queryStartDate.compareTo(indexedDates.getStartDay().getYyyymmdd());
                    
                    if (dateComparison < 0) { // Query Start date is before the indexedDates.getStartDay
                        // Create a hole from the start date to the first date the field was indexed
                        diffBetweenQueryStartAndIndexStart = YearMonthDay.getNumOfDaysBetween(new YearMonthDay(queryStartDate), indexedDates.getStartDay());
                        if (diffBetweenQueryStartAndIndexStart > Integer.MAX_VALUE) {
                            log.error("Date span is too long create bitset for indexes - this should not happen");
                            log.error("Will not attempt to create FieldIndexHoles for field: " + field);
                            continue;
                        } else { // There is a series of days where the field is not indexed.
                        
                            bitSetStartIndex = (int) diffBetweenQueryStartAndIndexStart;
                            // CASE #2 from long comment above.
                            // 1. Create an FieldIndexHole from query start date to the day before indexedDates.getStartDay()
                            // Line below should be correct but breaks the build
                            // TODO Figure out what the first index hole is without breaking any tests.
                            // config.getFieldIndexHoles().add(new FieldIndexHole(field, queryStartDate, indexedDates.getStartDay().getYyyymmdd()));
                            // TODO Use the bitset object to create the remaining field index holes
                            
                        }
                    } else if (dateComparison > 0)//
                    {
                        diffBetweenQueryStartAndIndexStart = YearMonthDay.getNumOfDaysBetween(indexedDates.getStartDay(), new YearMonthDay(queryStartDate));
                        if (diffBetweenQueryStartAndIndexStart > Integer.MAX_VALUE) {
                            log.error("Date span is too long create bitset for indexes - this should not happen");
                            log.error("Will not attempt to create FieldIndexHoles for field: " + field);
                            continue;
                        } else {
                            bitSetStartIndex = (int) diffBetweenQueryStartAndIndexStart;
                            
                        }
                        
                    } else {
                        bitSetStartIndex = 0;
                    }
                    
                    /*
                     * for (int dayIndex = 0; dayIndex < numDaysRepresentedInBitset; dayIndex++) { if
                     * (nextIndexedDatesValue.getIndexedDatesBitSet().get(dayIndex)) { accumulatedDatesBitset.set(dayIndex + aggregatedBitSetIndex); }
                     * nextDateToContinueBitset = YearMonthDay.nextDay(nextDateToContinueBitset.getYyyymmdd()); aggregatedBitSetIndex++; }
                     */
                    
                    for (int bitSetIndex = bitSetStartIndex; bitSetIndex < indexedDates.getIndexedDatesBitSet().length(); bitSetIndex++) {
                        // Create the FieldIndexHoles and put them in the config.
                    }
                } else {
                    // CASE #6 from above. Hole range is a hole for this field
                    FieldIndexHole queryRangeIsAHole = new FieldIndexHole(field, queryStartDate, queryEndDate);
                    config.getFieldIndexHoles().add(queryRangeIsAHole);
                    // Get the next field to calculate field index hole
                }
            }
            
        }
        
    }
    
    private void addFieldIndexHoleToConfig(ShardQueryConfiguration config, FieldIndexHole fieldIndexHole) {
        config.addFieldIndexHole(fieldIndexHole);
    }
    
    private static String previousDay(String day) {
        return YearMonthDay.previousDay(day).getYyyymmdd();
    }
    
    private static String nextDay(String day) {
        return YearMonthDay.nextDay(day).getYyyymmdd();
    }
    
}
