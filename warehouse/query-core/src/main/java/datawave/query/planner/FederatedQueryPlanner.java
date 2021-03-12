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
            List<ValueIndexHole> valueIndexHoles = ((ShardQueryConfiguration) config).getValueIndexHoles();
            holeDates = generateStartAndEndDates((ShardQueryConfiguration) config);
            if ((valueIndexHoles == null && fieldIndexHoles == null) || (valueIndexHoles.size() == 0 && fieldIndexHoles.size() == 0) || holeDates.size() == 0) {
                results = super.process(config, query, settings, scannerFactory);
                returnQueryData.addDelegate(results);
                return returnQueryData;
            }
            
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
            
        }
        
        return returnQueryData;
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
            // TODO remove comparison below. calculateFieldHoles needs to be fixed.
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
        String origStartDate = DateHelper.format(config.getBeginDate().getTime());
        String origEndDate = DateHelper.format(config.getEndDate().getTime());
        String holeStart = origStartDate;
        YearMonthDay.Bounds bounds = new YearMonthDay.Bounds(origStartDate, false, origEndDate, false);
        boolean firstHole = true;
        log.debug("startDate is: " + origStartDate + " and endDate is " + origEndDate);
        
        for (String field : fieldToDatatypeMap.keySet()) {
            indexedDates = metadataHelper.getIndexDates(field, config.getDatatypeFilter());
            if (indexedDates != null && indexedDates.getIndexedDatesSet().size() > 0) {
                for (Iterator<YearMonthDay> it = indexedDates.getIndexedDatesSet().iterator(); it.hasNext();) {
                    // Only create a hole if the indexed field are within date bounds
                    YearMonthDay entry = it.next();
                    // Only create a hole if the indexed field are within date bounds
                    if (bounds.withinBounds(entry)) {
                        if (firstHole) {
                            // create the FieldIndexHole for the dates the field was not indexed before the first
                            // time in the date range that it was indexed.
                            
                            if (holeStart.compareTo(entry.getYyyymmdd()) <= 0) { // holeStart comes before entry date
                                // Hole spans original start date to first date field was indexed
                                FieldIndexHole firstIndexHole = new FieldIndexHole(field, origStartDate, entry.getYyyymmdd());
                                holeStart = nextDay(entry.getYyyymmdd());
                                addFieldIndexHoleToConfig(config, firstIndexHole);
                                firstHole = false;
                                
                            } else {
                                holeStart = nextDay(entry.getYyyymmdd());
                                if (entry.getYyyymmdd().compareTo(holeStart) == 0)
                                    continue;
                                else {
                                    FieldIndexHole nextFieldIndexHole = new FieldIndexHole(field, holeStart, entry.getYyyymmdd());
                                    addFieldIndexHoleToConfig(config, nextFieldIndexHole);
                                    holeStart = nextDay(entry.getYyyymmdd());
                                }
                                
                            }
                        } else {
                            if (origEndDate.compareTo(entry.getYyyymmdd()) < 0) { // origEndDate came before the indexed date
                                holeStart = origStartDate;
                                firstHole = true;
                                break;
                            } else if (origEndDate.compareTo(entry.getYyyymmdd()) == 0) {
                                FieldIndexHole nextIndexHole = new FieldIndexHole(field, holeStart, previousDay(origEndDate));
                                holeStart = origStartDate;
                                addFieldIndexHoleToConfig(config, nextIndexHole);
                                firstHole = true;
                                break;
                            } else {
                                holeStart = nextDay(entry.getYyyymmdd());
                                if (entry.getYyyymmdd().compareTo(holeStart) == 0)
                                    continue;
                                else {
                                    FieldIndexHole nextFieldIndexHole = new FieldIndexHole(field, holeStart, entry.getYyyymmdd());
                                    addFieldIndexHoleToConfig(config, nextFieldIndexHole);
                                    
                                }
                                
                            }
                            
                        }
                    }// end of if (bounds.withinBounds(entry))
                    
                    if (!it.hasNext()) {
                        FieldIndexHole nextFieldIndexHole = new FieldIndexHole(field, holeStart, origEndDate);
                        addFieldIndexHoleToConfig(config, nextFieldIndexHole);
                    }
                }// end indexDates iteration
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
    
    private void addFieldIndexHoleToConfig(ShardQueryConfiguration configuration, FieldIndexHole fieldIndexHole) {
        configuration.addFieldIndexHole(fieldIndexHole);
    }
    
    private static String previousDay(String day) {
        return YearMonthDay.previousDay(day).getYyyymmdd();
    }
    
    private static String nextDay(String day) {
        return YearMonthDay.nextDay(day).getYyyymmdd();
    }
    
}
