package datawave.query.planner;

import datawave.query.CloseableIterable;
import datawave.query.config.FieldIndexHole;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.config.ValueIndexHole;
import datawave.query.exceptions.DatawaveQueryException;
import datawave.query.planner.pushdown.rules.PushDownRule;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.MetadataHelper;
import datawave.query.util.YearMonthDay;
import datawave.util.time.DateHelper;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.configuration.QueryData;
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
        
        // The DefaultQueryPlanner.process needs to be called so that the FieldIndexHoles can be calculated
        // and the queryData returned will be used if there are no index holes of any kind.
        CloseableIterable<QueryData> queryData = super.process(config, query, settings, scannerFactory);
        
        setDoCalculateFieldIndexHoles(false);
        
        if (config instanceof ShardQueryConfiguration) {
            List<FieldIndexHole> fieldIndexHoles = ((ShardQueryConfiguration) config).getFieldIndexHoles();
            List<ValueIndexHole> valueIndexHoles = ((ShardQueryConfiguration) config).getValueIndexHoles();
            if ((valueIndexHoles == null && fieldIndexHoles == null) || (valueIndexHoles.size() == 0 && fieldIndexHoles.size() == 0)) {
                returnQueryData.addDelegate(queryData);
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
                
                queryData = getQueryData((ShardQueryConfiguration) config, query, settings, scannerFactory, startDate, endDate);
                returnQueryData.addDelegate(queryData);
                
            }
            
        }
        
        if (!returnQueryData.iterator().hasNext())
            returnQueryData.addDelegate(queryData);
        
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
    
}
