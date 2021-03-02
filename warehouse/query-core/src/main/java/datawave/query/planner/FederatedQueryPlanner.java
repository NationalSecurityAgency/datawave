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
        TreeSet<YearMonthDay> holeDates = new TreeSet<>();
        
        CloseableIterable<QueryData> queryData = super.process(config, query, settings, scannerFactory);
        
        setDoCalculateFieldIndexHoles(false);
        
        if (config instanceof ShardQueryConfiguration) {
            List<FieldIndexHole> fieldIndexHoles = ((ShardQueryConfiguration) config).getFieldIndexHoles();
            List<ValueIndexHole> valueIndexHoles = ((ShardQueryConfiguration) config).getValueIndexHoles();
            if ((valueIndexHoles == null && fieldIndexHoles == null) || (valueIndexHoles.size() == 0 && fieldIndexHoles.size() == 0)) {
                returnQueryData.addDelegate(queryData);
                return returnQueryData;
            }
            
            // TODO Need to iterate from originalEndDate to originalStartDate
            for (ValueIndexHole valueIndexHole : valueIndexHoles) {
                for (FieldIndexHole fieldIndexHole : fieldIndexHoles) {
                    if (fieldIndexHole.overlaps(valueIndexHole.getStartDate(), valueIndexHole.getEndDate())) {
                        
                        ShardQueryConfiguration tempConfig = new ShardQueryConfiguration((ShardQueryConfiguration) config);
                        tempConfig.setBeginDate(DateHelper.parse(fieldIndexHole.getStartDate()));
                        tempConfig.setEndDate(DateHelper.parse(fieldIndexHole.getEndDate()));
                        queryData = super.process(tempConfig, query, settings, scannerFactory);
                        returnQueryData.addDelegate(queryData);
                        log.debug("The field index and value index overlap");
                        log.debug("FieldIndexHole " + fieldIndexHole);
                        log.debug("ValueIndexHole " + valueIndexHole);
                        // Build up the returnQueryData
                    }
                }
            }
        }
        
        if (!returnQueryData.iterator().hasNext())
            returnQueryData.addDelegate(queryData);
        
        return returnQueryData;
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
    
}
