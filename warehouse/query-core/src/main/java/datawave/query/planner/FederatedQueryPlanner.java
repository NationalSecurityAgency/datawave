package datawave.query.planner;

import datawave.query.CloseableIterable;
import datawave.query.config.FieldIndexHole;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.config.ValueIndexHole;
import datawave.query.exceptions.DatawaveQueryException;
import datawave.query.planner.pushdown.rules.PushDownRule;
import datawave.query.tables.CountingShardQueryLogic;
import datawave.query.tables.ScannerFactory;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.MetadataHelper;
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

import java.awt.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

public class FederatedQueryPlanner {
    
    QueryPlanner originalQueryPlanner;
    ShardQueryLogic originalShardedQueryLogic;
    CountingShardQueryLogic originalcountingShardQueryLogic;
    
    private static final Logger log = ThreadConfigurableLogger.getLogger(FederatedQueryPlanner.class);
    {
        log.setLevel(Level.DEBUG);
    }
    
    public FederatedQueryPlanner(QueryPlanner original, ShardQueryLogic logic) {
        // originalQueryPlanner.metadataHelper.getAllFieldMetadataHelper().getIndexDates();
        originalQueryPlanner = original;
        originalShardedQueryLogic = logic;
    }
    
    public FederatedQueryDataIterable process(GenericQueryConfiguration config, String query, Query settings, ScannerFactory scannerFactory,
                    CloseableIterable<QueryData> queryData) throws DatawaveQueryException {
        
        FederatedQueryDataIterable returnQueryData = new FederatedQueryDataIterable();
        Date originalEndDate = config.getEndDate();
        Date originalStartDate = config.getBeginDate();
        
        if (originalQueryPlanner instanceof DefaultQueryPlanner)
            ((DefaultQueryPlanner) originalQueryPlanner).setDoCalculateFieldIndexHoles(false);
        
        if (config instanceof ShardQueryConfiguration && ((ShardQueryConfiguration) config).getFieldIndexHoles().size() > 0
                        && ((ShardQueryConfiguration) config).getFieldIndexHoles().size() > 0) {
            List<FieldIndexHole> fieldIndexHoles = ((ShardQueryConfiguration) config).getFieldIndexHoles();
            List<ValueIndexHole> valueIndexHoles = ((ShardQueryConfiguration) config).getValueIndexHoles();
            if (valueIndexHoles == null || fieldIndexHoles == null) {
                returnQueryData.addDelegate(queryData);
                return returnQueryData;
            }
            
            // TODO Need to iterate from originalEndDate to originalStartDate
            for (ValueIndexHole valueIndexHole : valueIndexHoles) {
                for (FieldIndexHole fieldIndexHole : fieldIndexHoles) {
                    if (fieldIndexHole.overlaps(valueIndexHole.getStartDate(), valueIndexHole.getEndDate())) {
                        config.setBeginDate(DateHelper.parse(fieldIndexHole.getStartDate()));
                        config.setEndDate(DateHelper.parse(fieldIndexHole.getEndDate()));
                        queryData = originalQueryPlanner.process(config, query, settings, scannerFactory);
                        returnQueryData.addDelegate(queryData);
                        log.debug("The field index and value index overlap");
                        log.debug("FieldIndexHole " + fieldIndexHole);
                        log.debug("ValueIndexHole " + valueIndexHole);
                        // Build up the returnQueryData
                    }
                }
            }
        }
        
        config.setBeginDate(originalStartDate);
        config.setEndDate(originalEndDate);
        
        return returnQueryData;
    }
    
    public CountingShardQueryLogic getOriginalcountingShardQueryLogic() {
        return originalcountingShardQueryLogic;
    }
    
    public void setOriginalcountingShardQueryLogic(CountingShardQueryLogic originalcountingShardQueryLogic) {
        this.originalcountingShardQueryLogic = originalcountingShardQueryLogic;
    }
    
}
