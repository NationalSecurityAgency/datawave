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
import java.util.List;

public class FederatedQueryPlanner extends QueryPlanner implements Cloneable {
    
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
    
    @Override
    public CloseableIterable<QueryData> process(GenericQueryConfiguration config, String query, Query settings, ScannerFactory scannerFactory)
                    throws DatawaveQueryException {
        if (config instanceof ShardQueryConfiguration) {
            List<FieldIndexHole> fieldIndexHoles = ((ShardQueryConfiguration) config).getFieldIndexHoles();
            List<ValueIndexHole> valueIndexHoles = ((ShardQueryConfiguration) config).getValueIndexHoles();
            for (ValueIndexHole valueIndexHole : valueIndexHoles) {
                for (FieldIndexHole fieldIndexHole : fieldIndexHoles) {
                    if (fieldIndexHole.overlaps(valueIndexHole.getStartDate(), valueIndexHole.getEndDate())) {
                        System.out.println("The field index and value index overlap");

                    }
                }
            }
            
        }
        
        return null;
    }
    
    @Override
    public long maxRangesPerQueryPiece() {
        return 0;
    }
    
    @Override
    public void close(GenericQueryConfiguration config, Query settings) {
        
    }
    
    @Override
    public void setQueryIteratorClass(Class<? extends SortedKeyValueIterator<Key,Value>> clazz) {
        
    }
    
    @Override
    public Class<? extends SortedKeyValueIterator<Key,Value>> getQueryIteratorClass() {
        return null;
    }
    
    @Override
    public String getPlannedScript() {
        return null;
    }
    
    @Override
    public QueryPlanner clone() {
        return null;
    }
    
    @Override
    public void setRules(Collection<PushDownRule> rules) {
        
    }
    
    @Override
    public Collection<PushDownRule> getRules() {
        return null;
    }
    
    @Override
    public ASTJexlScript applyRules(ASTJexlScript queryTree, ScannerFactory scannerFactory, MetadataHelper metadataHelper, ShardQueryConfiguration config) {
        return null;
    }
    
    public CountingShardQueryLogic getOriginalcountingShardQueryLogic() {
        return originalcountingShardQueryLogic;
    }
    
    public void setOriginalcountingShardQueryLogic(CountingShardQueryLogic originalcountingShardQueryLogic) {
        this.originalcountingShardQueryLogic = originalcountingShardQueryLogic;
    }
    
}
