package datawave.query.rewrite.config;

import java.util.Map.Entry;

import datawave.query.rewrite.planner.DefaultQueryPlanner;
import datawave.query.rewrite.planner.QueryPlanner;
import datawave.query.rewrite.tables.RefactoredShardQueryLogic;
import datawave.query.rewrite.tld.CreateTLDUidsIterator;
import datawave.query.rewrite.tld.TLDQueryIterator;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.logic.BaseQueryLogic;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

public class LookupUUIDTune implements Profile {
    
    protected boolean bypassAccumulo = false;
    protected boolean enableCaching = false;
    protected boolean disableComplexFunctions = false;
    protected boolean reduceResponse = false;
    protected boolean enablePreload = false;
    protected boolean speculativeScanning = false;
    
    @Override
    public void configure(BaseQueryLogic<Entry<Key,Value>> logic) {
        if (logic instanceof RefactoredShardQueryLogic) {
            RefactoredShardQueryLogic rsq = RefactoredShardQueryLogic.class.cast(logic);
            rsq.setBypassAccumulo(bypassAccumulo);
            rsq.setSpeculativeScanning(speculativeScanning);
            rsq.setCacheModel(enableCaching);
            if (reduceResponse) {
                rsq.setCreateUidsIteratorClass(CreateTLDUidsIterator.class);
            }
        }
        
    }
    
    @Override
    public void configure(QueryPlanner planner) {
        if (planner instanceof DefaultQueryPlanner) {
            DefaultQueryPlanner dqp = DefaultQueryPlanner.class.cast(planner);
            dqp.setCacheDataTypes(enableCaching);
            dqp.setCondenseUidsInRangeStream(false);
            if (disableComplexFunctions) {
                dqp.setDisableAnyFieldLookup(true);
                dqp.setDisableBoundedLookup(true);
                dqp.setDisableCompositeFields(true);
                dqp.setDisableExpandIndexFunction(true);
                dqp.setDisableRangeCoalescing(true);
                dqp.setDisableTestNonExistentFields(true);
                if (reduceResponse)
                    dqp.setQueryIteratorClass(TLDQueryIterator.class);
            }
            if (enablePreload) {
                dqp.setPreloadOptions(true);
            }
        }
        
    }
    
    @Override
    public void configure(GenericQueryConfiguration configuration) {
        if (configuration instanceof RefactoredShardQueryConfiguration) {
            RefactoredShardQueryConfiguration rsqc = RefactoredShardQueryConfiguration.class.cast(configuration);
            rsqc.setTldQuery(reduceResponse);
            rsqc.setBypassAccumulo(bypassAccumulo);
            rsqc.setSerializeQueryIterator(true);
            rsqc.setMaxEvaluationPipelines(1);
            rsqc.setMaxPipelineCachedResults(1);
            // we need this since we've finished the deep copy already
            rsqc.setSpeculativeScanning(speculativeScanning);
        }
    }
    
    public boolean getSpeculativeScanning() {
        return speculativeScanning;
    }
    
    public void setSpeculativeScanning(boolean speculativeScanning) {
        this.speculativeScanning = speculativeScanning;
    }
    
    public void setBypassAccumulo(boolean bypassAccumulo) {
        this.bypassAccumulo = bypassAccumulo;
    }
    
    public boolean getBypassAccumulo() {
        return bypassAccumulo;
    }
    
    public void setEnableCaching(boolean enableCaching) {
        this.enableCaching = enableCaching;
    }
    
    public boolean getEnableCaching() {
        return enableCaching;
    }
    
    public void setEnablePreload(boolean enablePreload) {
        this.enablePreload = enablePreload;
    }
    
    public boolean getEnablePreload() {
        return enablePreload;
    }
    
    public boolean getDisableComplexFunctions() {
        return disableComplexFunctions;
    }
    
    public void setDisableComplexFunctions(boolean disableComplexFunctions) {
        this.disableComplexFunctions = disableComplexFunctions;
    }
    
    public void setReduceResponse(boolean forceTld) {
        this.reduceResponse = forceTld;
    }
    
    public boolean getReduceResponse() {
        return reduceResponse;
    }
}
