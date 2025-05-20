package datawave.query.config;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.query.Constants;
import datawave.query.language.parser.QueryParser;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.planner.QueryPlanner;
import datawave.query.planner.rules.NodeTransformRule;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tld.TLDQueryIterator;

public class LookupUUIDTune implements Profile {

    protected boolean bypassAccumulo = false;
    protected boolean enableCaching = false;
    protected boolean disableComplexFunctions = false;
    protected boolean reduceResponse = false;
    protected boolean enablePreload = false;
    protected boolean speculativeScanning = false;
    // lookup uuid profiles can override seeking configs for field index and event keys
    protected int fiFieldSeek = -1;
    protected int fiNextSeek = -1;
    protected int eventFieldSeek = -1;
    protected int eventNextSeek = -1;
    protected String queryIteratorClass = TLDQueryIterator.class.getCanonicalName();
    protected int maxShardsPerDayThreshold = -1;
    protected int pageByteTrigger = -1;
    protected int maxPageSize = -1;
    protected Map<String,List<String>> primaryToSecondaryFieldMap = Collections.emptyMap();
    protected boolean trackSizes = true;
    protected boolean reduceFields = false;
    protected int reduceFieldCount = -1;
    protected boolean reduceFieldsPreQueryEvaluation = false;
    protected String limitFieldsField = null;
    protected boolean reduceQuery = false;
    private boolean enforceUniqueTermsWithinExpressions = false;
    private boolean reduceQueryFields = false;
    private boolean seekingEventAggregation;
    protected List<NodeTransformRule> transforms = null;
    protected Map<String,QueryParser> querySyntaxParsers = null;

    @Override
    public void configure(BaseQueryLogic<Entry<Key,Value>> logic) {
        if (logic instanceof ShardQueryLogic) {
            ShardQueryLogic rsq = ShardQueryLogic.class.cast(logic);
            rsq.setBypassAccumulo(bypassAccumulo);
            rsq.setSpeculativeScanning(speculativeScanning);
            rsq.setCacheModel(enableCaching);
            rsq.setPrimaryToSecondaryFieldMap(primaryToSecondaryFieldMap);
            rsq.setEnforceUniqueTermsWithinExpressions(enforceUniqueTermsWithinExpressions);
            rsq.setReduceQueryFields(reduceQueryFields);

            rsq.setFiFieldSeek(getFiFieldSeek());
            rsq.setFiNextSeek(getFiNextSeek());
            rsq.setEventFieldSeek(getEventFieldSeek());
            rsq.setEventNextSeek(getEventNextSeek());
            rsq.setSeekingEventAggregation(isSeekingEventAggregation());

            if (querySyntaxParsers != null) {
                rsq.setQuerySyntaxParsers(querySyntaxParsers);
            }

            if (reduceResponse) {
                rsq.setParseTldUids(true);

                // pass through seek options
                rsq.setFiFieldSeek(fiFieldSeek);
                rsq.setFiNextSeek(fiNextSeek);
                rsq.setEventFieldSeek(eventFieldSeek);
                rsq.setEventNextSeek(eventNextSeek);

                if (maxPageSize != -1) {
                    rsq.setMaxPageSize(maxPageSize);
                }

                if (pageByteTrigger != -1) {
                    rsq.setPageByteTrigger(pageByteTrigger);
                }
            }
        }
    }

    @Override
    public void configure(QueryPlanner planner) {
        if (planner instanceof DefaultQueryPlanner) {
            DefaultQueryPlanner dqp = DefaultQueryPlanner.class.cast(planner);
            dqp.setCacheDataTypes(enableCaching);

            if (transforms != null) {
                dqp.setTransformRules(transforms);
            }

            if (disableComplexFunctions) {
                dqp.setDisableAnyFieldLookup(true);
                dqp.setDisableBoundedLookup(true);
                dqp.setDisableCompositeFields(true);
                dqp.setDisableExpandIndexFunction(true);
                dqp.setDisableTestNonExistentFields(true);
                if (reduceResponse)
                    try {
                        Class iteratorClass = Class.forName(this.queryIteratorClass);
                        dqp.setQueryIteratorClass(iteratorClass);
                    } catch (ClassNotFoundException e) {
                        throw new IllegalStateException("Cannot Instantiate queryIteratorClass: " + this.queryIteratorClass, e);
                    }
            }
            if (enablePreload) {
                dqp.setPreloadOptions(true);
            }
        }
    }

    @Override
    public void configure(GenericQueryConfiguration configuration) {
        if (configuration instanceof ShardQueryConfiguration) {
            ShardQueryConfiguration rsqc = ShardQueryConfiguration.class.cast(configuration);
            rsqc.setTldQuery(reduceResponse);
            rsqc.setBypassAccumulo(bypassAccumulo);
            rsqc.setSerializeQueryIterator(true);
            rsqc.setMaxEvaluationPipelines(1);
            rsqc.setMaxPipelineCachedResults(1);
            if (maxShardsPerDayThreshold != -1) {
                rsqc.setShardsPerDayThreshold(maxShardsPerDayThreshold);
            }

            rsqc.setFiFieldSeek(getFiFieldSeek());
            rsqc.setFiNextSeek(getFiNextSeek());
            rsqc.setEventFieldSeek(getEventFieldSeek());
            rsqc.setEventNextSeek(getEventNextSeek());
            rsqc.setSeekingEventAggregation(isSeekingEventAggregation());

            // we need this since we've finished the deep copy already
            rsqc.setSpeculativeScanning(speculativeScanning);
            rsqc.setTrackSizes(trackSizes);

            if (reduceResponse) {
                if (reduceFields && reduceFieldCount != -1) {
                    Set<String> fieldLimits = new HashSet<>(1);
                    fieldLimits.add(Constants.ANY_FIELD + "=" + reduceFieldCount);
                    rsqc.setLimitFields(fieldLimits);
                    rsqc.setLimitFieldsPreQueryEvaluation(reduceFieldsPreQueryEvaluation);
                    rsqc.setLimitFieldsField(limitFieldsField);
                }
            }
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

    public int getFiFieldSeek() {
        return fiFieldSeek;
    }

    public void setFiFieldSeek(int fiFieldSeek) {
        this.fiFieldSeek = fiFieldSeek;
    }

    public int getFiNextSeek() {
        return fiNextSeek;
    }

    public void setFiNextSeek(int fiNextSeek) {
        this.fiNextSeek = fiNextSeek;
    }

    public int getEventFieldSeek() {
        return eventFieldSeek;
    }

    public void setEventFieldSeek(int eventFieldSeek) {
        this.eventFieldSeek = eventFieldSeek;
    }

    public int getEventNextSeek() {
        return eventNextSeek;
    }

    public void setEventNextSeek(int eventNextSeek) {
        this.eventNextSeek = eventNextSeek;
    }

    public void setQueryIteratorClass(String queryIteratorClass) {
        this.queryIteratorClass = queryIteratorClass;
    }

    public String getQueryIteratorClass() {
        return queryIteratorClass;
    }

    @Deprecated(since = "7.1.0", forRemoval = true)
    public int getMaxShardsPerDayThreshold() {
        return maxShardsPerDayThreshold;
    }

    @Deprecated(since = "7.1.0", forRemoval = true)
    public void setMaxShardsPerDayThreshold(int maxShardsPerDayThreshold) {
        this.maxShardsPerDayThreshold = maxShardsPerDayThreshold;
    }

    public int getPageByteTrigger() {
        return pageByteTrigger;
    }

    public void setPageByteTrigger(int pageByteTrigger) {
        this.pageByteTrigger = pageByteTrigger;
    }

    public int getMaxPageSize() {
        return maxPageSize;
    }

    public void setMaxPageSize(int maxPageSize) {
        this.maxPageSize = maxPageSize;
    }

    public void setPrimaryToSecondaryFieldMap(Map<String,List<String>> primaryToSecondaryFieldMap) {
        this.primaryToSecondaryFieldMap = primaryToSecondaryFieldMap;
    }

    public Map<String,List<String>> getPrimaryToSecondaryFieldMap() {
        return primaryToSecondaryFieldMap;
    }

    public boolean isTrackSizes() {
        return trackSizes;
    }

    public void setTrackSizes(boolean trackSizes) {
        this.trackSizes = trackSizes;
    }

    public boolean isReduceFields() {
        return reduceFields;
    }

    public void setReduceFields(boolean reduceFields) {
        this.reduceFields = reduceFields;
    }

    public int getReduceFieldCount() {
        return reduceFieldCount;
    }

    public void setReduceFieldCount(int reduceFieldCount) {
        this.reduceFieldCount = reduceFieldCount;
    }

    public boolean isReduceFieldsPreQueryEvaluation() {
        return reduceFieldsPreQueryEvaluation;
    }

    public void setReduceFieldsPreQueryEvaluation(boolean reduceFieldsPreQueryEvaluation) {
        this.reduceFieldsPreQueryEvaluation = reduceFieldsPreQueryEvaluation;
    }

    public void setLimitFieldsField(String limitFieldsField) {
        this.limitFieldsField = limitFieldsField;
    }

    public String getLimitFieldsField() {
        return limitFieldsField;
    }

    public boolean isReduceQuery() {
        return reduceQuery;
    }

    public void setReduceQuery(boolean reduceQuery) {
        this.reduceQuery = reduceQuery;
    }

    public boolean isEnforceUniqueTermsWithinExpressions() {
        return enforceUniqueTermsWithinExpressions;
    }

    public void setEnforceUniqueTermsWithinExpressions(boolean enforceUniqueTermsWithinExpressions) {
        this.enforceUniqueTermsWithinExpressions = enforceUniqueTermsWithinExpressions;
    }

    public boolean getReduceQueryFields() {
        return this.reduceQueryFields;
    }

    public void setReduceQueryFields(boolean reduceQueryFields) {
        this.reduceQueryFields = reduceQueryFields;
    }

    public List<NodeTransformRule> getTransforms() {
        return transforms;
    }

    public void setTransforms(List<NodeTransformRule> transforms) {
        this.transforms = transforms;
    }

    public Map<String,QueryParser> getQuerySyntaxParsers() {
        return querySyntaxParsers;
    }

    public void setQuerySyntaxParsers(Map<String,QueryParser> querySyntaxParsers) {
        this.querySyntaxParsers = querySyntaxParsers;
    }

    public boolean isSeekingEventAggregation() {
        return seekingEventAggregation;
    }

    public void setSeekingEventAggregation(boolean seekingEventAggregation) {
        this.seekingEventAggregation = seekingEventAggregation;
    }
}
