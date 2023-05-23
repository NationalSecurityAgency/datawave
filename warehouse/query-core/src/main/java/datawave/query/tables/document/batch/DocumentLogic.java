package datawave.query.tables.document.batch;

import datawave.marking.MarkingFunctions;
import datawave.query.DocumentSerialization;
import datawave.query.config.DocumentQueryConfiguration;
import datawave.query.iterator.QueryOptions;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.planner.MetadataHelperQueryModelProvider;
import datawave.query.planner.QueryModelProvider;
import datawave.query.planner.QueryPlanner;
import datawave.query.planner.document.batch.DocumentQueryPlanner;
import datawave.query.scheduler.Scheduler;
import datawave.query.scheduler.document.pushdown.DocumentPushdownScheduler;
import datawave.query.tables.MyScannerFactory;
import datawave.query.tables.ScannerFactory;
import datawave.query.tables.ShardedBaseQueryLogic;
import datawave.query.tables.serialization.SerializedDocumentIfc;
import datawave.query.transformer.EventQueryDataDecoratorTransformer;
import datawave.query.transformer.JsonDocumentTransformer;
import datawave.query.transformer.JsonDocumentTransformerSupport;
import datawave.query.transformer.RawJsonDocumentTransformer;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.MetadataHelper;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.configuration.QueryData;
import datawave.webservice.query.logic.QueryLogicTransformer;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.Set;

/**
 * Extends the premise defined in shard query logic to support returning a SerializedDocumentIfc type through
 * the iterator.
 * */
public class DocumentLogic extends ShardedBaseQueryLogic<SerializedDocumentIfc, DocumentQueryConfiguration> {

    protected static final Logger log = ThreadConfigurableLogger.getLogger(DocumentLogic.class);

    /**
     * Basic constructor
     */
    public DocumentLogic() {
        super();
        if (log.isTraceEnabled())
            log.trace("Creating ShardQueryLogic: " + System.identityHashCode(this));
    }

    /**
     * Validate that the configuration is in a consistent state
     *
     * @throws IllegalArgumentException
     *             when config constraints are violated
     */
    @Override
    protected void validateConfiguration(DocumentQueryConfiguration config) {
    }

    /**
     * Copy constructor
     *
     * @param other
     *            - another ShardQueryLogic object
     */
    public DocumentLogic(DocumentLogic other) {
        super(other);

        if (log.isTraceEnabled())
            log.trace("Creating Cloned DocumentLogic: " + System.identityHashCode(this) + " from " + System.identityHashCode(other));

        // Set DocumentQueryConfiguration variables
        this.config = DocumentQueryConfiguration.create(other);

    }

    public static DocumentScannerBase createDocumentScanner(DocumentQueryConfiguration config, MyScannerFactory scannerFactory, QueryData qd, DocumentSerialization.ReturnType returnType) throws TableNotFoundException {
        final DocumentScannerBase bs = scannerFactory.newDocumentScanner(config.getShardTableName(), config.getAuthorizations(), config.getNumQueryThreads(),
                config.getQuery(),config.getDocRawFields(), returnType, config.getQueueCapacity() == 0 ? config.getNumQueryThreads() : config.getQueueCapacity(), config.getMaxTabletsPerRequest(), config.getMaxTabletThreshold());

        if (log.isTraceEnabled()) {
            log.trace("Running with " + config.getAuthorizations() + " and " + config.getNumQueryThreads() + " threads: " + qd);
        }

        bs.setRanges(qd.getRanges());

        for (IteratorSetting cfg : qd.getSettings()) {
            bs.addScanIterator(cfg);
        }

        return bs;
    }



    @Override
    public GenericQueryConfiguration initialize(AccumuloClient client, Query settings, Set<Authorizations> auths) throws Exception {

        this.config = DocumentQueryConfiguration.create(this, settings);
        if (log.isTraceEnabled())
            log.trace("Initializing DocumentLogic: " + System.identityHashCode(this) + '('
                    + (this.getSettings() == null ? "empty" : this.getSettings().getId()) + ')');
        this.config.setExpandFields(true);
        this.config.setExpandValues(true);
        initialize(config, client, settings, auths);
        return config;
    }

    @Override
    public String getPlan(AccumuloClient client, Query settings, Set<Authorizations> auths, boolean expandFields, boolean expandValues) throws Exception {

        this.config = DocumentQueryConfiguration.create(this, settings);
        if (log.isTraceEnabled())
            log.trace("Initializing ShardQueryLogic for plan: " + System.identityHashCode(this) + '('
                    + (this.getSettings() == null ? "empty" : this.getSettings().getId()) + ')');
        this.config.setExpandFields(expandFields);
        this.config.setExpandValues(expandValues);
        // if we are not generating the full plan, then set the flag such that we avoid checking for final executability/full table scan
        if (!expandFields || !expandValues) {
            this.config.setGeneratePlanOnly(true);
        }
        initialize(config, client, settings, auths);
        return config.getQueryString();
    }

    @Override
    public void configurePlanner(MetadataHelper metadataHelper, DateIndexHelper dateIndexHelper) {

        QueryPlanner queryPlanner = getQueryPlanner();
        if (queryPlanner instanceof DocumentQueryPlanner) {
            DocumentQueryPlanner currentQueryPlanner = (DocumentQueryPlanner) queryPlanner;

            currentQueryPlanner.setMetadataHelper(metadataHelper);
            currentQueryPlanner.setDateIndexHelper(dateIndexHelper);

            QueryModelProvider queryModelProvider = currentQueryPlanner.getQueryModelProviderFactory().createQueryModelProvider();
            if (queryModelProvider instanceof MetadataHelperQueryModelProvider) {
                ((MetadataHelperQueryModelProvider) queryModelProvider).setMetadataHelper(metadataHelper);
                ((MetadataHelperQueryModelProvider) queryModelProvider).setConfig(config);
            }

            if (null != queryModelProvider.getQueryModel()) {
                queryModel = queryModelProvider.getQueryModel();

            }
        }
        else  if (queryPlanner instanceof DefaultQueryPlanner) {
            DefaultQueryPlanner currentQueryPlanner = (DefaultQueryPlanner) queryPlanner;

            currentQueryPlanner.setMetadataHelper(metadataHelper);
            currentQueryPlanner.setDateIndexHelper(dateIndexHelper);

            QueryModelProvider queryModelProvider = currentQueryPlanner.getQueryModelProviderFactory().createQueryModelProvider();
            if (queryModelProvider instanceof MetadataHelperQueryModelProvider) {
                ((MetadataHelperQueryModelProvider) queryModelProvider).setMetadataHelper(metadataHelper);
                ((MetadataHelperQueryModelProvider) queryModelProvider).setConfig(config);
            }

            if (null != queryModelProvider.getQueryModel()) {
                queryModel = queryModelProvider.getQueryModel();

            }
        }
    }

    @Override
    protected Iterator<SerializedDocumentIfc> getDedupedIterator() {
        return new DocumentDedupingIterator(this.iterator);
    }

    protected String getStopwatchHeader(DocumentQueryConfiguration config) {
        return "DocumentLogic: " + config.getQueryString() + ", [" + config.getBeginDate() + ", " + config.getEndDate() + "]";
    }

    @Override
    public QueryLogicTransformer getTransformer(Query settings) {
        MarkingFunctions markingFunctions = this.getMarkingFunctions();
        ResponseObjectFactory responseObjectFactory = this.getResponseObjectFactory();

        boolean reducedInSettings = false;
        String reducedResponseStr = settings.findParameter(QueryOptions.REDUCED_RESPONSE).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(reducedResponseStr)) {
            reducedInSettings = Boolean.parseBoolean(reducedResponseStr);
        }
        boolean reduced = (this.isReducedResponse() || reducedInSettings);
        JsonDocumentTransformerSupport transformer = null;
        if (config.getDocRawFields()){
            transformer = new RawJsonDocumentTransformer(this, settings, markingFunctions, responseObjectFactory, reduced, config.getConvertToDocument());
        }
        else{
            transformer = new JsonDocumentTransformer(this, settings, markingFunctions, responseObjectFactory, reduced, config.getConvertToDocument());
        }
        transformer.setEventQueryDataDecoratorTransformer(eventQueryDataDecoratorTransformer);
        transformer.setContentFieldNames(getConfig().getContentFieldNames());
        transformer.setLogTimingDetails(this.getLogTimingDetails());
        transformer.setCardinalityConfiguration(cardinalityConfiguration);
        if (!config.getDocRawFields()) {
            transformer.setPrimaryToSecondaryFieldMap(primaryToSecondaryFieldMap);
        }
        transformer.setQm(queryModel);
        if (getConfig() != null) {
            transformer.setProjectFields(getConfig().getProjectFields());
            transformer.setBlacklistedFields(getConfig().getBlacklistedFields());
            /*
            if (getConfig().getUniqueFields() != null && !getConfig().getUniqueFields().isEmpty()) {
                transformer.addTransform(new UniqueTransform(this, getConfig().getUniqueFields()));
            }
            if (getConfig().getGroupFields() != null && !getConfig().getGroupFields().isEmpty()) {
                transformer.addTransform(new GroupingTransform(this, getConfig().getGroupFields()));
            }*/
        }
        return transformer;
    }

    @Override
    protected Scheduler<SerializedDocumentIfc> getScheduler(DocumentQueryConfiguration config, ScannerFactory scannerFactory) {
        return new DocumentPushdownScheduler(config, scannerFactory, metadataHelperFactory);
    }

    public EventQueryDataDecoratorTransformer getEventQueryDataDecoratorTransformer() {
        return eventQueryDataDecoratorTransformer;
    }

    public void setEventQueryDataDecoratorTransformer(EventQueryDataDecoratorTransformer eventQueryDataDecoratorTransformer) {
        this.eventQueryDataDecoratorTransformer = eventQueryDataDecoratorTransformer;
    }

    @Override
    public DocumentLogic clone() {
        return new DocumentLogic(this);
    }


    @Override
    public DocumentQueryConfiguration getConfig() {
        if (config == null) {
            config = DocumentQueryConfiguration.create();
        }

        return config;
    }

    public void setConfig(DocumentQueryConfiguration config) {
        this.config = config;
    }


    @Override
    public QueryPlanner getQueryPlanner() {
        if (null == planner) {
            planner = new DefaultQueryPlanner();
        }

        return planner;
    }


    public boolean getSerializeQueryIterator() {
        return getConfig().getSerializeQueryIterator();
    }

    public void setSerializeQueryIterator(boolean serializeQueryIterator) {
        getConfig().setSerializeQueryIterator(serializeQueryIterator);
    }

    public String getAllowedTypes() {
        return getConfig().getAllowedTypes();
    }

    public void setAllowedTypes(String allowedTypes){
        getConfig().setAllowedTypes(allowedTypes);
    }

    public boolean getForceAllTypes() {
        return getConfig().getForceAllTypes();
    }

    public void setForceAllTypes(boolean forceAllTypes) {
        getConfig().setForceAllTypes(forceAllTypes);
    }

    public Set<String> getProjectFields() {
        return getConfig().getProjectFields();
    }

    public void setProjectFields(Set<String> projectFields) {
        getConfig().setProjectFields(projectFields);
    }

    public boolean getTypeString() {
        return getConfig().getTypeString();
    }

    public void setTypeString(final Boolean setType){
        getConfig().setTypeString(setType);
    }

    public int getQueueCapacity() {
        return getConfig().getQueueCapacity();
    }

    public void setQueueCapacity(int queueCapacity){
        getConfig().setQueueCapacity(queueCapacity);
    }

    public boolean getPushdownLogic() {
        return getConfig().getPushdownLogic();
    }

    public void setPushdownLogic(boolean pushdownLogic){
        getConfig().setPushdownLogic(pushdownLogic);
    }

    public boolean getConvertToDocument() {
        return getConfig().getConvertToDocument();
    }

    public void setConvertToDocument(boolean document){
        getConfig().setConvertToDocument(document);
    }

    public int getInitialMaxTermThreshold() {
        return getConfig().getInitialMaxTermThreshold();
    }

    public void setInitialMaxTermThreshold(int initialMaxTermThreshold) {
        getConfig().setInitialMaxTermThreshold(initialMaxTermThreshold);
    }

    public int getFinalMaxTermThreshold() {
        return getConfig().getFinalMaxTermThreshold();
    }

    public void setFinalMaxTermThreshold(int finalMaxTermThreshold) {
        getConfig().setFinalMaxTermThreshold(finalMaxTermThreshold);
    }

    public boolean getDocRawFields() {
        return getConfig().getDocRawFields();
    }

    public void setDocRawFields(boolean rawDocFields) {
        getConfig().setDocRawFields(rawDocFields);
    }
}
