package datawave.query.config;

import datawave.query.QueryParameters;
import datawave.query.tables.document.batch.DocumentLogic;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.Set;

/**
 * <p>
 * A GenericQueryConfiguration implementation that provides the additional logic on top of the traditional query that is needed to run a DATAWAVE sharded
 * boolean-logic query
 *
 * <p>
 * Provides support for normalizers, enricher classes, filter classes, projection, and datatype filters, in addition to additional parameters also exposed in
 * the Webservice QueryTable interface
 *
 * <p>
 * This class can be initialized with an instance of a DocumentLogic or ShardQueryTable which will grab the already configured parameters from the Accumulo
 * Webservice QueryTable and apply them to this configuration object
 */
public class DocumentQueryConfiguration extends ShardQueryConfiguration implements Serializable {
    
    private static final Logger log = Logger.getLogger(DocumentQueryConfiguration.class);
    
    /**
     * Query itertor class name
     */
    private String queryIteratorClazz = "";
    
    private boolean forceAllTypes = false;
    
    private String allowedTypes = "";
    private boolean typeString = true;
    
    private boolean customBatchScanner = false;
    
    private boolean docRawFields = false;
    
    private int queueCapacity = 1000;
    private int maxTabletsPerRequest = 0;
    private int maxTabletThreshold = 5000;
    private boolean pushdownLogic = false;
    private boolean convertToDocument = true;
    
    /**
     * Default constructor
     */
    public DocumentQueryConfiguration() {
        super();
    }
    
    /**
     * Performs a deep copy of the provided DocumentQueryConfiguration into a new instance
     *
     * @param other
     *            - another DocumentQueryConfiguration instance
     */
    public DocumentQueryConfiguration(DocumentQueryConfiguration other) {
        
        // GenericQueryConfiguration copy first
        super(other);
        this.setDocRawFields(other.getDocRawFields());
    }
    
    /**
     * Delegates deep copy work to appropriate constructor, sets additional values specific to the provided DocumentLogic
     *
     * @param logic
     *            - a DocumentLogic instance or subclass
     */
    public DocumentQueryConfiguration(DocumentLogic logic) {
        this((DocumentQueryConfiguration) logic.getConfig());
    }
    
    /**
     * Factory method that instantiates an fresh DocumentQueryConfiguration
     *
     * @return - a clean DocumentQueryConfiguration
     */
    public static DocumentQueryConfiguration create() {
        return new DocumentQueryConfiguration();
    }
    
    /**
     * Factory method that returns a deep copy of the provided DocumentQueryConfiguration
     *
     * @param other
     *            - another instance of a DocumentQueryConfiguration
     * @return - copy of provided DocumentQueryConfiguration
     */
    public static DocumentQueryConfiguration create(DocumentQueryConfiguration other) {
        return new DocumentQueryConfiguration(other);
    }
    
    /**
     * Factory method that creates a DocumentQueryConfiguration deep copy from a DocumentLogic
     *
     * @param DocumentLogic
     *            - a configured DocumentLogic
     * @return - a DocumentQueryConfiguration
     */
    public static DocumentQueryConfiguration create(DocumentLogic DocumentLogic) {

        DocumentQueryConfiguration config = create((DocumentQueryConfiguration) DocumentLogic.getConfig());

        // Lastly, honor overrides passed in via query parameters
        Set<QueryImpl.Parameter> parameterSet = config.getQuery().getParameters();
        for (QueryImpl.Parameter parameter : parameterSet) {
            String name = parameter.getParameterName();
            String value = parameter.getParameterValue();
            if (name.equals(QueryParameters.HIT_LIST)) {
                config.setHitList(Boolean.parseBoolean(value));
            }
            if (name.equals(QueryParameters.DATE_INDEX_TIME_TRAVEL)) {
                config.setDateIndexTimeTravel(Boolean.parseBoolean(value));
            }
            if (name.equals(QueryParameters.PARAMETER_MODEL_NAME)) {
                config.setMetadataTableName(value);
            }
        }

        return config;
    }
    
    /**
     * Factory method that creates a DocumentQueryConfiguration from a DocumentLogic and a Query
     *
     * @param DocumentLogic
     *            - a configured DocumentLogic
     * @param query
     *            - a configured Query object
     * @return - a DocumentQueryConfiguration
     */
    public static DocumentQueryConfiguration create(DocumentLogic DocumentLogic, Query query) {
        DocumentQueryConfiguration config = create(DocumentLogic);
        config.setQuery(query);
        return config;
    }
    
    // new additions
    
    public String getQueryIteratorClass() {
        return queryIteratorClazz;
    }
    
    public void setQueryIteratorClass(final String queryIteratorClazz) {
        this.queryIteratorClazz = queryIteratorClazz;
    }
    
    public boolean getForceAllTypes() {
        return forceAllTypes;
    }
    
    public void setForceAllTypes(final boolean forceAllTypes) {
        this.forceAllTypes = forceAllTypes;
    }
    
    public String getAllowedTypes() {
        return allowedTypes;
    }
    
    public void setAllowedTypes(final String allowedTypes) {
        this.allowedTypes = allowedTypes;
    }
    
    public boolean getTypeString() {
        return this.typeString;
    }
    
    public void setTypeString(final Boolean setType) {
        this.typeString = setType;
    }
    
    public boolean getCustomBatchScanner() {
        return this.customBatchScanner;
    }
    
    public void setCustomBatchScanner(final Boolean customBatchScanner) {
        this.customBatchScanner = customBatchScanner;
    }
    
    public boolean getDocRawFields() {
        return this.docRawFields;
    }
    
    public void setDocRawFields(final Boolean docRawFields) {
        this.docRawFields = docRawFields;
    }
    
    public int getQueueCapacity() {
        return queueCapacity;
    }
    
    public void setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
    }
    
    public int getMaxTabletsPerRequest() {
        return maxTabletsPerRequest;
    }
    
    public void setMaxTabletsPerRequest(int maxTabletsPerRequest) {
        this.maxTabletsPerRequest = maxTabletsPerRequest;
    }
    
    public int getMaxTabletThreshold() {
        return this.maxTabletThreshold;
    }
    
    public void setMaxTabletThreshold(int maxTabletThreshold) {
        this.maxTabletThreshold = maxTabletThreshold;
    }
    
    public boolean getPushdownLogic() {
        return pushdownLogic;
    }
    
    public void setPushdownLogic(boolean pushdownLogic) {
        this.pushdownLogic = pushdownLogic;
    }
    
    public boolean getConvertToDocument() {
        return this.convertToDocument;
    }
    
    public void setConvertToDocument(boolean document) {
        this.convertToDocument = document;
    }
}
