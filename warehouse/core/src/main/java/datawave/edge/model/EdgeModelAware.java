package datawave.edge.model;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

/**
 * Interface that allows internal field names used within the edge model to be configured and injected transparently into classes that need them.<br>
 * <br>
 * 
 * The intention is to enforce a unified approach to field name management and usage throughout the entire codebase, so that the actual field names in use can
 * be dictated by the deployment environment rather than the code itself.
 */
public interface EdgeModelAware {
    
    String EQUALS = "==";
    String EQUALS_REGEX = "=~";
    String NOT_EQUALS = "!=";
    String NOT_EQUALS_REGEX = "!~";
    String FUNCTION = "function";
    String OR = " || ";
    String AND = " && ";
    char STRING_QUOTE = '\'';
    char BACKSLASH = '\\';
    
    /* Base field names */
    
    /** Internal field name denoting edge vertex 1 */
    String EDGE_SOURCE = Fields.getInstance().getSourceFieldName();
    /** Internal field name denoting edge vertex 2 */
    String EDGE_SINK = Fields.getInstance().getSinkFieldName();
    /** Internal field name denoting the edge type defined by the two vertices */
    String EDGE_TYPE = Fields.getInstance().getTypeFieldName();
    /** Internal field name denoting the source-sink relationship */
    String EDGE_RELATIONSHIP = Fields.getInstance().getRelationshipFieldName();
    /** Internal field name denoting the edge's 1st attribute */
    String EDGE_ATTRIBUTE1 = Fields.getInstance().getAttribute1FieldName();
    /** Internal field name denoting the edge's 2nd attribute */
    String EDGE_ATTRIBUTE2 = Fields.getInstance().getAttribute2FieldName();
    /** Internal field name denoting the edge's 3rd attribute */
    String EDGE_ATTRIBUTE3 = Fields.getInstance().getAttribute3FieldName();
    /** Internal field name denoting a 'stats' edge */
    String STATS_EDGE = Fields.getInstance().getStatsEdgeFieldName();
    /** Internal field name denoting the edge date */
    String DATE = Fields.getInstance().getDateFieldName();
    
    /* These are specific to edge key processing (as previously managed with enum in EdgeKeyUtil) */
    
    /** Internal field name denoting the edge enrichment type */
    String ENRICHMENT_TYPE = Fields.getInstance().getEnrichmentTypeFieldName();
    /** Internal field name denoting the edge fact type */
    String FACT_TYPE = Fields.getInstance().getFactTypeFieldName();
    /** Internal field name denoting grouped edge fields */
    String GROUPED_FIELDS = Fields.getInstance().getGroupedFieldsFieldName();
    
    /* These are specific to query result transformation (previously hardcoded literals in EdgeQueryTransformer) */
    
    /** Internal field name used to convey an edge count */
    String COUNT = Fields.getInstance().getCountFieldName();
    /** Internal field name used to convey edge counts */
    String COUNTS = Fields.getInstance().getCountsFieldName();
    /** Internal field name denoting an edge load date */
    String LOAD_DATE = Fields.getInstance().getLoadDateFieldName();
    /** Internal field name denoting an edge id */
    String UUID = Fields.getInstance().getUuidFieldName();
    /** Internal field name denoting and edge activity date */
    String ACTIVITY_DATE = Fields.getInstance().getActivityDateFieldName();
    /** Internal field name denoting if the edge activity date was good or bad */
    String BAD_ACTIVITY_DATE = Fields.getInstance().getBadActivityDateFieldName();
    
    /**
     * On-demand singleton for loading the internal model.<br>
     * <br>
     * 
     * With the edge schema, since field names don't exist on disk in the way that they do for events, we have the flexiblity to alter the names within the
     * model to suit the query syntax preferred by the target deployment environment. This class uses Spring injection to load and map the internal model.<br>
     * <br>
     * 
     * NOTE:<br>
     * <br>
     * Since the EdgeModelAware interface is intended to provide a single point of access to field names for all application tiers, it is important to ensure
     * that the Spring config is available within a variety of distinct classloading contexts...ie, within webservers, tservers, etc. If the config fails to
     * load at any tier, then edge queries will fail. FATAL log entries are emitted when this case arises.
     */
    class Fields {
        /** required bean context */
        /** common default locations for locating bean xml */
        static final String[] EDGE_MODEL_CONTEXT = {"classpath*:EdgeModelContext.xml"};
        /** required bean name */
        static final String BASE_MODEL_BEAN = "baseFieldMap";
        /** required bean name */
        static final String KEYUTIL_MODEL_BEAN = "keyUtilFieldMap";
        /** required bean name */
        static final String TRANSFORM_MODEL_BEAN = "transformFieldMap";
        
        /** internal fields common to all application tiers */
        private Map<String,String> baseFieldMap;
        /** internal fields used in key processing (as previously defined by enum within EdgeKeyUtil.java) */
        private Map<String,String> keyUtilFieldMap;
        /** internal fields used in query result transformation (eg, EdgeQueryTransformer.java) */
        private Map<String,String> transformFieldMap;
        
        private Logger log = Logger.getLogger(Fields.class);
        
        private Fields() {
            loadContext();
        }
        
        @SuppressWarnings("unchecked")
        private void loadMaps(ApplicationContext context) {
            baseFieldMap = (Map<String,String>) context.getBean(BASE_MODEL_BEAN);
            keyUtilFieldMap = (Map<String,String>) context.getBean(KEYUTIL_MODEL_BEAN);
            transformFieldMap = (Map<String,String>) context.getBean(TRANSFORM_MODEL_BEAN);
        }
        
        /**
         * Initializes the internal field maps using the EdgeModelContext.xml config
         */
        private void loadContext() {
            
            String contextOverride = System.getProperty("edge.model.context.path");
            if (null != contextOverride) {
                FileSystemXmlApplicationContext context = new FileSystemXmlApplicationContext(contextOverride);
                loadMaps(context);
                context.close();
            } else {
                ClassLoader thisClassLoader = EdgeModelAware.class.getClassLoader();
                ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext();
                try {
                    context.setClassLoader(thisClassLoader);
                    context.setConfigLocations(EDGE_MODEL_CONTEXT);
                    context.refresh();
                    loadMaps(context);
                } catch (Throwable t) {
                    log.fatal("Edge model configuration not loaded!! Edge queries will fail until this issue is corrected.");
                    log.fatal(String.format("Ensure that the Spring config file '%s' is on the classpath and contains bean names '%s', '%s', and '%s'",
                                    EDGE_MODEL_CONTEXT, BASE_MODEL_BEAN, KEYUTIL_MODEL_BEAN, TRANSFORM_MODEL_BEAN), t);
                } finally {
                    context.close();
                }
            }
        }
        
        /**
         * Enum that can be used for convenience to lookup the internal edge field name values.
         */
        public enum FieldKey {
            /** Key to the internal field name used to denote edge vertex 1 */
            EDGE_SOURCE,
            /** Key to the internal field name used to denote edge vertex 2 */
            EDGE_SINK,
            /** Key to the internal field name used to denote the edge type defined by the two vertices */
            EDGE_TYPE,
            /** Key to the internal field name used to denote the source-sink relationship */
            EDGE_RELATIONSHIP,
            /** Key to the internal field name used to denote the edge's 1st attribute */
            EDGE_ATTRIBUTE1,
            /** Key to the internal field name used to denote the edge's 2nd attribute */
            EDGE_ATTRIBUTE2,
            /** Key to the internal field name used to denote the edge's 3rd attribute */
            EDGE_ATTRIBUTE3,
            /** Key to the internal field name used to denote a stats edge */
            STATS_EDGE,
            /** Key to the internal field name used to denote the edge's date */
            DATE,
            /** Key to the internal field name used to denote the edge enrichment type */
            ENRICHMENT_TYPE,
            /** Key to the internal field name used to denote the edge fact type */
            FACT_TYPE,
            /** Key to the internal field name used to denote the edge grouped fields */
            GROUPED_FIELDS,
            /** Key to the internal field name used to convey an edge count */
            COUNT,
            /** Key to the internal field name used to convey edge counts */
            COUNTS,
            /** Key to the internal field name used to denote an edge load date */
            LOAD_DATE,
            /** Key to the internal field name used to denote and edge id */
            UUID,
            /** Key to the internal field name used to denote the edge activity date */
            ACTIVITY_DATE,
            /** Key to the internal field name used to denote weather the edge activity date was good or bad */
            BAD_ACTIVITY_DATE;
            
            private static Map<String,FieldKey> reverseMap = new HashMap<>();
            
            static {
                Map<String,String> fieldMap = getInstance().getAllFieldsMap();
                for (Map.Entry<String,String> entry : fieldMap.entrySet()) {
                    reverseMap.put(entry.getValue(), FieldKey.valueOf(entry.getKey()));
                }
            }
            
            /**
             * Returns the FieldKey associated with the actual (ie, configured) edge field name, ie, reverse lookup...
             * 
             * @param internalFieldName
             *            configured field name to parse
             * @return the reverse lookup field name
             */
            public static FieldKey parse(String internalFieldName) {
                return reverseMap.get(internalFieldName);
            }
        }
        
        public static Fields getInstance() {
            return OnDemand.INSTANCE;
        }
        
        private static class OnDemand {
            private static final Fields INSTANCE = new Fields();
        }
        
        public String getSourceFieldName() {
            return baseFieldMap.get(FieldKey.EDGE_SOURCE.name());
        }
        
        public String getSinkFieldName() {
            return baseFieldMap.get(FieldKey.EDGE_SINK.name());
        }
        
        public String getTypeFieldName() {
            return baseFieldMap.get(FieldKey.EDGE_TYPE.name());
        }
        
        public String getRelationshipFieldName() {
            return baseFieldMap.get(FieldKey.EDGE_RELATIONSHIP.name());
        }
        
        public String getAttribute1FieldName() {
            return baseFieldMap.get(FieldKey.EDGE_ATTRIBUTE1.name());
        }
        
        public String getAttribute2FieldName() {
            return baseFieldMap.get(FieldKey.EDGE_ATTRIBUTE2.name());
        }
        
        public String getAttribute3FieldName() {
            return baseFieldMap.get(FieldKey.EDGE_ATTRIBUTE3.name());
        }
        
        public String getDateFieldName() {
            return baseFieldMap.get(FieldKey.DATE.name());
        }
        
        public String getStatsEdgeFieldName() {
            return baseFieldMap.get(FieldKey.STATS_EDGE.name());
        }
        
        public String getGroupedFieldsFieldName() {
            return keyUtilFieldMap.get(FieldKey.GROUPED_FIELDS.name());
        }
        
        public String getEnrichmentTypeFieldName() {
            return keyUtilFieldMap.get(FieldKey.ENRICHMENT_TYPE.name());
        }
        
        public String getFactTypeFieldName() {
            return keyUtilFieldMap.get(FieldKey.FACT_TYPE.name());
        }
        
        public String getCountFieldName() {
            return transformFieldMap.get(FieldKey.COUNT.name());
        }
        
        public String getCountsFieldName() {
            return transformFieldMap.get(FieldKey.COUNTS.name());
        }
        
        public String getLoadDateFieldName() {
            return transformFieldMap.get(FieldKey.LOAD_DATE.name());
        }
        
        public String getUuidFieldName() {
            return transformFieldMap.get(FieldKey.UUID.name());
        }
        
        public String getActivityDateFieldName() {
            return transformFieldMap.get(FieldKey.ACTIVITY_DATE.name());
        }
        
        public String getBadActivityDateFieldName() {
            return transformFieldMap.get(FieldKey.BAD_ACTIVITY_DATE.name());
        }
        
        /**
         * Returns the subset of all edge-related field names which are common to all application tiers.
         * 
         * @return subset of field names
         */
        public Collection<String> getBaseFieldNames() {
            return Collections.unmodifiableCollection(baseFieldMap.values());
        }
        
        /**
         * Returns the field names associated with key manipulation and processing, a superset of the fields given by the {@link Fields#getBaseFieldNames}
         * method
         * 
         * @return field names associated with key manipulation and processing
         */
        public Collection<String> getKeyProcessingFieldNames() {
            HashSet<String> fields = new HashSet<>();
            fields.addAll(baseFieldMap.values());
            fields.addAll(keyUtilFieldMap.values());
            return Collections.unmodifiableCollection(fields);
        }
        
        /**
         * Returns the field names associated with query result transformation, a superset of the fields given by the {@link Fields#getBaseFieldNames} method
         * 
         * @return query result field names
         */
        public Collection<String> getTransformFieldNames() {
            HashSet<String> fields = new HashSet<>();
            fields.addAll(baseFieldMap.values());
            fields.addAll(transformFieldMap.values());
            return Collections.unmodifiableCollection(fields);
        }
        
        /**
         * Returns the mapped fields associated with edge key manipulation and processing, where the keys are represented as FieldKey.name()
         * 
         * @return mapped key processing fields
         */
        public Map<String,String> getKeyProcessingFieldMap() {
            HashMap<String,String> all = new HashMap<>();
            all.putAll(baseFieldMap);
            all.putAll(keyUtilFieldMap);
            return Collections.unmodifiableMap(all);
        }
        
        /**
         * Returns the mapping for fields associated with query result transformation, where the keys are represented as FieldKey.name()
         * 
         * @return mapping for fields associated with query result transformation
         */
        public Map<String,String> getTransformFieldMap() {
            HashMap<String,String> all = new HashMap<>();
            all.putAll(baseFieldMap);
            all.putAll(transformFieldMap);
            return Collections.unmodifiableMap(all);
        }
        
        /**
         * Returns all mapped field names, where the keys are represented as FieldKey.name()
         * 
         * @return map of field names
         */
        public Map<String,String> getAllFieldsMap() {
            HashMap<String,String> all = new HashMap<>();
            all.putAll(baseFieldMap);
            all.putAll(keyUtilFieldMap);
            all.putAll(transformFieldMap);
            return Collections.unmodifiableMap(all);
        }
        
        /**
         * Returns the field name mapped to the specified FieldKey
         * 
         * @param key
         *            key to pull name from
         * @return field name
         */
        public String getFieldName(FieldKey key) {
            return getAllFieldsMap().get(key.name());
        }
    }
}
