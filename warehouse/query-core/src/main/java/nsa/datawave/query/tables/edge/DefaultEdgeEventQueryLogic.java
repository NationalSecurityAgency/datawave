package nsa.datawave.query.tables.edge;

import nsa.datawave.query.QueryParameters;
import nsa.datawave.query.model.edge.EdgeQueryModel;
import nsa.datawave.query.rewrite.jexl.JexlASTHelper;
import nsa.datawave.query.rewrite.jexl.visitors.JexlStringBuildingVisitor;
import nsa.datawave.query.rewrite.jexl.visitors.QueryModelVisitor;
import nsa.datawave.query.rewrite.tables.RefactoredShardQueryLogic;
import nsa.datawave.webservice.edgedictionary.DatawaveEdgeDictionary;
import nsa.datawave.webservice.query.Query;
import nsa.datawave.webservice.query.configuration.GenericQueryConfiguration;
import nsa.datawave.webservice.results.edgedictionary.EdgeDictionaryBase;
import nsa.datawave.webservice.results.edgedictionary.MetadataBase;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.log4j.Logger;

import javax.inject.Inject;
import java.util.HashSet;
import java.util.Set;

/**
 * This Logic highjacks the Query string, and transforms it into a RefactoredShardQueryLogic query The query string is of the form:
 * 
 * SOURCE == xxx AND SINK == yyy AND TYPE == zzz AND RELATIONSHIP == www AND EDGE_ATTRIBUTE1 == vvv
 * 
 */
public class DefaultEdgeEventQueryLogic extends RefactoredShardQueryLogic {
    
    private static final Logger log = Logger.getLogger(DefaultEdgeEventQueryLogic.class);
    
    private String edgeModelName = null;
    private EdgeQueryModel edgeQueryModel = null;
    
    protected EdgeDictionaryBase<?,? extends MetadataBase<?>> dict;
    
    @Inject
    protected DatawaveEdgeDictionary datawaveEdgeDictionary;
    
    public DefaultEdgeEventQueryLogic() {}
    
    // Required for clone method. Clone is called by the Logic Factory. If you don't override
    // the method, the Factory will create an instance of the super class instead of
    // this logic.
    public DefaultEdgeEventQueryLogic(DefaultEdgeEventQueryLogic other) {
        super(other);
        this.dict = other.dict;
        this.edgeModelName = other.edgeModelName;
        this.edgeQueryModel = other.edgeQueryModel;
    }
    
    @Override
    public DefaultEdgeEventQueryLogic clone() {
        return new DefaultEdgeEventQueryLogic(this);
    }
    
    @SuppressWarnings("unchecked")
    protected EdgeDictionaryBase<?,? extends MetadataBase<?>> getEdgeDictionary(Connector connector, Set<Authorizations> auths, int numThreads)
                    throws Exception {
        
        return this.datawaveEdgeDictionary.getEdgeDictionary(this.getMetadataTableName(), connector, auths, numThreads);
    }
    
    protected DefaultEventQueryBuilder getEventQueryBuilder() {
        return new DefaultEventQueryBuilder(dict);
    }
    
    @Override
    public GenericQueryConfiguration initialize(Connector connection, Query settings, Set<Authorizations> auths) throws Exception {
        
        setEdgeDictionary(getEdgeDictionary(connection, auths, 8)); // TODO grab threads from somewhere
        
        // Load and apply the configured edge query model
        loadEdgeQueryModel();
        
        String queryString = applyQueryModel(getJexlQueryString(settings));
        
        DefaultEventQueryBuilder eventQueryBuilder = getEventQueryBuilder();
        
        // punch in the new query String
        settings.setQuery(eventQueryBuilder.getEventQuery(queryString));
        
        // new query string will always be in the JEXL syntax
        settings.addParameter(QueryParameters.QUERY_SYNTAX, "JEXL");
        
        GenericQueryConfiguration config = super.initialize(connection, settings, auths);
        
        return config;
    }
    
    /**
     * Loads the query model specified by the current configuration, to be applied to the incoming query.
     * 
     * @return
     */
    protected void loadEdgeQueryModel() {
        String model = getEdgeModelName() == null ? "" : getEdgeModelName();
        String modelTable = getModelTableName() == null ? "" : getModelTableName();
        if (null == getEdgeQueryModel() && (!model.isEmpty() && !modelTable.isEmpty())) {
            try {
                setEdgeQueryModel(new EdgeQueryModel(getMetadataHelperFactory().createMetadataHelper().getQueryModel(config.getModelTableName(),
                                config.getModelName())));
            } catch (Throwable t) {
                log.error("Unable to load edgeQueryModel from metadata table", t);
            }
        }
    }
    
    /**
     * Parses the Jexl Query string into an ASTJexlScript and then uses QueryModelVisitor to apply the loaded model to the query string, and then rewrites the
     * translated ASTJexlScript back to a query string using JexlStringBuildingVisitor.
     * 
     * @param queryString
     * @return modified query string
     */
    protected String applyQueryModel(String queryString) {
        ASTJexlScript origScript = null;
        ASTJexlScript script = null;
        try {
            origScript = JexlASTHelper.parseJexlQuery(queryString);
            HashSet<String> allFields = new HashSet<String>();
            allFields.addAll(getEdgeQueryModel().getAllInternalFieldNames());
            script = QueryModelVisitor.applyModel(origScript, getEdgeQueryModel(), allFields);
            return JexlStringBuildingVisitor.buildQuery(script);
            
        } catch (Throwable t) {
            throw new IllegalStateException("Edge query model could not be applied", t);
        }
    }
    
    public void setEdgeDictionary(EdgeDictionaryBase<?,? extends MetadataBase<?>> dict) {
        this.dict = dict;
    }
    
    public EdgeQueryModel getEdgeQueryModel() {
        return this.edgeQueryModel;
    }
    
    public void setEdgeQueryModel(EdgeQueryModel model) {
        this.edgeQueryModel = model;
    }
    
    public String getEdgeModelName() {
        return this.edgeModelName;
    }
    
    public void setEdgeModelName(String edgeModelName) {
        this.edgeModelName = edgeModelName;
    }
    
    protected String getEventQuery(Query settings) throws Exception {
        return getEventQueryBuilder().getEventQuery(getJexlQueryString(settings));
    }
    
}
