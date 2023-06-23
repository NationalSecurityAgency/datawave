package datawave.query.tables.edge;

import datawave.core.common.edgedictionary.EdgeDictionaryProvider;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.edge.model.EdgeModelFields;
import datawave.edge.model.EdgeModelFieldsFactory;
import datawave.query.QueryParameters;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.QueryModelVisitor;
import datawave.query.model.edge.EdgeQueryModel;
import datawave.query.tables.ShardQueryLogic;
import datawave.webservice.dictionary.edge.EdgeDictionaryBase;
import datawave.webservice.dictionary.edge.MetadataBase;
import datawave.webservice.query.Query;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.Set;

/**
 * This Logic highjacks the Query string, and transforms it into a ShardQueryLogic query The query string is of the form:
 *
 * SOURCE == xxx AND SINK == yyy AND TYPE == zzz AND RELATIONSHIP == www AND EDGE_ATTRIBUTE1 == vvv
 *
 */
public class DefaultEdgeEventQueryLogic extends ShardQueryLogic {

    private static final Logger log = Logger.getLogger(DefaultEdgeEventQueryLogic.class);

    private String edgeModelName = null;
    private EdgeQueryModel edgeQueryModel = null;

    protected EdgeDictionaryBase<?,? extends MetadataBase<?>> dict;

    protected EdgeDictionaryProvider edgeDictionaryProvider;

    protected EdgeModelFields edgeFields;

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
    protected EdgeDictionaryBase<?,? extends MetadataBase<?>> getEdgeDictionary(Query settings) {
        return edgeDictionaryProvider.getEdgeDictionary(settings, getMetadataTableName());
    }

    protected DefaultEventQueryBuilder getEventQueryBuilder() {
        return new DefaultEventQueryBuilder(dict, getEdgeFields());
    }

    @Override
    public GenericQueryConfiguration initialize(AccumuloClient client, Query settings, Set<Authorizations> auths) throws Exception {

        setEdgeDictionary(getEdgeDictionary(settings)); // TODO grab threads from somewhere

        // Load and apply the configured edge query model
        loadEdgeQueryModel(client, auths);

        String queryString = applyQueryModel(getJexlQueryString(settings));

        DefaultEventQueryBuilder eventQueryBuilder = getEventQueryBuilder();

        // punch in the new query String
        settings.setQuery(eventQueryBuilder.getEventQuery(queryString));

        // new query string will always be in the JEXL syntax
        settings.addParameter(QueryParameters.QUERY_SYNTAX, "JEXL");

        return super.initialize(client, settings, auths);
    }

    /**
     * Loads the query model specified by the current configuration, to be applied to the incoming query.
     *
     * @param auths
     *            set of auths
     * @param client
     *            the client
     */
    protected void loadEdgeQueryModel(AccumuloClient client, Set<Authorizations> auths) {
        String model = getEdgeModelName() == null ? "" : getEdgeModelName();
        String modelTable = getModelTableName() == null ? "" : getModelTableName();
        if (null == getEdgeQueryModel() && (!model.isEmpty() && !modelTable.isEmpty())) {
            try {
                setEdgeQueryModel(new EdgeQueryModel(getMetadataHelperFactory().createMetadataHelper(client, getConfig().getMetadataTableName(), auths)
                                .getQueryModel(getConfig().getModelTableName(), getConfig().getModelName()), getEdgeFields()));
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
     *            the query string
     * @return modified query string
     */
    protected String applyQueryModel(String queryString) {
        ASTJexlScript origScript = null;
        ASTJexlScript script = null;
        try {
            origScript = JexlASTHelper.parseAndFlattenJexlQuery(queryString);
            HashSet<String> allFields = new HashSet<>();
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

    public EdgeDictionaryProvider getEdgeDictionaryProvider() {
        return edgeDictionaryProvider;
    }

    public void setEdgeDictionaryProvider(EdgeDictionaryProvider edgeDictionaryProvider) {
        this.edgeDictionaryProvider = edgeDictionaryProvider;
    }

    public void setEdgeModelFieldsFactory(EdgeModelFieldsFactory edgeModelFieldsFactory) {
        this.edgeFields = edgeModelFieldsFactory.createFields();
    }

    public EdgeModelFields getEdgeFields() {
        return edgeFields;
    }

    public void setEdgeFields(EdgeModelFields edgeFields) {
        this.edgeFields = edgeFields;
    }
}
