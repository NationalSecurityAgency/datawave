package datawave.query.tables.edge;

import datawave.edge.model.EdgeModelAware;
import datawave.edge.model.EdgeModelAware.Fields.FieldKey;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.TreeFlatteningRebuildingVisitor;
import datawave.webservice.dictionary.edge.EdgeDictionaryBase;
import datawave.webservice.dictionary.edge.EventField;
import datawave.webservice.dictionary.edge.MetadataBase;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class DefaultEventQueryBuilder {
    private static final Logger log = LoggerFactory.getLogger(DefaultEventQueryBuilder.class);

    protected static final String DEQ = " == ";
    protected static final String DAND = " AND ";
    protected static final String DOR = " OR ";
    protected static final String TICK = "'";
    protected static final String OPEN = "(";
    protected static final String CLOSE = ")";

    private String source;
    private String sink;
    private String edgeType;
    private String relationship;
    private String attribute1;
    protected Set<String> attribute2;
    protected Set<String> attribute3;

    protected EdgeDictionaryBase<?,? extends MetadataBase<?>> dict;

    public DefaultEventQueryBuilder(EdgeDictionaryBase<?,? extends MetadataBase<?>> dict) {
        this.dict = dict;
    }

    public String getEventQuery(String jexlQueryString) throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(jexlQueryString);
        JexlNode node = (JexlNode) script.jjtAccept(new TreeFlatteningRebuildingVisitor(true), script);

        if (JexlNodes.id(node) == ParserTreeConstants.JJTJEXLSCRIPT) {
            if (node.jjtGetNumChildren() != 1) {
                throw new IllegalArgumentException("Parsing failed for " + jexlQueryString);
            }
            node = node.jjtGetChild(0);
        }

        log.debug("Beginning transform of query: {}", jexlQueryString);

        // Traverse the tree, create new query string
        Set<String> edgeQueries = new HashSet<>();
        switch (JexlNodes.id(node)) {
            // not acceptable ... must have some AND operations to
            case ParserTreeConstants.JJTEQNODE:
                throw new IllegalArgumentException("Query not specific enough to specify an edge.");
            case ParserTreeConstants.JJTORNODE:
                for (int ii = 0; ii < node.jjtGetNumChildren(); ii++) {
                    edgeQueries.add(createEventQueryString(node.jjtGetChild(ii)));
                }
                break;
            case ParserTreeConstants.JJTANDNODE:
                edgeQueries.add(createEventQueryString(node));
                break;
        }

        if (log.isDebugEnabled()) {
            log.debug("Generated " + edgeQueries.size() + " queries from query string");
        }

        String finalQuery = DefaultEventQueryBuilder.combineQueriesAsUnion(edgeQueries);

        if (log.isDebugEnabled()) {
            log.debug("Final Event Query String: " + finalQuery);
        }

        return finalQuery;
    }

    // to parse SOURCE == xxx AND SINK == yy AND .... etc.
    // top level needs to be an AND node.
    protected String createEventQueryString(JexlNode node) {

        switch (JexlNodes.id(node)) {
            case ParserTreeConstants.JJTANDNODE:
                // AND should have EQ children.
                for (int ii = 0, doneII = node.jjtGetNumChildren(); ii < doneII; ii++) {
                    JexlNode andChild = node.jjtGetChild(ii);

                    if (JexlNodes.id(andChild) == ParserTreeConstants.JJTORNODE) {
                        // Here certain fields are allowed to be or'ed togther provided that they are all the same field
                        String fieldname = null;

                        for (int jj = 0; jj < andChild.jjtGetNumChildren(); jj++) {
                            JexlNode orChild = andChild.jjtGetChild(jj);
                            // this better be an eqNode or else
                            if (JexlNodes.id(orChild) != ParserTreeConstants.JJTEQNODE) {
                                throw new IllegalArgumentException("Invalid query string");
                            }

                            JexlASTHelper.IdentifierOpLiteral idOpLit = JexlASTHelper.getIdentifierOpLiteral(orChild);

                            // Make sure all field names are the same within this Or nodes children
                            if (fieldname == null) {
                                fieldname = idOpLit.deconstructIdentifier();
                            } else if (!fieldname.equals(idOpLit.deconstructIdentifier())) {
                                throw new IllegalArgumentException("Invalid query can't or two different field names");
                            }

                            if (!this.orAbleField(fieldname)) {
                                throw new IllegalArgumentException("Invalid query OR's are not supported for field name: " + fieldname);
                            }
                            this.parseAndAdd(fieldname, (String) idOpLit.getLiteralValue());
                        }
                    } else {
                        // this better be an eqNode or else
                        if (JexlNodes.id(andChild) != ParserTreeConstants.JJTEQNODE) {
                            throw new IllegalArgumentException("Invalid query string");
                        }
                        JexlASTHelper.IdentifierOpLiteral idOpLit = JexlASTHelper.getIdentifierOpLiteral(andChild);
                        this.parseAndAdd(idOpLit.deconstructIdentifier(), (String) idOpLit.getLiteralValue());
                    }
                }
                break;

            default:
                throw new IllegalArgumentException("Invalid query string. Make sure you use parenthasis ");
        }

        String newQuery = this.build();
        if (log.isDebugEnabled())
            log.debug("Transformed query to: " + newQuery);

        return newQuery;
    }

    public String build() {
        checkMandatoryFieldsSet();
        StringBuilder sb = new StringBuilder();
        boolean foundDictionaryMatch = false;
        for (MetadataBase<? extends MetadataBase<?>> meta : dict.getMetadataList()) {
            // only process metadata for this particular edge type/relationship/data source
            if (matchesEdgeTypeAndRelationship(meta)) {
                // data source is optional, but if it is included it must match
                if (doesntMatchAttribute1SourceIfSpecified(meta)) {
                    continue;
                }

                if (doesntMatchAllowedAttribute1(meta)) {
                    continue;
                }

                // the dictionary doesn't hold all fields, so this will other fields to be added to each statement (if user provided them)
                String conditionsForUncachedFields = generateConditionsForUncachedFields();

                for (EventField field : meta.getEventFields()) {

                    sb.append(OPEN);
                    sb.append(cleanseFieldName(field.getSourceField()));
                    sb.append(DEQ);
                    sb.append(TICK).append(this.getSource()).append(TICK);
                    sb.append(DAND);
                    sb.append(cleanseFieldName(field.getSinkField()));
                    sb.append(DEQ);
                    sb.append(TICK).append(this.getSink()).append(TICK);
                    if (field.hasEnrichment()) {
                        sb.append(DAND);
                        sb.append(cleanseFieldName(field.getEnrichmentField()));
                        sb.append(DEQ);
                        sb.append(TICK).append(field.getEnrichmentIndex()).append(TICK);
                    }

                    // Check for JexlPreconditions here and add to the query as necessary...
                    if (field.hasJexlPrecondition()) {

                        // Because of complex preconditions surround the precondition
                        // with parens.
                        sb.append(DAND);
                        sb.append(OPEN);
                        sb.append(field.getJexlPrecondition());
                        sb.append(CLOSE);
                    }
                    sb.append(conditionsForUncachedFields);
                    sb.append(CLOSE);
                    sb.append(DOR);
                    foundDictionaryMatch = true;
                }
            }
        }
        clearFields();
        if (foundDictionaryMatch) {
            return sb.toString().substring(0, sb.length() - DOR.length());
        } else {
            throw new IllegalArgumentException("Edge Parameters Do Not Exist in Edge Dictionary.");
        }

    }

    protected void clearFields() {
        source = null;
        sink = null;
        edgeType = null;
        relationship = null;
        attribute1 = null;

        if (attribute2 != null) {
            attribute2.clear();
        }
        if (attribute3 != null) {
            attribute3.clear();
        }

    }

    protected String generateConditionsForUncachedFields() {
        return "";
    }

    protected boolean matchesEdgeTypeAndRelationship(MetadataBase<?> meta) {
        return meta.getEdgeType().equals(this.getEdgeType()) && meta.getEdgeRelationship().equals(this.getRelationship());
    }

    protected boolean doesntMatchAttribute1SourceIfSpecified(MetadataBase<?> meta) {
        return (meta.hasEdgeAttribute1Source() && this.hasEdgeAttribute1()) && !meta.getEdgeAttribute1Source().equals(this.getEdgeAttribute1());
    }

    // Use the helper to handle the $ and grouping of field names
    protected String cleanseFieldName(String fieldName) {
        fieldName = JexlASTHelper.deconstructIdentifier(fieldName, false);
        fieldName = JexlASTHelper.rebuildIdentifier(fieldName, false);
        return fieldName;
    }

    protected DefaultEventQueryBuilder parseAndAdd(String fieldName, String fieldValue) {
        FieldKey fieldKey = FieldKey.parse(fieldName);
        if (null != fieldKey) {
            switch (fieldKey) {
                case EDGE_SOURCE:
                    this.setSource(fieldValue);
                    return this;
                case EDGE_SINK:
                    this.setSink(fieldValue);
                    return this;
                case EDGE_TYPE:
                    this.setEdgeType(fieldValue);
                    return this;
                case EDGE_RELATIONSHIP:
                    this.setRelationship(fieldValue);
                    return this;
                case EDGE_ATTRIBUTE1:
                    this.setEdgeAttribute1(fieldValue);
                    return this;
                case EDGE_ATTRIBUTE2:
                    this.setEdgeAttribute2(fieldValue);
                    return this;
                case EDGE_ATTRIBUTE3:
                    this.setEdgeAttribute3(fieldValue);
                    return this;
            }
        }
        throw new IllegalArgumentException("Unknown Query field name: " + fieldName);
    }

    /*
     * Used to check which field names in the query string can be OR'ed together The basic rule is that field names that will be used when searching for the
     * event, fields used when searching for the definition should not be or'ed together
     */
    public boolean orAbleField(String fieldName) {
        FieldKey fieldKey = FieldKey.parse(fieldName);
        if (null != fieldKey) {

            // attributes 2 and 3 on the edge should be field names in the event
            switch (fieldKey) {
                case EDGE_ATTRIBUTE2:
                    return true;
                case EDGE_ATTRIBUTE3:
                    return true;
                default:
                    return false;
            }
        }
        return false;
    }

    protected boolean doesntMatchAllowedAttribute1(MetadataBase<?> meta) {
        return false;
    }

    static String combineQueriesAsUnion(Set<String> edgeQueries) {
        StringBuilder finalBuilder = new StringBuilder();
        for (String query : edgeQueries) {
            finalBuilder.append(query).append(DOR);
        }
        String finalQuery = finalBuilder.toString();
        finalQuery = finalQuery.substring(0, Math.max(0, finalQuery.length() - DOR.length()));
        return finalQuery;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getSink() {
        return sink;
    }

    public void setSink(String sink) {
        this.sink = sink;
    }

    protected void checkMandatoryFieldsSet() {
        String helpfullMsg = "If you believe this field has been set ensure proper placement of parenthesis to make sure the query is being evaluated in the order you would expect.";
        if (StringUtils.isBlank(this.sink)) {
            throw new IllegalArgumentException("Mandatory Field not set: " + EdgeModelAware.Fields.getInstance().getSinkFieldName() + ". " + helpfullMsg);
        } else if (StringUtils.isBlank(this.source)) {
            throw new IllegalArgumentException("Mandatory Field not set: " + EdgeModelAware.Fields.getInstance().getSourceFieldName() + ". " + helpfullMsg);
        } else if (StringUtils.isBlank(this.edgeType)) {
            throw new IllegalArgumentException("Mandatory Field not set: " + EdgeModelAware.Fields.getInstance().getTypeFieldName() + ". " + helpfullMsg);
        } else if (StringUtils.isBlank(this.relationship)) {
            throw new IllegalArgumentException(
                            "Mandatory Field not set: " + EdgeModelAware.Fields.getInstance().getRelationshipFieldName() + ". " + helpfullMsg);
        }
    }

    public String getEdgeType() {
        return edgeType;
    }

    public void setEdgeType(String edgeType) {
        this.edgeType = edgeType;
    }

    public String getRelationship() {
        return relationship;
    }

    public void setRelationship(String edgeRelationship) {
        this.relationship = edgeRelationship;
    }

    public String getEdgeAttribute1() {
        return attribute1;
    }

    public boolean hasEdgeAttribute1() {
        if (StringUtils.isBlank(this.attribute1))
            return false;
        return true;
    }

    public void setEdgeAttribute1(String attribute1) {
        this.attribute1 = attribute1;
    }

    public Set<String> getEdgeAttribute2() {
        return attribute2;
    }

    public void setEdgeAttribute2(String attribute2) {
        if (this.attribute2 == null) {
            this.attribute2 = new HashSet<>();
        }

        this.attribute2.add(attribute2);
    }

    public Set<String> getEdgeAttribute3() {
        return attribute3;
    }

    public void setEdgeAttribute3(String attribute3) {
        if (this.attribute3 == null) {
            this.attribute3 = new HashSet<>();
        }
        this.attribute3.add(attribute3);
    }
}
