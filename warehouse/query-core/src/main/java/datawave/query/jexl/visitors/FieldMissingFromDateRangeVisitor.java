package datawave.query.jexl.visitors;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.hadoop.util.Sets;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.JexlFunctionArgumentDescriptorFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.util.MetadataHelper;
import datawave.util.time.DateHelper;

/**
 * Class to check that each query node contains a field which exists in the schema for the given date range.
 *
 * <pre>
 * 1. If a datatype filter was specified, then the existence check is limited to only those datatypes
 * 2. If a datatype filter is NOT specified (null or empty), this implies ALL datatypes.
 * 3. If querySettings is NOT specified (null), it will not report any missing fields. This is due to no begin or end date being provided.
 * </pre>
 */
public class FieldMissingFromDateRangeVisitor extends ShortCircuitBaseVisitor {

    private final MetadataHelper helper;
    private final Set<String> datatypeFilter;

    public FieldMissingFromDateRangeVisitor(MetadataHelper helper, Set<String> datatypeFilter) {
        this.helper = helper;
        // if given datatypeFilter is empty or null, assume that means ALL datatypes
        if (datatypeFilter == null) {
            datatypeFilter = Collections.emptySet();
        }
        this.datatypeFilter = datatypeFilter;
    }

    @SuppressWarnings("unchecked")
    public static List<ImmaterialNode> getNonIngestedFields(MetadataHelper helper, ASTJexlScript script, Set<String> datatypes, Set<String> specialFields,
                    Date beginDate, Date endDate) {
        if (datatypes == null) {
            datatypes = Collections.emptySet();
        }

        // Collect the fields
        FieldMissingFromDateRangeVisitor visitor = new FieldMissingFromDateRangeVisitor(helper, datatypes);
        List<CandidateNode> nodeData = (List<CandidateNode>) script.jjtAccept(visitor, new ArrayList<>());

        if (nodeData.isEmpty()) {
            return Collections.emptyList();
        }

        Set<String> allFields = nodeData.stream().map(CandidateNode::getFields).flatMap(Set::stream).collect(Collectors.toSet());
        Set<String> missingFields = visitor.helper.getMissingFieldsInDateRange(allFields, datatypes, DateHelper.format(beginDate), DateHelper.format(endDate),
                        specialFields);

        return nodeData.stream().filter((node) -> !node.fields.isEmpty()).filter((node) -> node.containsNoneOf(specialFields))
                        .filter((node) -> node.allFieldsMissing(missingFields)).map(ImmaterialNode::new).collect(Collectors.toList());
    }

    /**
     * @param node
     *            Jexl node
     * @param data
     *            The set of names which we have determined have not been ingested during the date range.
     * @return the updated set of names which have not been ingested during the date range.
     */
    private Object collectFields(JexlNode node, Object data) throws TableNotFoundException {
        // If data is a CandidateNode, the node is a descendent of an OR node.
        if (data instanceof CandidateNode) {
            addFields(node, (CandidateNode) data);
        } else {
            // Otherwise it is a top-level node.
            @SuppressWarnings("unchecked")
            List<CandidateNode> list = (List<CandidateNode>) data;
            CandidateNode candidateNode = new CandidateNode(node);
            collectFields(node, candidateNode);
            list.add(candidateNode);
        }
        return data;
    }

    // All fields from the given node to the given candidate node.
    private void addFields(JexlNode node, CandidateNode data) {
        List<ASTIdentifier> identifiers;

        // A node could be literal == literal in terms of an identityQuery
        try {
            identifiers = JexlASTHelper.getIdentifiers(node);
        } catch (NoSuchElementException e) {
            return;
        }

        if (identifiers.isEmpty()) {
            // Catch cases where we have two literals
            // essentially everything but identifier op literal
            return;
        }

        for (ASTIdentifier identifier : identifiers) {
            String fieldName = JexlASTHelper.deconstructIdentifier(identifier);
            data.addField(fieldName);
        }
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        try {
            return collectFields(node, data);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        try {
            return collectFields(node, data);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        try {
            return collectFields(node, data);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        try {
            return collectFields(node, data);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        try {
            return collectFields(node, data);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        try {
            return collectFields(node, data);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        try {
            return collectFields(node, data);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        try {
            return collectFields(node, data);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        // If data is a CandidateNode, this function node is a descendant of an OR node.
        if (data instanceof CandidateNode) {
            CandidateNode candidateNode = (CandidateNode) data;
            addFields(node, candidateNode);
        } else {
            // Otherwise this is a top-level node. Create a new CandidateNode and collect all fields from the function node.
            @SuppressWarnings("unchecked")
            List<CandidateNode> list = (List<CandidateNode>) data;
            CandidateNode candidateNode = new CandidateNode(node);
            addFields(node, candidateNode);
            list.add(candidateNode);
        }
        return data;
    }

    // Add all fields found within the given function node to the given candidate node.
    private void addFields(ASTFunctionNode node, CandidateNode candidateNode) {
        JexlArgumentDescriptor desc = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(node);
        for (String fieldName : desc.fields(this.helper, this.datatypeFilter)) {
            fieldName = JexlASTHelper.deconstructIdentifier(fieldName);
            candidateNode.addField(fieldName);
        }
    }

    // Descend through these nodes
    @Override
    public Object visit(ASTJexlScript node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTOrNode node, Object data) {
        // If data is a CandidateNode, this OR node is nested within another OR node. Pass it along to its children.
        if (data instanceof CandidateNode) {
            node.childrenAccept(this, data);
        } else {
            // Otherwise this is a top-level OR node. Create a new CandidateNode and collect all fields from the node's children.
            @SuppressWarnings("unchecked")
            List<CandidateNode> list = (List<CandidateNode>) data;
            CandidateNode candidateNode = new CandidateNode(node);
            node.childrenAccept(this, candidateNode);
            if (!candidateNode.fields.isEmpty()) {
                list.add(candidateNode);
            }
        }
        return data;
    }

    /**
     * Represents a node that may be a candidate to be an immaterial node.
     */
    private static class CandidateNode {
        private final JexlNode node;
        private final Set<String> fields = new LinkedHashSet<>(); // Maintain insertion order.

        public CandidateNode(JexlNode node) {
            this.node = node;
        }

        public Set<String> getFields() {
            return fields;
        }

        public void addField(String field) {
            fields.add(field);
        }

        /**
         * Return whether this candidate node contains none of the given fields.
         *
         * @param specialFields
         *            the fields
         * @return true if this candidate node has none of the given fields, or false otherwise
         */
        public boolean containsNoneOf(Set<String> specialFields) {
            return Sets.intersection(specialFields, this.fields).isEmpty();
        }

        /**
         * Return whether this candidate node's fields consist only of missing fields.
         *
         * @param missingFields
         *            the missing fields
         * @return true if this candidate node contains only missing fields, or false otherwise
         */
        public boolean allFieldsMissing(Set<String> missingFields) {
            return missingFields.containsAll(this.fields);
        }
    }

    /**
     * Represents a node within a jexl query that is considered to be immaterial because it only contains fields that are missing within a query's date range.
     */
    public static class ImmaterialNode {

        private final JexlNode node;
        private final Set<String> fields;

        public ImmaterialNode(JexlNode node, Set<String> fields) {
            this.node = RebuildingVisitor.copy(node);
            this.fields = fields == null ? Set.of() : Collections.unmodifiableSet(fields); // Maintain insertion order.
        }

        private ImmaterialNode(CandidateNode data) {
            this(data.node, data.fields);
        }

        public JexlNode getNode() {
            return node;
        }

        public Set<String> getFields() {
            return fields;
        }
    }
}
