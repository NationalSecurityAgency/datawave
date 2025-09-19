package datawave.query.jexl.visitors;

import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTAndNode;
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
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;

import datawave.microservice.query.Query;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.JexlFunctionArgumentDescriptorFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.util.MetadataHelper;

/**
 * Class to check that each query node contains a field which exists in the schema.
 *
 * <pre>
 * 1. If a datatype filter was specified, then the existence check is limited to only those datatypes
 * 2. If a datatype filter is NOT specified (null or empty), this implies ALL datatypes.
 * </pre>
 */
public class FieldMissingFromDateRangeVisitor extends ShortCircuitBaseVisitor {

    private final MetadataHelper helper;
    private final Set<String> datatypeFilter;
    private final Date queryBeginDate;
    private final Date queryEndDate;
    private final Set<String> specialFields;

    public FieldMissingFromDateRangeVisitor(MetadataHelper helper, Set<String> datatypeFilter, Set<String> specialFields, Query querySettings) {
        this.helper = helper;
        // need to add null checks
        this.queryBeginDate = querySettings.getBeginDate();
        this.queryEndDate = querySettings.getEndDate();
        this.specialFields = specialFields;
        // if given datatypeFilter is empty or null, assume that means ALL datatypes
        if (datatypeFilter == null) {
            datatypeFilter = Collections.emptySet();
        }
        this.datatypeFilter = datatypeFilter;
    }

    @SuppressWarnings("unchecked")
    public static Set<String> getNonIngestedFields(MetadataHelper helper, ASTJexlScript script, Set<String> datatypes, Set<String> specialFields,
                                                   Query querySettings) {
        FieldMissingFromDateRangeVisitor visitor = new FieldMissingFromDateRangeVisitor(helper, datatypes, specialFields, querySettings);
        // Maintain insertion order.
        return (Set<String>) script.jjtAccept(visitor, new LinkedHashSet<>());
    }

    /**
     * @param node
     *            Jexl node
     * @param data
     *            The set of names which we have determined do not exist
     * @return the updated set of names which do not exist
     */
    protected Object ingestDateOrNodeVisit(JexlNode node, Object data) {
        @SuppressWarnings("unchecked")
        Set<String> nonExistentFieldNames = (null == data) ? new HashSet<>() : (Set<String>) data;
        Set<String> fieldNamesToTestDateRange = new HashSet<>();
        Set<String> nonIngestedFieldNames = new HashSet<>();
        List<ASTIdentifier> identifiers;

        int numChildren = node.jjtGetNumChildren();

        for (int i = 0; i < numChildren; i++) {
            JexlNode child = node.jjtGetChild(i);

            // A node could be literal == literal in terms of an identityQuery
            try {
                identifiers = JexlASTHelper.getIdentifiers(child);
            } catch (NoSuchElementException e) {
                return nonExistentFieldNames;
            }

            if (identifiers.isEmpty()) {
                // Catch cases where we have two literals
                // essentially everything but identifier op literal
                return nonExistentFieldNames;
            }

            for (ASTIdentifier identifier : identifiers) {
                String fieldName = JexlASTHelper.deconstructIdentifier(identifier);
                fieldNamesToTestDateRange.add(fieldName);
            }
        }

        Map<String,Long> occurrences = helper.getCountsForFieldsInDateRange(fieldNamesToTestDateRange, this.datatypeFilter, this.queryBeginDate,
                this.queryEndDate);
        if (occurrences.values().stream().mapToLong(Long::longValue).sum() < 1) {
            nonIngestedFieldNames.addAll(fieldNamesToTestDateRange);
        }

        // If ALL fields in the OR are NOT ingested within date window, add them to nonExistentFieldNames
        if (nonIngestedFieldNames.size() == numChildren) {
            return nonExistentFieldNames.addAll(nonIngestedFieldNames);
        } else {
            return nonExistentFieldNames;
        }
    }

    /**
     * @param node
     *            Jexl node
     * @param data
     *            The set of names which we have determined do not exist
     * @return the updated set of names which do not exist
     */
    protected Object genericIngestDateVisit(JexlNode node, Object data) {
        @SuppressWarnings("unchecked")
        Set<String> nonIngestedFieldNames = (null == data) ? new HashSet<>() : (Set<String>) data;
        List<ASTIdentifier> identifiers;

        // A node could be literal == literal in terms of an identityQuery
        try {
            identifiers = JexlASTHelper.getIdentifiers(node);
        } catch (NoSuchElementException e) {
            return nonIngestedFieldNames;
        }

        if (identifiers.isEmpty()) {
            // Catch cases where we have two literals
            // essentially everything but identifier op literal
            return nonIngestedFieldNames;
        }

        for (ASTIdentifier identifier : identifiers) {
            String fieldName = JexlASTHelper.deconstructIdentifier(identifier);
            Long occurrences = helper.getCountsByFieldForDays(fieldName, this.queryBeginDate, this.queryEndDate, this.datatypeFilter);
            if (!specialFields.contains(fieldName) && occurrences < 1) {
                nonIngestedFieldNames.add(fieldName);
            }
        }
        return nonIngestedFieldNames;

    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        return genericIngestDateVisit(node, data);
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        return genericIngestDateVisit(node, data);
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        return genericIngestDateVisit(node, data);
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        return genericIngestDateVisit(node, data);
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        return genericIngestDateVisit(node, data);
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        return genericIngestDateVisit(node, data);
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        return genericIngestDateVisit(node, data);
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        return genericIngestDateVisit(node, data);
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        JexlArgumentDescriptor desc = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(node);
        @SuppressWarnings("unchecked")
        Set<String> nonIngestedFieldNames = (null == data) ? new HashSet<>() : (Set<String>) data;

        for (String fieldName : desc.fields(this.helper, this.datatypeFilter)) {
            // deconstruct the identifier
            final String testFieldName = JexlASTHelper.deconstructIdentifier(fieldName);
            Long occurrences = helper.getCountsByFieldForDays(fieldName, this.queryBeginDate, this.queryEndDate, this.datatypeFilter);
            // changed to allow _ANYFIELD_ in functions
            if (!specialFields.contains(fieldName) && occurrences < 1) {
                nonIngestedFieldNames.add(testFieldName);
            }
        }

        return nonIngestedFieldNames;
    }

    // Descend through these nodes
    @Override
    public Object visit(ASTJexlScript node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTNotNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTReference node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTOrNode node, Object data) {
        return ingestDateOrNodeVisit(node, data);
    }
}
