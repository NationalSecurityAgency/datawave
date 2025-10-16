package datawave.query.jexl.visitors;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

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

import datawave.microservice.query.Query;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.JexlFunctionArgumentDescriptorFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.util.MetadataHelper;

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
    private final Query querySettings;
    private final Set<String> specialFields;

    public FieldMissingFromDateRangeVisitor(MetadataHelper helper, Set<String> datatypeFilter, Set<String> specialFields, Query querySettings) {
        this.helper = helper;
        this.querySettings = querySettings;
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
     *            The set of names which we have determined have not been ingested during the date range.
     * @return the updated set of names which have not been ingested during the date range.
     */
    private Object findMissingFields(ASTOrNode node, Object data) throws TableNotFoundException {
        @SuppressWarnings("unchecked")
        Set<String> nonExistentFieldNames = (null == data) ? new LinkedHashSet<>() : (Set<String>) data;
        Set<String> fieldNamesToTestDateRange = new HashSet<>();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
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
        Set<String> missingFields = helper.getMissingFieldsInDateRange(fieldNamesToTestDateRange, datatypeFilter,
                        formatter.format(this.querySettings.getBeginDate()), formatter.format(this.querySettings.getEndDate()));
        if (!specialFields.containsAll(fieldNamesToTestDateRange) && missingFields.containsAll(fieldNamesToTestDateRange)) {
            return nonExistentFieldNames.addAll(missingFields);
        } else {
            return nonExistentFieldNames;
        }
    }

    /**
     * @param node
     *            Jexl node
     * @param data
     *            The set of names which we have determined have not been ingested during the date range.
     * @return the updated set of names which have not been ingested during the date range.
     */
    private Object findMissingFields(JexlNode node, Object data) throws TableNotFoundException {
        @SuppressWarnings("unchecked")
        Set<String> nonIngestedFieldNames = (null == data) ? new HashSet<>() : (Set<String>) data;
        List<ASTIdentifier> identifiers;
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");

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
            if (!specialFields.contains(fieldName)) {
                nonIngestedFieldNames.addAll(helper.getMissingFieldsInDateRange(Set.of(fieldName), datatypeFilter,
                                formatter.format(this.querySettings.getBeginDate()), formatter.format(this.querySettings.getEndDate())));
            }
        }
        return nonIngestedFieldNames;

    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        try {
            return findMissingFields(node, data);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        try {
            return findMissingFields(node, data);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        try {
            return findMissingFields(node, data);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        try {
            return findMissingFields(node, data);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        try {
            return findMissingFields(node, data);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        try {
            return findMissingFields(node, data);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        try {
            return findMissingFields(node, data);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        try {
            return findMissingFields(node, data);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        JexlArgumentDescriptor desc = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(node);
        @SuppressWarnings("unchecked")
        Set<String> nonIngestedFieldNames = (null == data) ? new HashSet<>() : (Set<String>) data;
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");

        for (String fieldName : desc.fields(this.helper, this.datatypeFilter)) {
            // deconstruct the identifier
            final String testFieldName = JexlASTHelper.deconstructIdentifier(fieldName);
            // changed to allow _ANYFIELD_ in functions
            if (!specialFields.contains(fieldName)) {
                try {
                    nonIngestedFieldNames.addAll(helper.getMissingFieldsInDateRange(Set.of(testFieldName), datatypeFilter,
                                    formatter.format(this.querySettings.getBeginDate()), formatter.format(this.querySettings.getEndDate())));
                } catch (TableNotFoundException e) {
                    throw new RuntimeException(e);
                }
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
    public Object visit(ASTOrNode node, Object data) {
        try {
            return findMissingFields(node, data);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
