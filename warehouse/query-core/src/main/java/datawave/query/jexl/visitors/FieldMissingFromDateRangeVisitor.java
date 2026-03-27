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
    public static Set<String> getNonIngestedFields(MetadataHelper helper, ASTJexlScript script, Set<String> datatypes, Set<String> specialFields,
                    Date beginDate, Date endDate) {
        if (datatypes == null) {
            datatypes = Collections.emptySet();
        }

        // Collect the fields
        FieldMissingFromDateRangeVisitor visitor = new FieldMissingFromDateRangeVisitor(helper, datatypes);
        List<Set<String>> fieldSets = (List<Set<String>>) script.jjtAccept(visitor, new ArrayList<>());

        if (fieldSets.isEmpty()) {
            return Set.of();
        }

        // @formatter:off
        Set<String> allFields = fieldSets.stream()
                        .flatMap(Set::stream)
                        .collect(Collectors.toSet());
        // @formatter:on

        // If only special field were found, do not return any fields.
        if (specialFields.containsAll(allFields)) {
            return Set.of();
        }

        // Fetch all fields not found in the target date range.
        Set<String> missingFields = visitor.helper.getMissingFieldsInDateRange(allFields, datatypes, DateHelper.format(beginDate), DateHelper.format(endDate),
                        specialFields);

        // @formatter:off
        return fieldSets.stream()
                        .filter((set) -> !set.isEmpty())
                        .filter((set) -> Sets.intersection(specialFields, set).isEmpty())
                        .filter(missingFields::containsAll)
                        .flatMap(Set::stream)
                        .collect(Collectors.toCollection(LinkedHashSet::new));
        // @formatter:on
    }

    // Collect all field names in given node.
    private Object collectFields(JexlNode node, Object data) throws TableNotFoundException {
        // If data is a set, the node is a descendant of an OR node.
        if (data instanceof Set) {
            // noinspection unchecked
            addFields(node, (Set<String>) data);
        } else {
            // Otherwise it is a top-level node. Create a new set and collect all fields into it.
            @SuppressWarnings("unchecked")
            List<Set<String>> fieldSets = (List<Set<String>>) data;
            Set<String> fields = new LinkedHashSet<>();
            collectFields(node, fields);
            fieldSets.add(fields);
        }
        return data;
    }

    // All fields from the given node to the given candidate node.
    private void addFields(JexlNode node, Set<String> data) {
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
            data.add(fieldName);
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
        // If data is a set, this function node is a descendant of an OR node.
        if (data instanceof Set) {
            // noinspection unchecked
            addFields(node, (Set<String>) data);
        } else {
            // Otherwise this is a top-level node. Create a new set of fields and collect all fields from the function node.
            @SuppressWarnings("unchecked")
            List<Set<String>> fieldSets = (List<Set<String>>) data;
            Set<String> fields = new LinkedHashSet<>();
            addFields(node, fields);
            fieldSets.add(fields);
        }
        return data;
    }

    // Add all fields found within the given function node to the given set.
    private void addFields(ASTFunctionNode node, Set<String> fields) {
        JexlArgumentDescriptor desc = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(node);
        for (String fieldName : desc.fields(this.helper, this.datatypeFilter)) {
            fieldName = JexlASTHelper.deconstructIdentifier(fieldName);
            fields.add(fieldName);
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
        // If data is a set, this OR node is nested within another OR node. Pass it along to its children.
        if (data instanceof Set) {
            node.childrenAccept(this, data);
        } else {
            // Otherwise this is a top-level OR node. Create a new set and collect all fields from the node's children.
            @SuppressWarnings("unchecked")
            List<Set<String>> fieldSets = (List<Set<String>>) data;
            Set<String> fields = new LinkedHashSet<>();
            node.childrenAccept(this, fields);
            if (!fields.isEmpty()) {
                fieldSets.add(fields);
            }
        }
        return data;
    }

}
