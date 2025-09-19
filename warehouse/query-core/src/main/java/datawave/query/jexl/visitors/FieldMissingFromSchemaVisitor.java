package datawave.query.jexl.visitors;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.accumulo.core.client.TableNotFoundException;
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
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.log4j.Logger;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.JexlFunctionArgumentDescriptorFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.model.QueryModel;
import datawave.query.util.MetadataHelper;

/**
 * Class to check that each query node contains a field which exists in the schema.
 *
 * <pre>
 * 1. If a datatype filter was specified, then the existence check is limited to only those datatypes
 * 2. If a datatype filter is NOT specified (null or empty), this implies ALL datatypes.
 * </pre>
 */
public class FieldMissingFromSchemaVisitor extends ShortCircuitBaseVisitor {

    private static final Logger log = Logger.getLogger(FieldMissingFromSchemaVisitor.class);

    private final MetadataHelper helper;
    private final Set<String> allFieldsForDatatypes; // All fields for the specified datatypes pulled from MetadataHelper
    private final Set<String> specialFields;
    private final Set<String> datatypeFilter;
    private final GenericQueryConfiguration queryConfiguration;

    public FieldMissingFromSchemaVisitor(MetadataHelper helper, Set<String> datatypeFilter, Set<String> specialFields,
                    GenericQueryConfiguration queryConfiguration) {
        this.helper = helper;
        this.specialFields = specialFields;
        try {
            // if given datatypeFilter is empty or null, assume that means ALL datatypes
            if (datatypeFilter == null) {
                datatypeFilter = Collections.emptySet();
            }
            this.allFieldsForDatatypes = this.helper.getAllFields(datatypeFilter);
        } catch (TableNotFoundException e) {
            log.error(e);
            throw new RuntimeException("Unable to get metadata", e);
        }
        this.datatypeFilter = datatypeFilter;
        if (queryConfiguration == null) {
            queryConfiguration = new GenericQueryConfiguration();
        }
        this.queryConfiguration = queryConfiguration;
    }

    @SuppressWarnings("unchecked")
    public static Set<String> getNonExistentFields(MetadataHelper helper, ASTJexlScript script, Set<String> datatypes, Set<String> specialFields,
                    GenericQueryConfiguration queryConfiguration) {
        FieldMissingFromSchemaVisitor visitor = new FieldMissingFromSchemaVisitor(helper, datatypes, specialFields, queryConfiguration);
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
    protected Object genericVisit(JexlNode node, Object data) throws TableNotFoundException {
        @SuppressWarnings("unchecked")
        Set<String> nonExistentFieldNames = (null == data) ? new HashSet<>() : (Set<String>) data;
        List<ASTIdentifier> identifiers;
        Collection<String> modelFields = List.of();

        // A node could be literal == literal in terms of an identityQuery
        try {
            identifiers = JexlASTHelper.getIdentifiers(node);
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
            if (this.queryConfiguration instanceof ShardQueryConfiguration) {
                String modelName = ((ShardQueryConfiguration) this.queryConfiguration).getModelName();
                String modelTableName = ((ShardQueryConfiguration) this.queryConfiguration).getModelTableName();
                QueryModel queryModel = helper.getQueryModel(modelTableName, modelName);
                modelFields = queryModel.getMappingsForAlias(fieldName);
            }
            if (!this.allFieldsForDatatypes.contains(fieldName) && !specialFields.contains(fieldName)
                            || (helper.isHidden(fieldName, this.datatypeFilter) && !modelFields.isEmpty())) {
                nonExistentFieldNames.add(fieldName);
            }
        }
        return nonExistentFieldNames;
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        try {
            return genericVisit(node, data);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        try {
            return genericVisit(node, data);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        try {
            return genericVisit(node, data);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        try {
            return genericVisit(node, data);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        try {
            return genericVisit(node, data);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        try {
            return genericVisit(node, data);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        try {
            return genericVisit(node, data);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        try {
            return genericVisit(node, data);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        JexlArgumentDescriptor desc = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(node);
        @SuppressWarnings("unchecked")
        Set<String> nonExistentFieldNames = (null == data) ? new HashSet<>() : (Set<String>) data;
        Collection<String> modelFields = List.of();

        for (String fieldName : desc.fields(this.helper, this.datatypeFilter)) {
            // deconstruct the identifier
            final String testFieldName = JexlASTHelper.deconstructIdentifier(fieldName);
            // changed to allow _ANYFIELD_ in functions
            if (this.queryConfiguration instanceof ShardQueryConfiguration) {
                String modelName = ((ShardQueryConfiguration) this.queryConfiguration).getModelName();
                String modelTableName = ((ShardQueryConfiguration) this.queryConfiguration).getModelTableName();
                QueryModel queryModel;
                try {
                    queryModel = helper.getQueryModel(modelTableName, modelName);
                } catch (TableNotFoundException e) {
                    throw new RuntimeException(e);
                }
                modelFields = queryModel.getMappingsForAlias(fieldName);
            }
            try {
                if (!this.allFieldsForDatatypes.contains(testFieldName) && !specialFields.contains(fieldName)
                                || (helper.isHidden(fieldName, this.datatypeFilter) && !modelFields.isEmpty())) {
                    nonExistentFieldNames.add(testFieldName);
                }
            } catch (TableNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        return nonExistentFieldNames;
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

}
