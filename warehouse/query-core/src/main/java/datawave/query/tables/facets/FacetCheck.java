package datawave.query.tables.facets;

import java.text.MessageFormat;
import java.util.NoSuchElementException;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.PrintingVisitor;
import datawave.query.Constants;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.InvalidFieldIndexQueryFatalQueryException;
import datawave.query.jexl.visitors.AllTermsIndexedVisitor;
import datawave.query.util.MetadataHelper;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.PreConditionFailedQueryException;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl2.parser.JexlNode;

import com.google.common.collect.Multimap;

public class FacetCheck extends AllTermsIndexedVisitor {
    
    Multimap<String,String> facetMultimap;
    
    public FacetCheck(ShardQueryConfiguration config, MetadataHelper helper) {
        super(config, helper);
        try {
            facetMultimap = helper.getFacets("FacetsNatingMetadata");
        } catch (InstantiationException | IllegalAccessException | TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Determine, for a binary equality node, if the field name is indexed
     * 
     * @param node
     * @param data
     * @return
     */
    @Override
    protected JexlNode equalityVisitor(JexlNode node, Object data) {
        String fieldName;
        try {
            fieldName = JexlASTHelper.getIdentifier(node);
        } catch (NoSuchElementException e) {
            // We only have literals
            PreConditionFailedQueryException qe = new PreConditionFailedQueryException(DatawaveErrorCode.EQUALS_NODE_TWO_LITERALS, e, MessageFormat.format(
                            "Node: {0}", PrintingVisitor.formattedQueryString(node).replace('\n', ' ')));
            throw new InvalidFieldIndexQueryFatalQueryException(qe);
        }
        
        if (Constants.ANY_FIELD.equals(fieldName) || Constants.NO_FIELD.equals(fieldName)) {
            return null;
        }
        
        if (!facetMultimap.containsKey(fieldName)) {
            PreConditionFailedQueryException qe = new PreConditionFailedQueryException(DatawaveErrorCode.FIELD_NOT_INDEXED, MessageFormat.format(
                            "Fieldname: {0}", fieldName));
            throw new InvalidFieldIndexQueryFatalQueryException(qe);
        }
        
        return copy(node);
        
    }
    
}
