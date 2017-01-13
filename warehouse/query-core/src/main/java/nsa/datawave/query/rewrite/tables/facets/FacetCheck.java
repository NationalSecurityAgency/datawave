package nsa.datawave.query.rewrite.tables.facets;

import java.text.MessageFormat;
import java.util.NoSuchElementException;

import nsa.datawave.query.rewrite.Constants;
import nsa.datawave.query.rewrite.config.RefactoredShardQueryConfiguration;
import nsa.datawave.query.rewrite.exceptions.DatawaveFatalQueryException;
import nsa.datawave.query.rewrite.exceptions.InvalidFieldIndexQueryFatalQueryException;
import nsa.datawave.query.rewrite.jexl.JexlASTHelper;
import nsa.datawave.query.rewrite.jexl.visitors.AllTermsIndexedVisitor;
import nsa.datawave.query.rewrite.jexl.visitors.PrintingVisitor;
import nsa.datawave.query.util.MetadataHelper;
import nsa.datawave.webservice.query.exception.DatawaveErrorCode;
import nsa.datawave.webservice.query.exception.PreConditionFailedQueryException;
import nsa.datawave.webservice.query.exception.QueryException;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl2.parser.JexlNode;

import com.google.common.collect.Multimap;

public class FacetCheck extends AllTermsIndexedVisitor {
    
    Multimap<String,String> facetMultimap;
    
    public FacetCheck(RefactoredShardQueryConfiguration config, MetadataHelper helper) {
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
        String fieldName = null;
        try {
            fieldName = JexlASTHelper.getIdentifier(node);
        } catch (NoSuchElementException e) {
            // We only have literals
            PreConditionFailedQueryException qe = new PreConditionFailedQueryException(DatawaveErrorCode.EQUALS_NODE_TWO_LITERALS, e, MessageFormat.format(
                            "Node: {0}", PrintingVisitor.formattedQueryString(node).replace('\n', ' ')));
            throw new InvalidFieldIndexQueryFatalQueryException(qe);
        }
        
        if (Constants.ANY_FIELD.equals(fieldName)) {
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
