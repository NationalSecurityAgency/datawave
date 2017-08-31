package nsa.datawave.query.rewrite.predicate;

import java.util.Map;
import java.util.Map.Entry;

import nsa.datawave.query.rewrite.Constants;
import nsa.datawave.query.rewrite.attributes.Document;
import nsa.datawave.query.rewrite.tld.TLD;
import org.apache.accumulo.core.data.Key;
import org.apache.commons.jexl2.parser.ASTJexlScript;

/**
 * This filter will filter event data keys by only those fields that are required in the specified query except for the base document in which case all fields
 * are returned.
 */
public class AncestorEventDataFilter extends EventDataQueryFilter {
    /**
     * Initialize the query field filter with all of the fields required to evaluation this query
     * 
     * @param script
     */
    public AncestorEventDataFilter(ASTJexlScript script) {
        super(script);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see nsa.datawave.query.rewrite.function.Filter#accept(org.apache.accumulo.core.data.Key)
     */
    @Override
    public boolean apply(Entry<Key,String> input) {
        // if the base document, then accept em all, otherwise defer to the quey field filter
        if (keep(input.getKey())) {
            return true;
        } else {
            return super.apply(input);
        }
    }
    
    protected String docUid = null;
    
    @Override
    public void setDocumentKey(Key document) {
        super.setDocumentKey(document);
        this.docUid = getUid(document);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see nsa.datawave.query.rewrite.function.Filter#keep(org.apache.accumulo.core.data.Key)
     */
    @Override
    public boolean keep(Key k) {
        // only keep the data for the document of interest
        return (docUid == null || docUid.equals(getUid(k)));
    }
    
    /**
     * We need to remap the start key to the begining of the TLD so that we can get all of the necessary fields when scanning the shard table.
     * 
     * @param from
     */
    @Override
    public Key getStartKey(Key from) {
        Key startKey = new Key(from.getRow().toString(), TLD.parseRootPointerFromId(from.getColumnFamily().toString()));
        return startKey;
    }
    
    protected String getUid(Key k) {
        String uid;
        String cf = k.getColumnFamily().toString();
        if (cf.equals(Constants.TERM_FREQUENCY_COLUMN_FAMILY.toString())) {
            String cq = k.getColumnQualifier().toString();
            int start = cq.indexOf('\0') + 1;
            uid = cq.substring(start, cq.indexOf('\0', start));
        } else if (cf.startsWith("fi\0")) {
            String cq = k.getColumnQualifier().toString();
            uid = cq.substring(cq.lastIndexOf('\0') + 1);
        } else {
            uid = cf.substring(cf.lastIndexOf('\0') + 1);
        }
        return uid;
    }
}
