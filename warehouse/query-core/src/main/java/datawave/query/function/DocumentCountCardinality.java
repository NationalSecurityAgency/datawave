package datawave.query.function;

import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

import datawave.query.attributes.Cardinality;
import datawave.query.attributes.Document;
import datawave.query.attributes.FieldValueCardinality;
import datawave.query.tables.facets.FacetedSearchType;

/**
 *
 */
public class DocumentCountCardinality implements Function<Entry<Key,Document>,Entry<Key,Document>> {

    private static final Logger log = Logger.getLogger(DocumentCountCardinality.class);
    private static final Text EMPTY_TEXT = new Text();

    protected FacetedSearchType type;

    protected boolean setDocIds = true;

    public DocumentCountCardinality(FacetedSearchType type, boolean setDocIds) {
        this.type = type;
        this.setDocIds = setDocIds;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.google.common.base.Function#apply(java.lang.Object)
     */
    @Override
    public Entry<Key,Document> apply(Entry<Key,Document> input) {
        Document prevDoc = input.getValue();
        Key key = input.getKey();

        // reduce the key to the document key pieces only
        key = new Key(key.getRow(), key.getColumnFamily());

        Document newDoc = new Document();
        if (prevDoc.size() > 0) {
            String dayOrShard = key.getRow().toString();
            if (FacetedSearchType.DAY_COUNT == type) {
                dayOrShard = dayOrShard.substring(0, dayOrShard.indexOf("_"));
            }

            FieldValueCardinality fvC = new FieldValueCardinality();
            fvC.setContent(dayOrShard);
            if (setDocIds)
                fvC.setDocId(dayOrShard);
            // use the overall visibility and timestamp
            Cardinality card = new Cardinality(fvC, key, true);

            newDoc.put(dayOrShard, card);

        }

        return Maps.immutableEntry(key, newDoc);
    }

}
