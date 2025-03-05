package datawave.query.function;

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Cardinality;
import datawave.query.attributes.Document;
import datawave.query.attributes.FieldValueCardinality;
import datawave.query.data.parsers.DatawaveKey;

/**
 *
 */
public class AttributeToCardinality implements Function<Entry<Key,Document>,Entry<Key,Document>> {

    private static final Logger log = Logger.getLogger(AttributeToCardinality.class);
    private static final Text EMPTY_TEXT = new Text();

    /*
     * (non-Javadoc)
     *
     * @see com.google.common.base.Function#apply(java.lang.Object)
     */
    @Override
    public Entry<Key,Document> apply(Entry<Key,Document> input) {
        Document prevDoc = input.getValue();
        Key key = input.getKey();

        // for cardinalities, only use the visibility metadata
        Key metadata = new Key(EMPTY_TEXT, EMPTY_TEXT, EMPTY_TEXT, prevDoc.getColumnVisibility(), -1);

        Document newDoc = new Document();

        Map<?,?> dictionary = (Map<?,?>) prevDoc.getData();
        TreeMap<String,Attribute<? extends Comparable<?>>> newDictionary = Maps.newTreeMap();

        DatawaveKey parser = new DatawaveKey(input.getKey());

        for (Entry<?,?> attrE : dictionary.entrySet()) {

            Entry<String,Attribute<?>> attr = (Entry<String,Attribute<?>>) attrE;
            if (!attr.getKey().equals(Document.DOCKEY_FIELD_NAME)) {
                Attribute<?> attribute = attr.getValue();

                if (attribute instanceof Attributes) {
                    Attributes attrs = (Attributes) attribute;
                    Attributes newAttrs = new Attributes(attrs.isToKeep());

                    for (Attribute<?> attributeItem : attrs.getAttributes()) {
                        Cardinality card = null;
                        if (attributeItem instanceof Cardinality) {
                            card = (Cardinality) attributeItem;
                        } else {
                            FieldValueCardinality fvC = new FieldValueCardinality();
                            fvC.setContent(attributeItem.getData().toString());
                            fvC.setDoc(prevDoc);
                            card = new Cardinality(fvC, metadata, attrs.isToKeep());
                            if (log.isTraceEnabled())
                                log.trace("Adding from attributes " + attr.getKey() + " " + attributeItem.getData());
                        }
                        newAttrs.add(card);
                    }

                    newDictionary.put(attr.getKey(), newAttrs);
                } else {
                    Cardinality card = null;
                    if (attribute instanceof Cardinality) {
                        card = (Cardinality) attribute;
                    } else {
                        FieldValueCardinality fvC = new FieldValueCardinality();
                        fvC.setContent(attribute.getData().toString());
                        fvC.setDoc(prevDoc);
                        card = new Cardinality(fvC, metadata, attribute.isToKeep());

                        if (log.isTraceEnabled())
                            log.trace("Adding " + parser.getUid() + " " + attr.getKey() + " " + attribute.getData() + " " + fvC.getEstimate().cardinality());

                    }
                    newDictionary.put(attr.getKey(), card);

                }
            }

        }
        newDoc.putAll(newDictionary.entrySet().iterator(), false);

        return Maps.immutableEntry(key, newDoc);
    }

}
