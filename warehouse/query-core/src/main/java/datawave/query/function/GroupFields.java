package datawave.query.function;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.attributes.Numeric;

public class GroupFields implements Function<Entry<Key,Document>,Entry<Key,Document>> {

    private static final Logger log = Logger.getLogger(GroupFields.class);

    public static final String ORIGINAL_COUNT_SUFFIX = "ORIGINAL_COUNT";

    private Map<String,Integer> groupFieldsMap = Maps.newConcurrentMap();

    private Collection<String> groupFieldsSet;

    public GroupFields(Collection<String> groupFieldsSet) {
        this.groupFieldsSet = groupFieldsSet;
        if (log.isDebugEnabled())
            log.debug("GroupFields: groupFieldsSet set to:" + groupFieldsSet);
    }

    @Override
    public Entry<Key,Document> apply(Entry<Key,Document> entry) {

        Document document = entry.getValue();

        for (Map.Entry<String,Attribute<? extends Comparable<?>>> de : document.entrySet()) {
            String keyWithGrouping = de.getKey();
            log.trace("keyWithGrouping is:" + keyWithGrouping);
            String keyNoGrouping = keyWithGrouping;
            // if we have grouping context on, remove the grouping context
            if (keyNoGrouping.indexOf('.') != -1) {
                keyNoGrouping = keyNoGrouping.substring(0, keyNoGrouping.indexOf('.'));
                log.trace("keyNoGrouping is:" + keyNoGrouping);
            }
            if (this.groupFieldsSet.contains(keyNoGrouping)) { // look for the key without the grouping context
                if (log.isTraceEnabled())
                    log.trace("groupFieldsSet contains " + keyNoGrouping + " so grouping with " + keyWithGrouping);
                Attribute<?> attr = de.getValue();

                int delta = 1;
                if (attr instanceof Attributes) {
                    Attributes attrs = (Attributes) attr;
                    delta = attrs.size();
                    log.trace("delta for " + attrs + " is " + delta);
                } else {
                    log.trace("delta for " + attr + " is " + delta);
                }
                // increment the count
                int count = this.groupFieldsMap.get(keyWithGrouping) == null ? 0 : this.groupFieldsMap.get(keyWithGrouping);
                this.groupFieldsMap.put(keyWithGrouping, count + delta);

            }
        }
        // mutate the document with the changes collected in the above loop
        applyCounts(document, groupFieldsMap);
        return entry;
    }

    /**
     * Adds new fields to the document to hold the original count of any fields that have been reduced by the limit.fields parameter. The new fields are named
     * like this: {the.field.that.was.limited}_ORIGINAL_COUNT
     *
     * @param doc
     *            a document
     * @param groupFieldsMap
     *            the group fields map
     */
    private void applyCounts(Document doc, Map<String,Integer> groupFieldsMap) {
        if (!groupFieldsMap.entrySet().isEmpty()) {
            for (Entry<String,Integer> groupFieldCountEntry : groupFieldsMap.entrySet()) {
                Attribute<?> removedAttr = doc.remove(groupFieldCountEntry.getKey());
                log.debug("removed from document:" + groupFieldCountEntry.getKey());
                doc.put(groupFieldCountEntry.getKey(), new Numeric(groupFieldCountEntry.getValue(), doc.getMetadata(), removedAttr.isToKeep()), true);
                log.debug("added to document:" + groupFieldCountEntry.getKey() + " with count of " + groupFieldCountEntry.getValue());
            }
        }
    }

}
