package datawave.query.function;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.predicate.Projection;
import datawave.query.attributes.Document;

import org.apache.accumulo.core.data.Key;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

/**
 * Applies a whitelist or blacklist projection to a Document. Whitelist projection will preserve Document sub-substructure whereas blacklist projection will
 * prune sub-substructure which does not match the blacklist.
 *
 * e.g. Input: {NAME:'bob', CHILDREN:[{NAME:'frank', AGE:12}, {NAME:'sally', AGE:10}], AGE:40}
 *
 * Whitelist of 'NAME' applied: {NAME:'bob', CHILDREN:[{NAME:'frank'}, {NAME:'sally'}]}
 *
 * Blacklist of 'NAME' applied: {CHILDREN:[{AGE:12}, {AGE:10}], AGE:40}
 */
public class DocumentProjection implements Permutation<Entry<Key,Document>> {
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(DocumentProjection.class);
    
    private final boolean includeGroupingContext;
    private final boolean reducedResponse;
    private final Projection projection;
    
    public DocumentProjection() {
        this(false, false);
    }
    
    public DocumentProjection(boolean includeGroupingContext, boolean reducedResponse) {
        this.includeGroupingContext = includeGroupingContext;
        this.reducedResponse = reducedResponse;
        this.projection = new Projection();
    }
    
    public void initializeWhitelist(Set<String> whiteListFields) {
        this.projection.setWhitelist(whiteListFields);
    }
    
    public void initializeBlacklist(Set<String> blackListFields) {
        this.projection.setBlacklist(blackListFields);
    }
    
    public Projection getProjection() {
        return projection;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.google.common.base.Function#apply(java.lang.Object)
     */
    @Override
    public Entry<Key,Document> apply(Entry<Key,Document> from) {
        Document returnDoc = trim(from.getValue());
        
        return Maps.immutableEntry(from.getKey(), returnDoc);
    }
    
    private Document trim(Document d) {
        Map<String,Attribute<? extends Comparable<?>>> dict = d.getDictionary();
        Document newDoc = new Document();
        
        for (Entry<String,Attribute<? extends Comparable<?>>> entry : dict.entrySet()) {
            String fieldName = entry.getKey();
            Attribute<?> attr = entry.getValue();
            
            if (projection.apply(fieldName)) {
                // If the projection is using a blacklist, we want to
                // traverse down this subtree, and remove entries that
                // should be excluded via the blacklist
                if (projection.isUseBlacklist()) {
                    if (attr instanceof Document) {
                        Document newSubDoc = trim((Document) attr);
                        
                        if (0 < newSubDoc.size()) {
                            newDoc.put(fieldName, newSubDoc.copy(), this.includeGroupingContext, this.reducedResponse);
                        }
                        
                        continue;
                    } else if (attr instanceof Attributes) {
                        Attributes subAttrs = trim((Attributes) attr, fieldName);
                        
                        if (0 < subAttrs.size()) {
                            newDoc.put(fieldName, subAttrs.copy(), this.includeGroupingContext, this.reducedResponse);
                        }
                        
                        continue;
                    }
                }
                
                // We just want to add this subtree
                newDoc.put(fieldName, (Attribute<?>) attr.copy(), this.includeGroupingContext, this.reducedResponse);
                
            } else if (!projection.isUseBlacklist()) {
                // Blacklist will completely exclude a subtree, whereas a whitelist
                // will include parents that potentially do not match the whitelist
                // if there is a child that does match the whitelist
                if (attr instanceof Document) {
                    Document newSubDoc = trim((Document) attr);
                    
                    if (0 < newSubDoc.size()) {
                        newDoc.put(fieldName, newSubDoc.copy(), this.includeGroupingContext, this.reducedResponse);
                    }
                } else if (attr instanceof Attributes) {
                    // Since Document instances can be nested under attributes and vice-versa
                    // all the way down, we need to pass along the fieldName so that when we
                    // have come up with a nested document it can be evaluated by its own name
                    Attributes subAttrs = trim((Attributes) attr, fieldName);
                    
                    if (0 < subAttrs.size()) {
                        newDoc.put(fieldName, subAttrs.copy(), this.includeGroupingContext, this.reducedResponse);
                    }
                }
            }
        }
        
        return newDoc;
    }
    
    private Attributes trim(Attributes attrs, String fieldName) {
        Attributes newAttrs = new Attributes(attrs.isToKeep());
        for (Attribute<? extends Comparable<?>> attr : attrs.getAttributes()) {
            if (attr instanceof Document) {
                Document newAttr = trim((Document) attr);
                
                if (0 < newAttr.size()) {
                    newAttrs.add(newAttr);
                }
            } else if (attr instanceof Attributes) {
                Attributes newAttr = trim((Attributes) attr, fieldName);
                
                if (0 < newAttr.size()) {
                    newAttrs.add(newAttr);
                }
            } else if (projection.apply(fieldName)) {
                // If we're trimming an Attributes and find an Attribute that
                // doesn't nest more Attribute's (Document, Attributes), otherwise,
                // we can retain the "singular" Attribute's (Content, Numeric, etc)
                // if it applies
                newAttrs.add(attr);
            }
        }
        
        return newAttrs;
    }
    
}
