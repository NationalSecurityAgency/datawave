package datawave.query.util;

import com.google.common.collect.Sets;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public final class AttributeComparator {
    
    // this is a utility and should never be instantiated
    private AttributeComparator() {
        throw new UnsupportedOperationException();
    }
    
    public static boolean singleToSingle(final Attribute attr1, final Attribute attr2) {
        return (attr1.getData().equals(attr2.getData()) && attr1.getColumnVisibility().equals(attr2.getColumnVisibility()) && attr1.getTimestamp() == attr2
                        .getTimestamp());
    }
    
    public static boolean singleToMultiple(final Attribute attr, final Attributes attrs) {
        return attrs.getAttributes().stream().anyMatch(existingAttribute -> singleToSingle(existingAttribute, attr));
    }
    
    public static boolean multipleToMultiple(final Attributes attrs1, final Attributes attrs2) {
        boolean containsMatch = false;
        for (Attribute<? extends Comparable<?>> newAttr : attrs2.getAttributes()) {
            if (singleToMultiple(newAttr, attrs1)) {
                containsMatch = true;
            }
        }
        return containsMatch;
    }
    
    /**
     * Performs a basic match on the content/data of two given Attributes. If such a match is found, combine the metadata from the two Attributes and create new
     * deduplicated Attributes to return to the Document.
     *
     * @param attrs1
     *            The Attributes against which we want to compare
     * @param attrs2
     *            The Attributes against which we want to check
     * @return
     */
    public static Set<Attribute<? extends Comparable<?>>> combineMultipleAttributes(final Attributes attrs1, final Attributes attrs2) {
        Map<Attribute<? extends Comparable<?>>,Integer> combinedAttrMap = new HashMap<>();
        List<Attribute> trueDuplicates = new ArrayList<Attribute>();
        
        attrs1.getAttributes().forEach(attr1 -> {
            AtomicBoolean containsMatch = new AtomicBoolean(false);
            attrs2.getAttributes().forEach(attr2 -> {
                if (singleToSingle(attr1, attr2) && (attr1.isToKeep() || attr2.isToKeep())) {
                    if (attr1.getMetadata().getSize() == attr2.getMetadata().getSize()) {
                        containsMatch.set(true);
                        trueDuplicates.add(attr1);
                        combinedAttrMap.put(attr1, 1); // found true match so return pre-existing Attribute
                        } else {
                            Attribute combinedAttr = combineSingleAttributes(attr1, attr2);
                            if (combinedAttrMap.get(combinedAttr) == null) {
                                combinedAttrMap.put(combinedAttr, 1);
                            } else {
                                combinedAttrMap.put(combinedAttr, combinedAttrMap.get(combinedAttr) + 1);
                            }
                            containsMatch.set(true);
                        }
                    } else if (attrs2.getAttributes().size() > 1 && !trueDuplicates.contains(attr2)) {
                        if (combinedAttrMap.get(attr2) == null) {
                            combinedAttrMap.put(attr2, 1);
                        } else {
                            combinedAttrMap.put(attr2, combinedAttrMap.get(attr2) + 1);
                        }
                    }
                });
            if (!containsMatch.get() && attrs1.getAttributes().size() > 1 && !trueDuplicates.contains(attr1)) {
                if (combinedAttrMap.get(attr1) == null) {
                    combinedAttrMap.put(attr1, 1);
                } else {
                    combinedAttrMap.put(attr1, combinedAttrMap.get(attr1) + 1);
                }
            }
        });
        
        return combinedAttrMap.entrySet().stream().filter(entry -> entry.getValue() == 1).map(Map.Entry::getKey).collect(Collectors.toSet());
    }
    
    public static Set<Attribute<? extends Comparable<?>>> combineMultipleAttributes(final Attribute attr, final Attributes attrs, final boolean trackSizes) {
        HashSet<Attribute<? extends Comparable<?>>> attrSet = Sets.newHashSet();
        attrSet.add(attr);
        
        return combineMultipleAttributes(new Attributes(attrSet, attrs.isToKeep(), trackSizes), attrs);
    }
    
    public static Set<Attribute<? extends Comparable<?>>> combineMultipleAttributes(final Attributes attrs, final Attribute attr, final boolean trackSizes) {
        HashSet<Attribute<? extends Comparable<?>>> attrSet = Sets.newHashSet();
        attrSet.add(attr);
        
        return combineMultipleAttributes(attrs, new Attributes(attrSet, attrs.isToKeep(), trackSizes));
    }
    
    public static Attribute combineSingleAttributes(final Attribute attr1, final Attribute attr2) {
        int attr1mdSize = attr1.getMetadata().getSize();
        int attr2mdSize = attr2.getMetadata().getSize();
        
        Attribute mergedAttr = null;
        if (attr1.isToKeep()) {
            if (attr1mdSize > attr2mdSize) {
                mergedAttr = attr1;
            } else {
                mergedAttr = (Attribute) attr1.copy();
                mergedAttr.setMetadata(attr2.getMetadata());
            }
        } else {
            if (attr2mdSize > attr1mdSize) {
                mergedAttr = attr2;
            } else {
                mergedAttr = (Attribute) attr2.copy();
                mergedAttr.setMetadata(attr1.getMetadata());
            }
        }
        
        return mergedAttr;
    }
    
}
