package datawave.query.util;

import com.google.common.collect.Sets;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public final class AttributeComparator {
    
    // this is a utility and should never be instantiated
    private AttributeComparator() {
        throw new UnsupportedOperationException();
    }
    
    /**
     * Performs a simple comparison on a set of Attribute's data and some parts of their metadata
     *
     * @param attr1
     *            An Attribute against which we want to compare
     * @param attr2
     *            Another Attribute against which we want to check for likeness
     * @return
     */
    public static boolean singleToSingle(final Attribute attr1, final Attribute attr2) {
        //  @formatter:off
        return attr1.getData().equals(attr2.getData()) &&
                attr1.getColumnVisibility().equals(attr2.getColumnVisibility()) &&
                attr1.getTimestamp() == attr2.getTimestamp();
        //  @formatter:on
    }
    
    /**
     * Performs a simple comparison on a set of Attribute's data and some parts of their metadata
     *
     * @param attr
     *            An Attribute against which we want to compare
     * @param attrs
     *            An Attributes against which we want to check for any matches contained therein
     * @return
     */
    public static boolean singleToMultiple(final Attribute attr, final Attributes attrs) {
        return attrs.getAttributes().stream().anyMatch(existingAttribute -> singleToSingle(existingAttribute, attr));
    }
    
    /**
     * Performs a simple comparison on a set of Attribute's data and some parts of their metadata
     *
     * @param attrs1
     *            An Attributes against which we want to compare
     * @param attrs1
     *            An Attributes against which we want to check for any matches contained between the two sets
     * @return
     */
    public static boolean multipleToMultiple(final Attributes attrs1, final Attributes attrs2) {
        for (Attribute<? extends Comparable<?>> newAttr : attrs2.getAttributes()) {
            if (singleToMultiple(newAttr, attrs1)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Performs a basic match on the content/data of two given Attributes. If such a match is found, combine the metadata from the two Attributes and create new
     * deduplicated Attributes to return to the Document. Preference is given to the Attribute found in the event, NOT the index. In the case that true
     * duplicates are discovered we keep the Attribute that already exists within the Document, set the Attribute's toKeep() flag to true (if not already), and
     * ignore the potential new Attribute being checked. All other things being equal, priority will be given to the first collection.
     *
     * @param attrs1
     *            The Attributes already in the Document against which we want to compare
     * @param attrs2
     *            The Attributes we want to check before potentially adding to the Document
     * @return
     */
    public static Set<Attribute<? extends Comparable<?>>> combineMultipleAttributes(final Attributes attrs1, final Attributes attrs2) {
        Set<Attribute<? extends Comparable<?>>> combinedAttrSet = new HashSet<>();
        Set<Attribute> previouslyMerged = new HashSet<Attribute>();
        List<Attribute> trueDuplicates = new ArrayList<Attribute>();
        
        attrs1.getAttributes().forEach(attr1 -> {
            AtomicBoolean containsMatch = new AtomicBoolean(false);
            attrs2.getAttributes().forEach(attr2 -> {
                if (singleToSingle(attr1, attr2) && (attr1.isToKeep() || attr2.isToKeep())) {
                    if (attr1.getMetadata().getSize() == attr2.getMetadata().getSize()) {
                        containsMatch.set(true);
                        trueDuplicates.add(attr1);
                        attr1.setToKeep(true);
                        combinedAttrSet.add(attr1); // found true match so return pre-existing Attribute
                        } else {
                            Attribute combinedAttr = combineSingleAttributes(attr1, attr2);
                            if (!combinedAttrSet.contains(combinedAttr)) {
                                combinedAttrSet.add(combinedAttr);
                            }
                            previouslyMerged.add(attr1);
                            previouslyMerged.add(attr2);
                            
                            // keep track of whether or not we merged an Attribute for later
                            if (combinedAttrSet.contains(attr1)) {
                                combinedAttrSet.remove(attr1);
                            }
                            if (combinedAttrSet.contains(attr2)) {
                                combinedAttrSet.remove(attr2);
                            }
                            
                            containsMatch.set(true);
                        }
                    } else if (attrs2.getAttributes().size() > 1 && !trueDuplicates.contains(attr2)) {
                        if (!combinedAttrSet.contains(attr2)) {
                            attr2.setToKeep(true);
                            combinedAttrSet.add(attr2);
                        }
                    }
                });
            if (!containsMatch.get() && attrs1.getAttributes().size() > 1 && !trueDuplicates.contains(attr1)) {
                if (!combinedAttrSet.contains(attr1)) {
                    combinedAttrSet.add(attr1);
                }
            }
        });
        
        // remove any previously "unique" attribute previously added but has now been merged
        Iterator<Attribute> previousItr = previouslyMerged.iterator();
        while (previousItr.hasNext()) {
            combinedAttrSet.remove(previousItr.next());
        }
        
        return combinedAttrSet;
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
    
    /**
     * Compares two individual Attribute's flags and metadata, preferring to keep the Attribute from the event rather than the index. If the origin of the
     * Attributes being compared is identical, prefer to keep the one with the toKeep() flag set.
     *
     * @param attr1
     *            An Attribute against which we want to compare
     * @param attr2
     *            Another Attribute against which we want to check for likeness
     * @return
     */
    public static Attribute combineSingleAttributes(final Attribute attr1, final Attribute attr2) {
        int attr1mdSize = attr1.getMetadata().getSize();
        int attr2mdSize = attr2.getMetadata().getSize();
        
        Attribute mergedAttr = null;
        
        if (!attr1.isFromIndex() && attr2.isFromIndex()) {
            // prefer attr1 since from event
            if (attr1mdSize > attr2mdSize) {
                mergedAttr = attr1;
            } else {
                mergedAttr = (Attribute) attr1.copy();
                mergedAttr.setMetadata(attr2.getMetadata());
                mergedAttr.setFromIndex(false);
            }
        } else if (!attr2.isFromIndex() && attr1.isFromIndex()) {
            // prefer attr2 since from event
            if (attr2mdSize > attr1mdSize) {
                mergedAttr = attr2;
            } else {
                mergedAttr = (Attribute) attr2.copy();
                mergedAttr.setMetadata(attr1.getMetadata());
                mergedAttr.setFromIndex(false);
            }
        } else if ((!attr1.isFromIndex() && !attr2.isFromIndex()) || (attr1.isFromIndex()) && attr2.isFromIndex()) {
            // prefer neither since both have identical origins
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
        }
        
        return mergedAttr;
    }
    
}
