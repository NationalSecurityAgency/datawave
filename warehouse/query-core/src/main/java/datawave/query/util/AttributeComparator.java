package datawave.query.util;

import com.google.common.collect.Sets;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * This is a utility class used to ensure we are not overloading the JexlContext with unnecessary Attribute instances. In some cases, we will end up with a set
 * of Attributes that have identical data, visibilities, and timestamps, but differing class types and/or column family information. This can include but is not
 * limited to: datatype, uid. Attributes are considered equivalent when we find matching data, visibilities, timestamps, and where one has a populated column
 * family, but the other does not. In these cases we can safely combine datatype and uids into a single Attribute instance. If there are non-null deltas found
 * within column families, we do NOT consider these identical as they may be parent/child or multiple child relationships within Attributes.
 * <p>
 * Key k1 = new Key(row, new Text("datatype%00;d8zay2.-3pnndm.-anolok"), cq, cv, ts); Key k2 = new Key(row, new Text(""), cq, cv, ts);
 * <p>
 * PreNormalizedAttribute content1 = new PreNormalizedAttribute ("foo", k1, true); Content content2 = new Content("foo", k2, true);
 * <p>
 * We will end up with a single merged Attribute of type Content containing "datatype%00;d8zay2.-3pnndm.-anolok" in the Column Family.
 */
public final class AttributeComparator {

    // this is a utility and should never be instantiated
    private AttributeComparator() {
        throw new UnsupportedOperationException();
    }

    /**
     * Performs a simple comparison on a set of Attribute's data, Column Visibility, and Timestamp
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
     * Performs a simple comparison on a set of Attribute's data, Column Visibility, and Timestamp
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
     * Performs a simple comparison on a set of Attribute's data, Column Visibility, and Timestamp
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
     * Performs a matching check on the data, visibilities, and timestamps of two given Attributes. If such a match is found, further check the data contained
     * within each Column Family and combine the two Attributes if one is empty. If a delta is discovered, we return both Attribute instances as these should
     * not be considered equal. In the case that true duplicates are discovered we keep the Attribute that already exists within the Document and ignore the
     * potential new Attribute being checked. Preference is given to Attributes found in events, rather than the index. All other things being equal, priority
     * will be given to the Attribute in the first collection.
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

        //  @formatter:off
        attrs1.getAttributes().forEach(attr1 -> {
            attrs2.getAttributes().forEach(attr2 -> {
                if (singleToSingle(attr1, attr2)) {
                    if (attr1.getMetadata().getColumnFamily().getLength() == 0
                            && attr2.getMetadata().getColumnFamily().getLength() > 0) {
                        if (attr1.isFromIndex() && !attr2.isFromIndex()) {
                            combinedAttrSet.add(attr2);
                        } else if (!attr1.isFromIndex() && attr2.isFromIndex()) {
                            combinedAttrSet.add(attr1);
                        } else { // discard the Attribute without a populated Column Family
                            combinedAttrSet.add(mergeAttributes(attr2, attr1));
                            previouslyMerged.add(attr1);
                            previouslyMerged.add(attr2);
                        }
                    } else if ((attr2.getMetadata().getColumnFamily().getLength() == 0
                            && attr1.getMetadata().getColumnFamily().getLength() > 0)) {
                        if (attr1.isFromIndex() && !attr2.isFromIndex()) {
                            combinedAttrSet.add(attr2);
                        } else if (!attr1.isFromIndex() && attr2.isFromIndex()) {
                            combinedAttrSet.add(attr1);
                        } else { // discard the Attribute without a populated Column Family
                            combinedAttrSet.add(mergeAttributes(attr1, attr2));
                            previouslyMerged.add(attr1);
                            previouslyMerged.add(attr2);
                        }
                    } else if (attr1.getMetadata().compareTo(attr2.getMetadata()) == 0) { // true match, keep only one
                        combinedAttrSet.add(attr1);
                    } else { // non-matching metadata, keep both as these are possibly unique
                        combinedAttrSet.add(attr1);
                        combinedAttrSet.add(attr2);
                    }
                } else { // no match, assume unique and remove duplicates later
                    combinedAttrSet.add(attr1);
                    combinedAttrSet.add(attr2);
                }
            });
        });
        // @formatter:on

        // remove any "unique" attribute previously added but has now been merged
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

    public static Set<Attribute<? extends Comparable<?>>> combineSingleAttributes(final Attribute attr1, final Attribute attr2, final boolean trackSizes) {
        HashSet<Attribute<? extends Comparable<?>>> attrSet1 = Sets.newHashSet();
        HashSet<Attribute<? extends Comparable<?>>> attrSet2 = Sets.newHashSet();
        attrSet1.add(attr1);
        attrSet2.add(attr2);

        return combineMultipleAttributes(new Attributes(attrSet1, attr1.isToKeep(), trackSizes), new Attributes(attrSet2, attr2.isToKeep(), trackSizes));
    }

    /**
     * Combines two Attributes' with identical data, Visibilities, and timestamps into a single Attribute. This method assumes one Attribute has a populated
     * Column Family, the other does not, and that we want to transfer the information located within the populated Column Family's Attribute to the other
     * Attribute due to class type differences.
     *
     * @param attrWithCF
     *            An Attribute with populated datatype and uid in the Column Family
     * @param attrWithoutCF
     *            An Attribute without a populated datatype and uid in the Column Family
     * @return
     */
    private static Attribute mergeAttributes(final Attribute attrWithCF, final Attribute attrWithoutCF) {
        Attribute mergedAttr = null;

        mergedAttr = (Attribute) attrWithoutCF.copy();
        mergedAttr.setColumnFamily(attrWithCF.getMetadata().getColumnFamily());

        return mergedAttr;
    }

}
