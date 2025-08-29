package datawave.query.attributes;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import datawave.query.common.grouping.GroupingAttribute;

/**
 * A utility class that returns an index for a given Datawave {@link Attribute}
 */
public class DatawaveAttributeIndex {

    private DatawaveAttributeIndex() {
        // static utility
    }

    private static final Map<String,Integer> classNameIndex = new HashMap<>();
    static {
        classNameIndex.put(AttributeBag.class.getTypeName(), 1);
        classNameIndex.put(Attributes.class.getTypeName(), 2);
        classNameIndex.put(Cardinality.class.getTypeName(), 3);
        classNameIndex.put(Content.class.getTypeName(), 4);
        classNameIndex.put(DiacriticContent.class.getTypeName(), 5);
        // skip Document
        classNameIndex.put(DocumentKey.class.getTypeName(), 6);
        classNameIndex.put(GeoPoint.class.getTypeName(), 7);
        classNameIndex.put(Geometry.class.getTypeName(), 8);
        classNameIndex.put(GroupingAttribute.class.getTypeName(), 9);
        classNameIndex.put(IpAddress.class.getTypeName(), 10);
        classNameIndex.put(Latitude.class.getTypeName(), 11);
        classNameIndex.put(Longitude.class.getTypeName(), 12);
        classNameIndex.put(Metadata.class.getTypeName(), 13);
        classNameIndex.put(Numeric.class.getTypeName(), 14);
        classNameIndex.put(PreNormalizedAttribute.class.getTypeName(), 15);
        classNameIndex.put(TimingMetadata.class.getTypeName(), 16);
        classNameIndex.put(TypeAttribute.class.getTypeName(), 17);
        classNameIndex.put(WaitWindowExceededMetadata.class.getTypeName(), 18);
    }

    private static final Map<Integer,String> indexToClassName;
    static {
        indexToClassName = classNameIndex.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    }

    /**
     * Get the index of the provided {@link Attribute} class name
     *
     * @param className
     *            the attribute name
     * @return the index of the attribute name, or zero if no such attribute exists
     */
    public static int getAttributeIndex(String className) {
        return classNameIndex.getOrDefault(className, 0);
    }

    /**
     * Get the {@link Attribute} name associated with the provided index
     *
     * @param index
     *            the index
     * @return the Attribute name or null if no such type name exists
     */
    public static String getAttributeClassName(int index) {
        return indexToClassName.get(index);
    }
}
