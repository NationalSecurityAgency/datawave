package datawave.data.type;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A utility class that returns an index for a given Datawave {@link Type}
 */
public class DatawaveTypeIndex {

    private DatawaveTypeIndex() {
        // static utility
    }

    private static final Map<String,Integer> classNameIndex = new HashMap<>();
    static {
        classNameIndex.put(DateType.class.getTypeName(), 1);
        classNameIndex.put(GeoLatType.class.getTypeName(), 2);
        classNameIndex.put(GeoLonType.class.getTypeName(), 3);
        classNameIndex.put(GeometryType.class.getTypeName(), 4);
        classNameIndex.put(GeoType.class.getTypeName(), 5);
        classNameIndex.put(HexStringType.class.getTypeName(), 6);
        classNameIndex.put(HitTermType.class.getTypeName(), 7);
        classNameIndex.put(IpAddressType.class.getTypeName(), 8);
        classNameIndex.put(IpV4AddressType.class.getTypeName(), 9);
        classNameIndex.put(LcNoDiacriticsListType.class.getTypeName(), 10);
        classNameIndex.put(LcNoDiacriticsType.class.getTypeName(), 11);
        classNameIndex.put(LcType.class.getTypeName(), 12);
        classNameIndex.put(MacAddressType.class.getTypeName(), 13);
        classNameIndex.put(NoOpType.class.getTypeName(), 14);
        classNameIndex.put(NumberListType.class.getTypeName(), 15);
        classNameIndex.put(NumberType.class.getTypeName(), 16);
        classNameIndex.put(PointType.class.getTypeName(), 17);
        classNameIndex.put(RawDateType.class.getTypeName(), 18);
        classNameIndex.put(StringType.class.getTypeName(), 19);
        classNameIndex.put(TrimLeadingZerosType.class.getTypeName(), 20);
    }

    private static final Map<Integer,String> indexToClassName;
    static {
        indexToClassName = classNameIndex.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    }

    /**
     * Get the index for the provided {@link Type} name
     *
     * @param className
     *            the Type name
     * @return the index for the Type name, or zero if no such Type exists
     */
    public static int getIndexForTypeName(String className) {
        return classNameIndex.getOrDefault(className, 0);
    }

    /**
     * Get the {@link Type} name associated with the provided index
     *
     * @param index
     *            the index
     * @return the Type name or null if no such type name exists
     */
    public static String getTypeNameForIndex(int index) {
        return indexToClassName.get(index);
    }
}
