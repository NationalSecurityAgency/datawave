package datawave.data.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class DatawaveTypeIndexTest {

    @Test
    public void testGetIndexForTypeName() {
        assertEquals(1, DatawaveTypeIndex.getIndexForTypeName(DateType.class.getTypeName()));
        assertEquals(2, DatawaveTypeIndex.getIndexForTypeName(GeoLatType.class.getTypeName()));
        assertEquals(3, DatawaveTypeIndex.getIndexForTypeName(GeoLonType.class.getTypeName()));
        assertEquals(4, DatawaveTypeIndex.getIndexForTypeName(GeometryType.class.getTypeName()));
        assertEquals(5, DatawaveTypeIndex.getIndexForTypeName(GeoType.class.getTypeName()));
        assertEquals(6, DatawaveTypeIndex.getIndexForTypeName(HexStringType.class.getTypeName()));
        assertEquals(7, DatawaveTypeIndex.getIndexForTypeName(HitTermType.class.getTypeName()));
        assertEquals(8, DatawaveTypeIndex.getIndexForTypeName(IpAddressType.class.getTypeName()));
        assertEquals(9, DatawaveTypeIndex.getIndexForTypeName(IpV4AddressType.class.getTypeName()));
        assertEquals(10, DatawaveTypeIndex.getIndexForTypeName(LcNoDiacriticsListType.class.getTypeName()));
        assertEquals(11, DatawaveTypeIndex.getIndexForTypeName(LcNoDiacriticsType.class.getTypeName()));
        assertEquals(12, DatawaveTypeIndex.getIndexForTypeName(LcType.class.getTypeName()));
        assertEquals(13, DatawaveTypeIndex.getIndexForTypeName(MacAddressType.class.getTypeName()));
        assertEquals(14, DatawaveTypeIndex.getIndexForTypeName(NoOpType.class.getTypeName()));
        assertEquals(15, DatawaveTypeIndex.getIndexForTypeName(NumberListType.class.getTypeName()));
        assertEquals(16, DatawaveTypeIndex.getIndexForTypeName(NumberType.class.getTypeName()));
        assertEquals(17, DatawaveTypeIndex.getIndexForTypeName(PointType.class.getTypeName()));
        assertEquals(18, DatawaveTypeIndex.getIndexForTypeName(RawDateType.class.getTypeName()));
        assertEquals(19, DatawaveTypeIndex.getIndexForTypeName(StringType.class.getTypeName()));
        assertEquals(20, DatawaveTypeIndex.getIndexForTypeName(TrimLeadingZerosType.class.getTypeName()));
    }

    @Test
    public void testGetTypeNameForIndex() {
        assertEquals(DateType.class.getTypeName(), DatawaveTypeIndex.getTypeNameForIndex(1));
        assertEquals(GeoLatType.class.getTypeName(), DatawaveTypeIndex.getTypeNameForIndex(2));
        assertEquals(GeoLonType.class.getTypeName(), DatawaveTypeIndex.getTypeNameForIndex(3));
        assertEquals(GeometryType.class.getTypeName(), DatawaveTypeIndex.getTypeNameForIndex(4));
        assertEquals(GeoType.class.getTypeName(), DatawaveTypeIndex.getTypeNameForIndex(5));
        assertEquals(HexStringType.class.getTypeName(), DatawaveTypeIndex.getTypeNameForIndex(6));
        assertEquals(HitTermType.class.getTypeName(), DatawaveTypeIndex.getTypeNameForIndex(7));
        assertEquals(IpAddressType.class.getTypeName(), DatawaveTypeIndex.getTypeNameForIndex(8));
        assertEquals(IpV4AddressType.class.getTypeName(), DatawaveTypeIndex.getTypeNameForIndex(9));
        assertEquals(LcNoDiacriticsListType.class.getTypeName(), DatawaveTypeIndex.getTypeNameForIndex(10));
        assertEquals(LcNoDiacriticsType.class.getTypeName(), DatawaveTypeIndex.getTypeNameForIndex(11));
        assertEquals(LcType.class.getTypeName(), DatawaveTypeIndex.getTypeNameForIndex(12));
        assertEquals(MacAddressType.class.getTypeName(), DatawaveTypeIndex.getTypeNameForIndex(13));
        assertEquals(NoOpType.class.getTypeName(), DatawaveTypeIndex.getTypeNameForIndex(14));
        assertEquals(NumberListType.class.getTypeName(), DatawaveTypeIndex.getTypeNameForIndex(15));
        assertEquals(NumberType.class.getTypeName(), DatawaveTypeIndex.getTypeNameForIndex(16));
        assertEquals(PointType.class.getTypeName(), DatawaveTypeIndex.getTypeNameForIndex(17));
        assertEquals(RawDateType.class.getTypeName(), DatawaveTypeIndex.getTypeNameForIndex(18));
        assertEquals(StringType.class.getTypeName(), DatawaveTypeIndex.getTypeNameForIndex(19));
        assertEquals(TrimLeadingZerosType.class.getTypeName(), DatawaveTypeIndex.getTypeNameForIndex(20));
    }

    @Test
    public void testGetIndexForTypeThatDoesNotExist() {
        assertEquals(0, DatawaveTypeIndex.getIndexForTypeName(BaseType.class.getTypeName()));
    }

    @Test
    public void testGetTypeNameForIndexThatDoesNotExist() {
        assertNull(DatawaveTypeIndex.getTypeNameForIndex(100));
        assertNull(DatawaveTypeIndex.getTypeNameForIndex(0));
        assertNull(DatawaveTypeIndex.getTypeNameForIndex(-3));
    }

}
