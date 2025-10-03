package datawave.query.common.grouping;

import java.math.BigDecimal;

import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import datawave.data.type.LcNoDiacriticsListType;
import datawave.data.type.NumberType;

@SuppressWarnings({"unchecked", "rawtypes"})
class GroupingAttributeTest {

    @Test
    void testEqualsWithSameRowAndValue() {
        GroupingAttribute<BigDecimal> attr1 = new GroupingAttribute<>(new NumberType("123"), new Key("FOO.1"), true);
        GroupingAttribute<BigDecimal> attr2 = new GroupingAttribute<>(new NumberType("123"), new Key("FOO.1"), true);

        Assertions.assertEquals(attr1, attr2);
    }

    @Test
    void testEqualsWithSameRowAndDifferentValue() {
        GroupingAttribute<BigDecimal> attr1 = new GroupingAttribute<>(new NumberType("123"), new Key("FOO.1"), true);
        GroupingAttribute<BigDecimal> attr2 = new GroupingAttribute<>(new NumberType("456"), new Key("FOO.1"), true);

        Assertions.assertNotEquals(attr1, attr2);
    }

    @Test
    void testEqualsWithDifferentRowAndSameValue() {
        GroupingAttribute<BigDecimal> attr1 = new GroupingAttribute<>(new NumberType("123"), new Key("FOO.1"), true);
        GroupingAttribute<BigDecimal> attr2 = new GroupingAttribute<>(new NumberType("123"), new Key("BAR.1"), true);

        Assertions.assertNotEquals(attr1, attr2);
    }

    @Test
    void testHashCodeIgnoresRowAndType() {
        GroupingAttribute<BigDecimal> attr1 = new GroupingAttribute<>(new NumberType("123"), new Key("FOO.1"), true);
        GroupingAttribute<BigDecimal> attr2 = new GroupingAttribute<>(new LcNoDiacriticsListType("123"), new Key("BAR.1"), true);

        Assertions.assertEquals(attr1.hashCode(), attr2.hashCode());

    }
}
