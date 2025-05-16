package datawave.data.type;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.util.Assert;

import com.google.common.collect.Lists;

public class ListTypeTest {

    @Test
    public void test() {
        String str = "1,2,3;a;b;c";

        LcNoDiacriticsListType t = new LcNoDiacriticsListType(str);
        Assert.equals(6, t.normalizeToMany(str).size());
        List<Pair<String,Type.Category>> expected = Lists.newArrayList();
        expected.add(Pair.of("1", Type.Category.LIST_ELEMENT));
        expected.add(Pair.of("2", Type.Category.LIST_ELEMENT));
        expected.add(Pair.of("3", Type.Category.LIST_ELEMENT));
        expected.add(Pair.of("a", Type.Category.LIST_ELEMENT));
        expected.add(Pair.of("b", Type.Category.LIST_ELEMENT));
        expected.add(Pair.of("c", Type.Category.LIST_ELEMENT));
        Assert.equals(expected, t.normalizeToMany(str));
    }

    @Test
    public void testLcNDList() {
        String str = "01,02,03;A;B;C";

        LcNoDiacriticsListType t = new LcNoDiacriticsListType();
        Assert.equals(6, t.normalizeToMany(str).size());
        List<Pair<String,Type.Category>> expected = Lists.newArrayList();
        expected.add(Pair.of("01", Type.Category.LIST_ELEMENT));
        expected.add(Pair.of("02", Type.Category.LIST_ELEMENT));
        expected.add(Pair.of("03", Type.Category.LIST_ELEMENT));
        expected.add(Pair.of("a", Type.Category.LIST_ELEMENT));
        expected.add(Pair.of("b", Type.Category.LIST_ELEMENT));
        expected.add(Pair.of("c", Type.Category.LIST_ELEMENT));
        Assert.equals(expected, t.normalizeToMany(str));
    }

    @Test
    public void testNumberList() {
        String str = "1,2,3,5.5";
        List<Pair<String,Type.Category>> expected = Lists.newArrayList();
        expected.add(Pair.of("+aE1", Type.Category.LIST_ELEMENT));
        expected.add(Pair.of("+aE2", Type.Category.LIST_ELEMENT));
        expected.add(Pair.of("+aE3", Type.Category.LIST_ELEMENT));
        expected.add(Pair.of("+aE5.5", Type.Category.LIST_ELEMENT));

        NumberListType nt = new NumberListType();
        Assert.equals(4, nt.normalizeToMany(str).size());
        Assert.equals(expected, nt.normalizeToMany(str));
    }

    @Test
    public void testBadNumberList() {
        String str = "3,2,1,banana";

        NumberListType nt = new NumberListType();
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            nt.normalizeToMany(str);
        });

    }

}
