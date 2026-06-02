package datawave.data.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TypeFactoryTest {

    private TypeFactory typeFactory;

    @BeforeEach
    public void before() {
        typeFactory = new TypeFactory();
    }

    @Test
    public void testWithCorrectType() {
        Type<?> type = Type.Factory.createType("datawave.data.type.LcType");
        assertInstanceOf(LcType.class, type);
    }

    @Test
    public void testWithIncorrectType() {
        assertThrows(IllegalArgumentException.class, () -> Type.Factory.createType("datawave.ingest.data.normalizer.LcNoDiacriticsNormalizer"));
    }

    @Test
    public void testTypeFactoryWithCache() {
        TypeFactory factory = new TypeFactory();

        Type<?> typeOne = factory.createType(LcType.class.getName());
        Type<?> typeTwo = factory.createType(LcType.class.getName());

        assertSame(typeOne, typeTwo);
    }

    @Test
    public void testTypeFactoryCustomSize() {
        TypeFactory factory = new TypeFactory(1, 15);

        Type<?> typeOne = factory.createType(LcType.class.getName());

        Type<?> left = factory.createType(IpAddressType.class.getName());
        Type<?> right = factory.createType(IpAddressType.class.getName());
        // same type created in a row with a cache size of one will return the same type instance
        assertSame(left, right);

        // at this point the cache could contain both the LcType and the IpAddressType if the eviction thread
        // has not run. Triggering the eviction thread will evict the oldest type (the LcType) leaving just
        // the IpAddressType. This sets the condition for creating a new instance of the LcType below.
        factory.cleanup();

        // creating a new LcType should return a new instance due to the low cache size
        Type<?> typeFour = factory.createType(LcType.class.getName());
        assertNotSame(typeOne, typeFour);

        // trigger maintenance tasks (i.e., eviction of old entries)
        factory.cleanup();

        assertEquals(1, factory.getCacheSize());
    }

    @Test
    public void testAllTypesAllFactories() {
        // AbstractGeometryType, BaseType and ListType are technically all abstract types and cannot be created

        //  @formatter:off
        List<String> typeClassNames = List.of(DateType.class.getName(),
                        GeoLatType.class.getName(),
                        GeoLonType.class.getName(),
                        GeometryType.class.getName(),
                        GeoType.class.getName(),
                        HexStringType.class.getName(),
                        HitTermType.class.getName(),
                        IpAddressType.class.getName(),
                        IpV4AddressType.class.getName(),
                        LcNoDiacriticsListType.class.getName(),
                        LcNoDiacriticsType.class.getName(),
                        LcType.class.getName(),
                        MacAddressType.class.getName(),
                        NoOpType.class.getName(),
                        NumberListType.class.getName(),
                        NumberType.class.getName(),
                        PointType.class.getName(),
                        RawDateType.class.getName(),
                        StringType.class.getName(),
                        TrimLeadingZerosType.class.getName());
        //  @formatter:on

        for (String typeClassName : typeClassNames) {
            assertTypeCreation(typeClassName);
        }

        assertEquals(20, typeFactory.getCacheSize());
    }

    /**
     * Assert that the same Type is created via the internal {@link Type.Factory} and the {@link TypeFactory}.
     * <p>
     * Also asserts that multiple calls to {@link TypeFactory#createType(String)} return the same instance.
     *
     * @param typeClassName
     *            the class name for a Type
     */
    private void assertTypeCreation(String typeClassName) {
        Type<?> internalCreate = Type.Factory.createType(typeClassName);

        Type<?> factoryCreateOne = typeFactory.createType(typeClassName);
        Type<?> factoryCreateTwo = typeFactory.createType(typeClassName);

        assertSame(factoryCreateOne, factoryCreateTwo, "TypeFactory should have returned the same instance");

        assertNotSame(internalCreate, factoryCreateOne, "Type.Factory and TypeFactory should have returned different instances");
        assertNotSame(internalCreate, factoryCreateTwo, "Type.Factory and TypeFactory should have returned different instances");
    }

}
