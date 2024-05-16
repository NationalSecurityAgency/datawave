package datawave.query.util.transformer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import datawave.data.type.DateType;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.LcType;
import datawave.data.type.NoOpType;
import datawave.data.type.NumberType;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.TypeAttribute;
import datawave.query.model.QueryModel;
import datawave.query.util.TypeMetadata;

class AttributeRebuilderTest {

    private static TypeMetadata typeMetadata;
    private static QueryModel queryModel;
    private static AttributeFactory attributeFactory;
    private final Key metadata = new Key("row", "dt\0uid");

    @BeforeAll
    public static void setup() {
        typeMetadata = new TypeMetadata();
        typeMetadata.put("A", "dt1", LcType.class.getTypeName());
        typeMetadata.put("B", "dt1", DateType.class.getTypeName());
        typeMetadata.put("C", "dt1", NumberType.class.getTypeName());

        // add a few fields with multiple types
        typeMetadata.put("D", "dt1", NoOpType.class.getTypeName());
        typeMetadata.put("D", "dt1", LcNoDiacriticsType.class.getTypeName());

        typeMetadata.put("E", "dt1", NoOpType.class.getTypeName());
        typeMetadata.put("E", "dt1", DateType.class.getTypeName());

        typeMetadata.put("F", "dt1", NoOpType.class.getTypeName());
        typeMetadata.put("F", "dt1", LcNoDiacriticsType.class.getTypeName());
        typeMetadata.put("F", "dt1", DateType.class.getTypeName());

        queryModel = new QueryModel();
        queryModel.addTermToModel("X", "A");
        queryModel.addTermToModel("Y", "B");
        queryModel.addTermToModel("Z", "C");
        queryModel.addTermToReverseModel("A", "X");
        queryModel.addTermToReverseModel("B", "Y");
        queryModel.addTermToReverseModel("C", "Z");

        attributeFactory = new AttributeFactory(typeMetadata);
    }

    @Test
    void testLcTypeRebuild() {
        Attribute<?> attr = createAttribute("FIELD", "value");
        assertTypes(attr, NoOpType.class.getTypeName());

        // rebuild as LcType
        Attribute<?> rebuilt = createRebuilder().rebuild("A", attr);
        assertTypes(rebuilt, LcType.class.getTypeName());
    }

    @Test
    void testDateTypeRebuild() {
        Attribute<?> attr = createAttribute("FIELD", "Thu Dec 28 00:00:05 GMT 2000");
        assertTypes(attr, NoOpType.class.getTypeName());

        // rebuild as DateType
        Attribute<?> rebuilt = createRebuilder().rebuild("B", attr);
        assertTypes(rebuilt, DateType.class.getTypeName());
    }

    @Test
    void testNumberTypeRebuild() {
        Attribute<?> attr = createAttribute("FIELD", "127");
        assertTypes(attr, NoOpType.class.getTypeName());

        // rebuild as NumberType
        Attribute<?> rebuilt = createRebuilder().rebuild("C", attr);
        assertTypes(rebuilt, NumberType.class.getTypeName());
    }

    @Test
    void testLcTypeRebuildViaModelReverseMapping() {
        Attribute<?> attr = createAttribute("FIELD", "value");
        assertTypes(attr, NoOpType.class.getTypeName());

        // rebuild as LcType
        Attribute<?> rebuilt = createRebuilder().rebuild("X", attr);
        assertTypes(rebuilt, LcType.class.getTypeName());
    }

    @Test
    void testDateTypeRebuildViaModelReverseMapping() {
        Attribute<?> attr = createAttribute("FIELD", "Thu Dec 28 00:00:05 GMT 2000");
        assertTypes(attr, NoOpType.class.getTypeName());

        // rebuild as DateType
        Attribute<?> rebuilt = createRebuilder().rebuild("Y", attr);
        assertTypes(rebuilt, DateType.class.getTypeName());
    }

    @Test
    void testNumberTypeRebuildViaModelReverseMapping() {
        Attribute<?> attr = createAttribute("FIELD", "127");
        assertTypes(attr, NoOpType.class.getTypeName());

        // rebuild as NumberType
        Attribute<?> rebuilt = createRebuilder().rebuild("Z", attr);
        assertTypes(rebuilt, NumberType.class.getTypeName());
    }

    @Test
    void testLcTypeRebuildWithGroupingEnabling() {
        Attribute<?> attr = createAttribute("FIELD", "value");
        assertTypes(attr, NoOpType.class.getTypeName());

        // rebuild as LcType
        Attribute<?> rebuilt = createRebuilder().rebuild("A.0", attr);
        assertTypes(rebuilt, LcType.class.getTypeName());
    }

    @Test
    void testLcTypeRebuildViaModelReverseMappingWithGroupingEnabled() {
        Attribute<?> attr = createAttribute("FIELD", "value");
        assertTypes(attr, NoOpType.class.getTypeName());

        // rebuild as LcType
        Attribute<?> rebuilt = createRebuilder().rebuild("X.0", attr);
        assertTypes(rebuilt, LcType.class.getTypeName());
    }

    @Test
    void testNumberTypeRebuildWithNullModel() {
        Attribute<?> attr = createAttribute("FIELD", "127");
        assertTypes(attr, NoOpType.class.getTypeName());

        // rebuild as NumberType
        AttributeRebuilder rebuilder = new AttributeRebuilder(typeMetadata, null);
        Attribute<?> rebuilt = rebuilder.rebuild("C", attr);
        assertTypes(rebuilt, NumberType.class.getTypeName());
    }

    @Test
    void testNumberTypeRebuildWithNullModelViaModelReverseMapping() {
        Attribute<?> attr = createAttribute("FIELD", "127");
        assertTypes(attr, NoOpType.class.getTypeName());

        // model required for building a NumberType, null model means the original attribute remains unchanged
        AttributeRebuilder rebuilder = new AttributeRebuilder(typeMetadata, null);
        Attribute<?> rebuilt = rebuilder.rebuild("X", attr);
        assertTypes(rebuilt, NoOpType.class.getTypeName());
    }

    @Test
    void testTwoTypesBothGeneral() {
        // given two normalizers of general types, one should remain
        Attribute<?> attr = createAttribute("FIELD", "127");
        assertTypes(attr, NoOpType.class.getTypeName());

        // model required for building a NumberType, null model means the original attribute remains unchanged
        AttributeRebuilder rebuilder = new AttributeRebuilder(typeMetadata, null);
        Attribute<?> rebuilt = rebuilder.rebuild("D", attr);
        assertTypes(rebuilt, LcNoDiacriticsType.class.getTypeName());
    }

    @Test
    void testTwoTypesOneGeneralOneSpecific() {
        // given two normalizers where one is a general type, the non-general type should remain
        Attribute<?> attr = createAttribute("FIELD", "Thu Dec 28 00:00:05 GMT 2000");
        assertTypes(attr, NoOpType.class.getTypeName());

        // model required for building a NumberType, null model means the original attribute remains unchanged
        AttributeRebuilder rebuilder = new AttributeRebuilder(typeMetadata, null);
        Attribute<?> rebuilt = rebuilder.rebuild("E", attr);
        assertTypes(rebuilt, DateType.class.getTypeName());
    }

    @Test
    void testThreeTypesTwoGeneralOneSpecific() {
        // given three normalizers, two of which are a general type, the most specific type should remain
        Attribute<?> attr = createAttribute("FIELD", "Thu Dec 28 00:00:05 GMT 2000");
        assertTypes(attr, NoOpType.class.getTypeName());

        // model required for building a NumberType, null model means the original attribute remains unchanged
        AttributeRebuilder rebuilder = new AttributeRebuilder(typeMetadata, null);
        Attribute<?> rebuilt = rebuilder.rebuild("F", attr);
        assertTypes(rebuilt, DateType.class.getTypeName());
    }

    private void assertTypes(Attribute<?> attribute, String expectedType) {
        assertFalse(attribute instanceof Attributes);
        assertInstanceOf(TypeAttribute.class, attribute);

        TypeAttribute<?> typedAttribute = (TypeAttribute<?>) attribute;
        Class<?> clazz = getClass(expectedType);
        assertEquals(clazz, typedAttribute.getType().getClass());
    }

    private Class<?> getClass(String name) {
        try {
            return Class.forName(name);
        } catch (ClassNotFoundException e) {
            fail("Could not find class with name: " + name);
            throw new RuntimeException(e);
        }
    }

    private AttributeRebuilder createRebuilder() {
        return new AttributeRebuilder(typeMetadata, queryModel);
    }

    private Attribute<?> createAttribute(String field, String value) {
        return attributeFactory.create(field, value, metadata, true);
    }
}
