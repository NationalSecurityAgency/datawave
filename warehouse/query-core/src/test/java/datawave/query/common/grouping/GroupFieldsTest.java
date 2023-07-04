package datawave.query.common.grouping;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

public class GroupFieldsTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Multimap<String,String> inverseReverseModel = HashMultimap.create();

    @BeforeClass
    public static void beforeClass() {
        inverseReverseModel.put("GEN", "GENERE");
        inverseReverseModel.put("GEN", "GENDER");
        inverseReverseModel.put("AG", "AGE");
        inverseReverseModel.put("NOME", "NAME");
    }

    @Test
    public void testEmptyGroupFieldsToString() {
        GroupFields groupFields = new GroupFields();
        assertThat(groupFields.toString()).isEmpty();
    }

    @Test
    public void testGroupFieldsToString() {
        GroupFields groupFields = new GroupFields();
        groupFields.setGroupByFields(Sets.newHashSet("A", "1"));
        groupFields.setSumFields(Sets.newHashSet("B", "2"));
        groupFields.setCountFields(Sets.newHashSet("C", "3"));
        groupFields.setAverageFields(Sets.newHashSet("D", "4"));
        groupFields.setMinFields(Sets.newHashSet("E", "5"));
        groupFields.setMaxFields(Sets.newHashSet("F", "6"));

        assertThat(groupFields.toString()).isEqualTo("GROUP(A,1)|SUM(B,2)|COUNT(C,3)|AVERAGE(D,4)|MIN(E,5)|MAX(F,6)");
    }

    @Test
    public void testRemappedGroupFieldsToString() {
        GroupFields groupFields = new GroupFields();
        groupFields.setGroupByFields(Sets.newHashSet("AG", "GEN"));
        groupFields.setSumFields(Sets.newHashSet("AG"));
        groupFields.setCountFields(Sets.newHashSet("NOME"));
        groupFields.setAverageFields(Sets.newHashSet("AG"));
        groupFields.setMinFields(Sets.newHashSet("GEN"));
        groupFields.setMaxFields(Sets.newHashSet("NOME"));

        groupFields.remapFields(inverseReverseModel);

        assertThat(groupFields.toString()).isEqualTo(
                        "GROUP(GENERE,GEN,AG,GENDER,AGE)|SUM(AG,AGE)|COUNT(NOME,NAME)|AVERAGE(AG,AGE)|MIN(GENERE,GEN,GENDER)|MAX(NOME,NAME)|MODEL_MAP(AG[AGE]:GEN[GENERE,GENDER]:NOME[NAME])");
    }

    @Test
    public void testParsingFromNullString() {
        assertThat(GroupFields.from(null)).isNull();
    }

    @Test
    public void testParsingFromEmptyString() {
        assertThat(GroupFields.from("")).isEqualTo(new GroupFields());
    }

    @Test
    public void testParsingFromWhitespace() {
        assertThat(GroupFields.from("   ")).isEqualTo(new GroupFields());
    }

    @Test
    public void testParsingGroupFieldsWithGroupByFieldsOnly() {
        GroupFields expected = new GroupFields();
        expected.setGroupByFields(Sets.newHashSet("AGE", "GENDER"));

        GroupFields actual = GroupFields.from("GROUP(AGE,GENDER)");

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testParsingGroupFieldsWithSomeAggregationFields() {
        GroupFields expected = new GroupFields();
        expected.setGroupByFields(Sets.newHashSet("AGE", "GENDER"));
        expected.setSumFields(Sets.newHashSet("AGE"));
        expected.setMaxFields(Sets.newHashSet("NAME"));

        GroupFields actual = GroupFields.from("GROUP(AGE,GENDER)|SUM(AGE)|MAX(NAME)");

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testParsingGroupFieldsWithAllAggregationFields() {
        GroupFields expected = new GroupFields();
        expected.setGroupByFields(Sets.newHashSet("AGE", "GENDER"));
        expected.setSumFields(Sets.newHashSet("BAT"));
        expected.setCountFields(Sets.newHashSet("FOO"));
        expected.setAverageFields(Sets.newHashSet("BAR"));
        expected.setMinFields(Sets.newHashSet("HAT"));
        expected.setMaxFields(Sets.newHashSet("BAH"));

        GroupFields actual = GroupFields.from("GROUP(AGE,GENDER)|SUM(BAT)|COUNT(FOO)|AVERAGE(BAR)|MIN(HAT)|MAX(BAH)");

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testParsingRemappedGroupFields() {
        GroupFields expected = new GroupFields();
        expected.setGroupByFields(Sets.newHashSet("AG"));
        expected.setSumFields(Sets.newHashSet("AG"));
        expected.setCountFields(Sets.newHashSet("NOME"));
        expected.setAverageFields(Sets.newHashSet("BAR"));
        expected.setMinFields(Sets.newHashSet("BAT"));
        expected.setMaxFields(Sets.newHashSet("FOO"));
        expected.remapFields(inverseReverseModel);

        GroupFields actual = GroupFields
                        .from("GROUP(AG,AGE)|SUM(AG,AGE)|COUNT(NOME,NAME)|AVERAGE(BAR)|MIN(BAT)|MAX(FOO)|MODEL_MAP(AG[AGE]:GEN[GENERE,GENDER]:NOME[NAME])");

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testParsingLegacyFormat() {
        GroupFields expected = new GroupFields();
        expected.setGroupByFields(Sets.newHashSet("AGE", "GENDER", "NAME"));

        GroupFields actual = GroupFields.from("AGE,GENDER,NAME");

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testDeconstructIdentifiers() {
        GroupFields groupFields = new GroupFields();
        groupFields.setGroupByFields(Sets.newHashSet("$AGE", "$GENDER"));
        groupFields.setSumFields(Sets.newHashSet("$AGE", "$GENDER"));
        groupFields.setCountFields(Sets.newHashSet("$AGE", "$GENDER"));
        groupFields.setAverageFields(Sets.newHashSet("$AGE", "$GENDER"));
        groupFields.setMinFields(Sets.newHashSet("$AGE", "$GENDER"));
        groupFields.setMaxFields(Sets.newHashSet("$AGE", "$GENDER"));

        groupFields.deconstructIdentifiers();

        assertThat(groupFields.getGroupByFields()).containsExactlyInAnyOrder("AGE", "GENDER");
        assertThat(groupFields.getSumFields()).containsExactlyInAnyOrder("AGE", "GENDER");
        assertThat(groupFields.getCountFields()).containsExactlyInAnyOrder("AGE", "GENDER");
        assertThat(groupFields.getMinFields()).containsExactlyInAnyOrder("AGE", "GENDER");
        assertThat(groupFields.getMaxFields()).containsExactlyInAnyOrder("AGE", "GENDER");
        assertThat(groupFields.getAverageFields()).containsExactlyInAnyOrder("AGE", "GENDER");
    }

    @Test
    public void testRemapFields() {
        GroupFields groupFields = new GroupFields();
        groupFields.setGroupByFields(Sets.newHashSet("AG", "GEN"));
        groupFields.setSumFields(Sets.newHashSet("AG"));
        groupFields.setCountFields(Sets.newHashSet("NOME"));
        groupFields.setAverageFields(Sets.newHashSet("AG"));
        groupFields.setMinFields(Sets.newHashSet("GEN"));
        groupFields.setMaxFields(Sets.newHashSet("NOME"));

        groupFields.remapFields(inverseReverseModel);

        assertThat(groupFields.getGroupByFields()).containsExactlyInAnyOrder("GENERE", "GEN", "GENDER", "AG", "AGE");
        assertThat(groupFields.getSumFields()).containsExactlyInAnyOrder("AG", "AGE");
        assertThat(groupFields.getCountFields()).containsExactlyInAnyOrder("NOME", "NAME");
        assertThat(groupFields.getAverageFields()).containsExactlyInAnyOrder("AG", "AGE");
        assertThat(groupFields.getMinFields()).containsExactlyInAnyOrder("GENERE", "GEN", "GENDER");
        assertThat(groupFields.getMaxFields()).containsExactlyInAnyOrder("NOME", "NAME");
        assertThat(groupFields.getModelMap()).isEqualTo(inverseReverseModel);
    }

    @Test
    public void testSerialization() throws JsonProcessingException {
        GroupFields groupFields = new GroupFields();
        groupFields.setGroupByFields(Sets.newHashSet("AG", "GEN"));
        groupFields.setSumFields(Sets.newHashSet("AG"));
        groupFields.setCountFields(Sets.newHashSet("NOME"));
        groupFields.setAverageFields(Sets.newHashSet("AG"));
        groupFields.setMinFields(Sets.newHashSet("GEN"));
        groupFields.setMaxFields(Sets.newHashSet("NOME"));

        groupFields.remapFields(inverseReverseModel);

        String json = objectMapper.writeValueAsString(groupFields);
        assertThat(json).isEqualTo(
                        "\"GROUP(GENERE,GEN,AG,GENDER,AGE)|SUM(AG,AGE)|COUNT(NOME,NAME)|AVERAGE(AG,AGE)|MIN(GENERE,GEN,GENDER)|MAX(NOME,NAME)|MODEL_MAP(AG[AGE]:GEN[GENERE,GENDER]:NOME[NAME])\"");
    }

    @Test
    public void testDeserialization() throws JsonProcessingException {
        GroupFields expected = new GroupFields();
        expected.setGroupByFields(Sets.newHashSet("AG", "GEN"));
        expected.setSumFields(Sets.newHashSet("AG"));
        expected.setCountFields(Sets.newHashSet("NOME"));
        expected.setAverageFields(Sets.newHashSet("AG"));
        expected.setMinFields(Sets.newHashSet("GEN"));
        expected.setMaxFields(Sets.newHashSet("NOME"));
        expected.remapFields(inverseReverseModel);

        String json = "\"GROUP(GENERE,GEN,AG,GENDER,AGE)|SUM(AG,AGE)|COUNT(NOME,NAME)|AVERAGE(AG,AGE)|MIN(GENERE,GEN,GENDER)|MAX(NOME,NAME)|MODEL_MAP(AG[AGE]:GEN[GENERE,GENDER]:NOME[NAME])\"";
        GroupFields actual = objectMapper.readValue(json, GroupFields.class);

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testGetFieldAggregatorFactory() {
        GroupFields groupFields = new GroupFields();
        groupFields.setGroupByFields(Sets.newHashSet("AGE", "GENDER"));
        groupFields.setSumFields(Sets.newHashSet("AGE"));
        groupFields.setCountFields(Sets.newHashSet("NAME"));
        groupFields.setAverageFields(Sets.newHashSet("HEIGHT"));
        groupFields.setMinFields(Sets.newHashSet("SALARY"));
        groupFields.setMaxFields(Sets.newHashSet("RANK"));

        // @formatter:off
        FieldAggregator.Factory expected = new FieldAggregator.Factory().withSumFields("AGE")
                        .withCountFields("NAME")
                        .withAverageFields("HEIGHT")
                        .withMinFields("SALARY")
                        .withMaxFields("RANK");
        // @formatter:on

        assertThat(groupFields.getFieldAggregatorFactory()).isEqualTo(expected);
    }
}
