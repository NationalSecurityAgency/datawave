package datawave.query.common.grouping;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.Test;

public class GroupAggregateFieldsTest {
    
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
    public void testDeconstructIdentifiers() {
        GroupAggregateFields groupAggregateFields = new GroupAggregateFields();
        groupAggregateFields.addGroupFields("$GENDER", "$AGE");
        groupAggregateFields.addSumFields("$GENDER", "$AGE");
        groupAggregateFields.addCountFields("$GENDER", "$AGE");
        groupAggregateFields.addAverageFields("$GENDER", "$AGE");
        groupAggregateFields.addMinFields("$GENDER", "$AGE");
        groupAggregateFields.addMaxFields("$GENDER", "$AGE");
        
        groupAggregateFields.deconstructIdentifiers();
        
        Multimap<String,String> expectedMap = TreeMultimap.create();
        expectedMap.put("GENDER", "GENDER");
        expectedMap.put("AGE", "AGE");
        
        Assertions.assertThat(groupAggregateFields.getGroupFieldsMap()).isEqualTo(expectedMap);
        Assertions.assertThat(groupAggregateFields.getSumFields()).isEqualTo(expectedMap);
        Assertions.assertThat(groupAggregateFields.getCountFields()).isEqualTo(expectedMap);
        Assertions.assertThat(groupAggregateFields.getAverageFields()).isEqualTo(expectedMap);
        Assertions.assertThat(groupAggregateFields.getMinFields()).isEqualTo(expectedMap);
        Assertions.assertThat(groupAggregateFields.getMaxFields()).isEqualTo(expectedMap);
    }
    
    @Test
    public void testRemapFields() {
        GroupAggregateFields groupAggregateFields = new GroupAggregateFields();
        groupAggregateFields.addGroupFields("GENDER", "AGE");
        groupAggregateFields.addSumFields("AGE");
        groupAggregateFields.addCountFields("GENDER");
        groupAggregateFields.addAverageFields("AGE");
        groupAggregateFields.addMaxFields("GENDER");
        groupAggregateFields.addMinFields("GENDER", "AGE");
        
        groupAggregateFields.remapFields(inverseReverseModel);
        
        Multimap<String,String> remappedAgeFields = TreeMultimap.create();
        remappedAgeFields.put("AG", "AGE");
        
        Multimap<String,String> remappedGenderFields = TreeMultimap.create();
        remappedGenderFields.put("GEN", "GENERE");
        remappedGenderFields.put("GEN", "GENDER");
        
        Multimap<String,String> remappedAgeAndGenderFields = TreeMultimap.create();
        remappedAgeAndGenderFields.putAll(remappedAgeFields);
        remappedAgeAndGenderFields.putAll(remappedGenderFields);
        
        Assertions.assertThat(groupAggregateFields.getGroupFieldsMap()).isEqualTo(remappedAgeAndGenderFields);
        Assertions.assertThat(groupAggregateFields.getSumFields()).isEqualTo(remappedAgeFields);
        Assertions.assertThat(groupAggregateFields.getAverageFields()).isEqualTo(remappedAgeFields);
        Assertions.assertThat(groupAggregateFields.getCountFields()).isEqualTo(remappedGenderFields);
        Assertions.assertThat(groupAggregateFields.getMaxFields()).isEqualTo(remappedGenderFields);
        Assertions.assertThat(groupAggregateFields.getMinFields()).isEqualTo(remappedAgeAndGenderFields);
    }
    
    @Test
    public void testEmptyGroupFieldsToString() {
        GroupAggregateFields groupAggregateFields = new GroupAggregateFields();
        Assertions.assertThat(groupAggregateFields.toString()).isEqualTo("");
    }
    
    @Test
    public void testSerialization() throws JsonProcessingException {
        GroupAggregateFields groupAggregateFields = new GroupAggregateFields();
        groupAggregateFields.addGroupFields("GENDER");
        groupAggregateFields.addSumFields("AGE");
        groupAggregateFields.addCountFields("GENDER");
        groupAggregateFields.addAverageFields("AGE");
        groupAggregateFields.addMaxFields("GENDER");
        groupAggregateFields.addMinFields("NAME");
        
        String json = objectMapper.writeValueAsString(groupAggregateFields);
        Assertions.assertThat(json).isEqualTo(
                        "\"GROUP(GENDER[GENDER]):SUM(AGE[AGE]):COUNT(GENDER[GENDER]):AVERAGE(AGE[AGE]):MIN(NAME[NAME]):MAX(GENDER[GENDER])\"");
    }
    
    @Test
    public void testNonEmptyUnmappedGroupFieldsToString() {
        GroupAggregateFields groupAggregateFields = new GroupAggregateFields();
        groupAggregateFields.addGroupFields("GENDER");
        groupAggregateFields.addSumFields("AGE");
        groupAggregateFields.addCountFields("GENDER");
        groupAggregateFields.addAverageFields("AGE");
        groupAggregateFields.addMaxFields("GENDER");
        groupAggregateFields.addMinFields("NAME");
        
        Assertions.assertThat(groupAggregateFields.toString()).isEqualTo(
                        "GROUP(GENDER[GENDER]):SUM(AGE[AGE]):COUNT(GENDER[GENDER]):AVERAGE(AGE[AGE]):MIN(NAME[NAME]):MAX(GENDER[GENDER])");
    }
    
    @Test
    public void testNonEmptyRemappedGroupFieldsToString() {
        GroupAggregateFields groupAggregateFields = new GroupAggregateFields();
        groupAggregateFields.addGroupFields("GENDER");
        groupAggregateFields.addSumFields("AGE");
        groupAggregateFields.addCountFields("GENDER");
        groupAggregateFields.addAverageFields("AGE");
        groupAggregateFields.addMaxFields("GENDER");
        groupAggregateFields.addMinFields("NAME");
        
        groupAggregateFields.remapFields(inverseReverseModel);
        
        Assertions.assertThat(groupAggregateFields.toString()).isEqualTo(
                        "GROUP(GEN[GENDER,GENERE]):SUM(AG[AGE]):COUNT(GEN[GENDER,GENERE]):AVERAGE(AG[AGE]):MIN(NOME[NAME]):MAX(GEN[GENDER,GENERE])");
    }
    
    @Test
    public void testParsingFromNullString() {
        Assertions.assertThat(GroupAggregateFields.from(null)).isNull();
    }
    
    @Test
    public void testParsingFromEmptyString() {
        Assertions.assertThat(GroupAggregateFields.from("")).isEqualTo(new GroupAggregateFields());
    }
    
    @Test
    public void testParsingFromNonEmptyString() {
        GroupAggregateFields expected = new GroupAggregateFields();
        expected.addGroupFields("GENDER");
        expected.addSumFields("AGE");
        expected.addCountFields("GENDER");
        expected.addAverageFields("AGE");
        expected.addMaxFields("GENDER");
        expected.addMinFields("GENDER", "AGE");
        expected.remapFields(inverseReverseModel);
        
        GroupAggregateFields parsed = GroupAggregateFields.from(expected.toString());
        
        Assertions.assertThat(parsed).isEqualTo(expected);
    }
}
