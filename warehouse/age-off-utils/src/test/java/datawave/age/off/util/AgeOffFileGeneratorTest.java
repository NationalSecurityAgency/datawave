package datawave.age.off.util;

import static datawave.age.off.util.AnyXmlElement.toJAXBElement;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import javax.xml.bind.JAXBElement;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.junit.Test;
import org.w3c.dom.Node;

import datawave.ingest.util.cache.watch.AgeOffRuleLoader;
import datawave.ingest.util.cache.watch.TestFilter;
import datawave.iterators.filter.ColumnVisibilityLabeledFilter;
import datawave.iterators.filter.ageoff.AppliedRule;
import datawave.iterators.filter.ageoff.ConfigurableIteratorEnvironment;
import datawave.iterators.filter.ageoff.FilterRule;

public class AgeOffFileGeneratorTest {
    private static final String PARENT_FILE_NAME = "alternate-root.xml";

    // @formatter:off
    private static final String EXPECTED_FILE_CONTENTS =
        "<ageoffConfiguration>\n" +
        "     <parent>" + PARENT_FILE_NAME + "</parent>\n" +
        "     <rules>\n" +
        "          <rule label=\"labeledPatternsFormat\" mode=\"merge\">\n" +
        "               <filterClass>datawave.iterators.filter.ColumnVisibilityLabeledFilter</filterClass>\n" +
        "               <ismerge>true</ismerge>\n" +
        "               <matchPattern>\n" +
        "                    dryFood bakingPowder=365d\n" +
        "                    dryFood driedBeans=548d\n" +
        "                    dryFood bakingSoda=720d\n" +
        "                    dryFood coffeeGround=90d\n" +
        "                    dryFood coffeeWholeBean=183d\n" +
        "                    dryFood coffeeInstant=730d\n" +
        "               </matchPattern>\n" +
        "          </rule>\n" +
        "          <rule>\n" +
        "          \t<filterClass>datawave.iterators.filter.ageoff.DataTypeAgeOffFilter</filterClass>\n" +
        "          \t<ttl units=\"d\">720</ttl>\n" +
        "          \t<datatypes>foo,bar</datatypes>\n" +
        "          \t<bar.ttl>44</bar.ttl>\n" +
        "          </rule>\n" +
        "          <rule>\n" +
        "               <filterClass>datawave.ingest.util.cache.watch.TestFilter</filterClass>\n" +
        "               <ttl units=\"ms\">10</ttl>\n" +
        "               <matchPattern>1</matchPattern>\n" +
        "               <myTagName ttl=\"1234\"></myTagName>\n" +
        "               <filtersWater>false</filtersWater>\n" +
        "          </rule>\n" +
        "     </rules>\n" +
        "</ageoffConfiguration>";

    private static final String OTHER_EXPECTED_FILE_CONTENTS =
            "<ageoffConfiguration>\n" +
            "     <parent>test-root-field.xml</parent>\n" +
            "     <rules>\n" +
            "          <rule mode=\"merge\">\n" +
            "            <filterClass>datawave.iterators.filter.ageoff.DataTypeAgeOffFilter</filterClass>\n" +
            "            <ismerge>true</ismerge>\n" +
            "            <isindextable>true</isindextable>\n" +
            "          </rule>\n" +
            "          <rule>\n" +
            "            <filterClass>datawave.iterators.filter.ageoff.FieldAgeOffFilter</filterClass>\n" +
            "            <ttl units=\"s\">5</ttl>\n" +
            "            <isindextable>true</isindextable>\n" +
            "            <fields>field_y,field_z</fields>\n" +
            "            <field_y.ttl>1</field_y.ttl>\n" +
            "            <field_z.ttl>2</field_z.ttl>\n" +
            "          </rule>\n" +
            "          <rule label=\"edge\">\n" +
            "               <filterClass>datawave.iterators.filter.EdgeColumnQualifierTokenFilter</filterClass>\n" +
            "               <matchPattern>\n" +
            "                    \"egg\" : 10d\n" +
            "                    \"chicken\" : 10d\n" +
            "                    \"ham\" : 10d\n" +
            "                    \"tunaSalad\" : 10d\n" +
            "                    \"macaroni\" : 10d\n" +
            "                    \n" +
            "                    <!-- Meats-->\n" +
            "                    \"hotDogsOpened\" : 30d\n" +
            "                    \"hotDogsUnopened\" : 60d\n" +
            "                    \"luncheonOpened\" : 14d\n" +
            "                    \"luncheonDeliSliced\" : 14d\n" +
            "                    \"luncheonUnopened\" : 60d\n" +
            "                    \"bacon\" : 30d\n" +
            "                    \"rawChickenSausage\" : 7d\n" +
            "                    \"rawTurkeySausage\" : 7d\n" +
            "                    \"rawPorkSausage\" : 7d\n" +
            "                    \"rawBeefSausage\" : 7d\n" +
            "                    \"cookedChickenSausage\" : 30d\n" +
            "                    \"cookedTurkeySausage\" : 30d\n" +
            "                    \"cookedPorkSausage\" : 30d\n" +
            "                    \"cookedBeefSausage\" : 30d\n" +
            "                    \"frozenSausageAfterCooking\" : 10d\n" +
            "                    \"hamburger\" : 7d\n" +
            "                    \"groundBeef\" : 7d\n" +
            "                    \"turkey\" : 7d\n" +
            "                    \"groundChicken\" : 7d\n" +
            "                    \"otherPoultry\" : 7d\n" +
            "                    \"veal\" : 7d\n" +
            "                    \"pork\" : 7d\n" +
            "                    \"lamb\" : 7d\n" +
            "                    \"mixturesOfOtherGroundMeats\" : 7d\n" +
            "                    \"steaks\" : 14d\n" +
            "                    \"chops\" : 14d\n" +
            "                    \"roasts\" : 14d\n" +
            "                    \"freshUncuredUncooked\" : 14d\n" +
            "                    \"freshUncuredCooked\" : 10d\n" +
            "                    \"curedUncooked\" : 30d\n" +
            "                    \"unopenedCookedAndSealedAtPlant\" : 60d\n" +
            "                    \"cookedStoreWrappedWhole\" : 30d\n" +
            "                    \"cookedStoreWrappedCut\" : 14d\n" +
            "                    \"cookedCountryHam\" : 30d\n" +
            "                    \"cannedUnopenedLabeledKeepRefrigerated\" : 0s\n" +
            "                    \"cannedOpenedShelfStable\" : 10d\n" +
            "                    \"unopenedShelfStableCannedAtRoomTemperature\" : 1826d\n" +
            "                    \"prosciutto\" : 0s\n" +
            "                    \"parma\" : 0s\n" +
            "                    \"sarrano\" : 0s\n" +
            "                    \"dryItalian\" : 0s\n" +
            "                    \"spanishType\" : 0s\n" +
            "                    \"cut\" : 0s\n" +
            "                    \"wholeChicken\" : 7d\n" +
            "                    \"wholeTurkey\" : 7d\n" +
            "                    \"chickenPieces\" : 7d\n" +
            "                    \"turkeyPieces\" : 7d\n" +
            "                    \"bluefish\" : 7d\n" +
            "                    \"catfish\" : 7d\n" +
            "                    \"mackerel\" : 7d\n" +
            "                    \"mullet\" : 7d\n" +
            "                    \"salmon\" : 7d\n" +
            "                    \"tuna\" : 7d\n" +
            "                    \"cod\" : 7d\n" +
            "                    \"flounder\" : 7d\n" +
            "                    \"haddock\" : 7d\n" +
            "                    \"halibut\" : 7d\n" +
            "                    \"sole\" : 7d\n" +
            "                    \"pollock\" : 7d\n" +
            "                    \"oceanPerch\" : 7d\n" +
            "                    \"rockfish\" : 7d\n" +
            "                    \"seaTrout\" : 7d\n" +
            "                    \"freshCrabMeat\" : 10d\n" +
            "                    \"freshLobster\" : 10d\n" +
            "                    \"liveCrab\" : 3d\n" +
            "                    \"liveLobster\" : 3d\n" +
            "                    \"scallops\" : 42d\n" +
            "                    \"shrimp\" : 14d\n" +
            "                    \"crayfish\" : 14d\n" +
            "                    \"shuckedClams\" : 42d\n" +
            "                    \"mussels\" : 42d\n" +
            "                    \"oysters\" : 42d\n" +
            "                    \"squid\" : 7d\n" +
            "                    \n" +
            "                    <!-- Other-->\n" +
            "                    \"rawEggInShell\" : 365d\n" +
            "                    \"rawEggWhitesAndYolks\" : 10d\n" +
            "                    \"eggFrozenInShell\" : 0s\n" +
            "                    \"eggInBrokenShell\" : 0s\n" +
            "                    \"hardCookedEggs\" : 30d\n" +
            "                    \"unopenedEggSubstitutes\" : 30d\n" +
            "                    \"openedEggSubstitutes\" : 7d\n" +
            "                    \"unfrozenUnopenedEggSubstitutes\" : 30d\n" +
            "                    \"unfrozenOpenedEggSubstitutes\" : 10d\n" +
            "                    \"eggCasseroles\" : 10d\n" +
            "                    \"commercialEggNog\" : 14d\n" +
            "                    \"homemadeEggNog\" : 10d\n" +
            "                    \"bakedPumpkinPies\" : 10d\n" +
            "                    \"bakedPecanPies\" : 10d\n" +
            "                    \"bakedCustardPies\" : 10d\n" +
            "                    \"bakedChiffonPies\" : 10d\n" +
            "                    \"quicheWithFilling\" : 14d\n" +
            "                    \"vegetableOrMeatAdded\" : 10d\n" +
            "                    \"cookedMeat\" : 10d\n" +
            "                    \"cookedPoultry\" : 10d\n" +
            "                    \"chickenNuggets\" : 10d\n" +
            "                    \"chickenPatties\" : 10d\n" +
            "                    \"pizza\" : 10d\n" +
            "               </matchPattern>\n" +
            "          </rule>\n" +
            "     </rules>\n" +
            "</ageoffConfiguration>";
    // @formatter:on

    @Test
    public void createFileWithTwoRules() throws IOException {
        AgeOffRuleConfiguration.Builder colVisFilterRule = defineColVisFilterRule();
        AgeOffRuleConfiguration.Builder testFilterRule = defineTestFilterRule();
        AgeOffRuleConfiguration.Builder dataTypeRule = defineDataTypeRule();

        AgeOffFileConfiguration.Builder builder = new AgeOffFileConfiguration.Builder();
        builder.withParentFile(PARENT_FILE_NAME);
        builder.addNextRule(colVisFilterRule);
        builder.addNextRule(dataTypeRule);
        builder.addNextRule(testFilterRule);
        builder.withIndentation("     ");
        assertEquals(EXPECTED_FILE_CONTENTS, generateFile(builder));
    }

    @Test
    public void createFileWithThreeRules() throws IOException, URISyntaxException {

        AgeOffRuleConfiguration.Builder dataTypeIndexTableRule = defineDataTypeRuleIndexTable();
        AgeOffRuleConfiguration.Builder fieldRule = defineFieldAgeOffRule();

        AgeOffFileConfiguration.Builder builder = new AgeOffFileConfiguration.Builder();
        builder.withParentFile("test-root-field.xml");
        builder.addNextRule(dataTypeIndexTableRule);
        builder.addNextRule(fieldRule);
        builder.addNextRule(defineColQualifierRule());
        builder.withIndentation("     ");
        assertEquals(OTHER_EXPECTED_FILE_CONTENTS, generateFile(builder));
    }

    @Test
    public void fileLoadable() throws IOException, InvocationTargetException, NoSuchMethodException {
        AgeOffRuleLoader.AgeOffFileLoaderDependencyProvider provider = new TestProvider();

        List<FilterRule> rules = loadAgeOffFilterFile(provider, EXPECTED_FILE_CONTENTS);
        assertEquals(3, rules.size());
        rulesRunWithoutErrors(rules);

        List<FilterRule> otherRules = loadAgeOffFilterFile(provider, OTHER_EXPECTED_FILE_CONTENTS);
        // inherited a rule from parent
        assertEquals(4, otherRules.size());
        rulesRunWithoutErrors(rules);
    }

    private void rulesRunWithoutErrors(List<FilterRule> rules) {
        Key key = new Key();
        key.setTimestamp(System.currentTimeMillis() + 1000000L);
        for (FilterRule filterRule : rules) {
            boolean result = ((AppliedRule) filterRule).accept(key, new Value());
            if (filterRule.getClass() == TestFilter.class) {
                assertFalse(filterRule.toString(), result);
            } else {
                assertTrue(filterRule.toString(), result);
            }
        }
    }

    private List<FilterRule> loadAgeOffFilterFile(AgeOffRuleLoader.AgeOffFileLoaderDependencyProvider provider, String fileContents)
                    throws IOException, InvocationTargetException, NoSuchMethodException {
        return new AgeOffRuleLoader(provider).load(toInputStream(fileContents));
    }

    private ByteArrayInputStream toInputStream(String fileContents) {
        return new ByteArrayInputStream(fileContents.getBytes(StandardCharsets.UTF_8));
    }

    private AgeOffRuleConfiguration.Builder defineTestFilterRule() {
        AgeOffRuleConfiguration.Builder builder = new AgeOffRuleConfiguration.Builder();
        builder.withFilterClass(datawave.ingest.util.cache.watch.TestFilter.class);

        String duration = "10";
        String units = "ms";
        builder.withTtl(duration, units);

        builder.addCustomElement(new JAXBElement<>(QName.valueOf("matchPattern"), String.class, "1"));

        AnyXmlElement any = new AnyXmlElement();
        any.addAttribute(new QName("ttl"), "1234");
        builder.addCustomElement(toJAXBElement(new QName("myTagName"), any));

        builder.addCustomElement(new JAXBElement<>(QName.valueOf("filtersWater"), Boolean.class, Boolean.FALSE));
        return builder;
    }

    private AgeOffRuleConfiguration.Builder defineColVisFilterRule() {
        AgeOffCsvToMatchPatternFormatterConfiguration.Builder patternBuilder = new AgeOffCsvToMatchPatternFormatterConfiguration.Builder();
        patternBuilder.useStaticLabel("dryFood");
        patternBuilder.setReader(new StringReader(AgeOffCsvToMatchPatternFormatterTest.INPUT_TEXT_WITHOUT_LABEL));

        AgeOffRuleConfiguration.Builder builder = new AgeOffRuleConfiguration.Builder();
        builder.withPatternConfigurationBuilder(patternBuilder);
        builder.withRuleLabel("labeledPatternsFormat");
        builder.withFilterClass(ColumnVisibilityLabeledFilter.class);
        builder.useMerge();
        return builder;
    }

    private AgeOffRuleConfiguration.Builder defineColQualifierRule() throws IOException, URISyntaxException {
        AgeOffCsvToMatchPatternFormatterConfiguration.Builder patternBuilder = new AgeOffCsvToMatchPatternFormatterConfiguration.Builder();
        patternBuilder.padEquivalencesWithSpace();
        patternBuilder.useColonForEquivalence();
        patternBuilder.quoteLiterals('"');
        AgeOffCsvToMatchPatternFormatterTest.setShelfLifeInputFile(patternBuilder);
        patternBuilder.useAgeOffMapping(new FridgeToFreezerMapping());

        AgeOffRuleConfiguration.Builder builder = new AgeOffRuleConfiguration.Builder();
        patternBuilder.disableLabel();
        builder.withPatternConfigurationBuilder(patternBuilder);
        builder.withRuleLabel("edge");
        builder.withFilterClass(datawave.iterators.filter.EdgeColumnQualifierTokenFilter.class);
        return builder;
    }

    private AgeOffRuleConfiguration.Builder defineDataTypeRule() {
        AgeOffRuleConfiguration.Builder builder = new AgeOffRuleConfiguration.Builder();
        builder.withIndentation("\t");
        builder.withFilterClass(datawave.iterators.filter.ageoff.DataTypeAgeOffFilter.class);

        String duration = "720";
        String units = "d";
        builder.withTtl(duration, units);

        builder.addCustomElement(new JAXBElement<>(QName.valueOf("datatypes"), String.class, "foo,bar"));

        builder.addCustomElement(new JAXBElement<>(QName.valueOf("bar.ttl"), Integer.class, 44));
        return builder;
    }

    private AgeOffRuleConfiguration.Builder defineDataTypeRuleIndexTable() {
        AgeOffRuleConfiguration.Builder builder = new AgeOffRuleConfiguration.Builder();
        builder.withIndentation("  ");
        builder.useMerge();
        builder.withFilterClass(datawave.iterators.filter.ageoff.DataTypeAgeOffFilter.class);

        builder.addCustomElement(new JAXBElement<>(QName.valueOf("isindextable"), Boolean.class, true));
        return builder;
    }

    private AgeOffRuleConfiguration.Builder defineFieldAgeOffRule() {
        AgeOffRuleConfiguration.Builder builder = new AgeOffRuleConfiguration.Builder();
        builder.withIndentation("  ");
        builder.withFilterClass(datawave.iterators.filter.ageoff.FieldAgeOffFilter.class);

        String duration = "5";
        String units = "s";
        builder.withTtl(duration, units);

        builder.addCustomElement(new JAXBElement<>(QName.valueOf("isindextable"), Boolean.class, true));

        builder.addCustomElement(new JAXBElement<>(QName.valueOf("fields"), String.class, "field_y,field_z"));

        builder.addCustomElement(new JAXBElement<>(QName.valueOf("field_y.ttl"), Integer.class, 1));
        builder.addCustomElement(new JAXBElement<>(QName.valueOf("field_z.ttl"), Integer.class, 2));
        return builder;
    }

    private String generateFile(AgeOffFileConfiguration.Builder builder) throws IOException {
        StringWriter out = new StringWriter();
        AgeOffFileGenerator generator = null;
        try {
            generator = new AgeOffFileGenerator(builder.build());
            generator.format(out);
        } catch (XMLStreamException e) {
            throw new IOException(e);
        }

        return out.toString();
    }

    private class TestProvider implements AgeOffRuleLoader.AgeOffFileLoaderDependencyProvider {
        @Override
        public IteratorEnvironment getIterEnv() {
            return new ConfigurableIteratorEnvironment();
        }

        @Override
        public InputStream getParentStream(Node parent) {
            return this.getClass().getResourceAsStream("/filter/" + parent.getTextContent());
        }
    }
}
