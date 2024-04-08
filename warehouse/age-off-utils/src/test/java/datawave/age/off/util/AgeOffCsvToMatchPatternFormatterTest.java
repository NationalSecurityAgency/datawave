package datawave.age.off.util;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Test;

import datawave.age.off.util.AgeOffCsvToMatchPatternFormatterConfiguration.Builder;

public class AgeOffCsvToMatchPatternFormatterTest {
    private static final String SHELF_LIFE_FILE_IN = "/filter/shelf-life.csv";
    private static final String SHELF_LIFE_FRIDGE_FILE_OUT = "/filter/shelf-life.refrigerator.matchPattern";
    private static final String SHELF_LIFE_FILE_STATIC_LABEL = "/filter/shelf-life-static-label.refrigerator.matchPattern";
    private static final String SHELF_LIFE_FREEZER_FILE_OUT = "/filter/shelf-life.freezer.matchPattern";

    private static final String HEADER_WITH_LABEL = "label,pattern,duration";
    private static final String HEADER_WITHOUT_LABEL = "pattern,duration";

    // @formatter:off
    private static final String INPUT_TEXT =
            "bakingPowder, 365d\n" +
            "driedBeans,548d\n" +
            "bakingSoda,\t720d\n" +
            "      coffeeGround       ,        90d\n          " +
            "coffeeWholeBean        ,183d\n" +
            "         coffeeInstant,730d\n" +
            "twinkies," + Integer.MAX_VALUE + "d\n";
    // @formatter:on

    private static final String INPUT_TEXT_WITH_LABEL = HEADER_WITH_LABEL + "\n" + adjustEachLine(INPUT_TEXT, item -> "dryFood, " + item);
    public static final String INPUT_TEXT_WITHOUT_LABEL = HEADER_WITHOUT_LABEL + "\n" + INPUT_TEXT;

    @Test
    public void reformatsFile() throws IOException, URISyntaxException {
        Builder builder = new Builder().useColonForEquivalence().quoteLiterals('"').padEquivalencesWithSpace();
        setShelfLifeInputFile(builder);
        String expectedResult = readFileContents(SHELF_LIFE_FRIDGE_FILE_OUT);
        assertEquals(expectedResult, reformat(builder));
    }

    @Test
    public void appliesFreezerMapping() throws IOException, URISyntaxException {
        Builder builder = new Builder().useColonForEquivalence().quoteLiterals('"').padEquivalencesWithSpace();
        builder.useAgeOffMapping(new FridgeToFreezerMapping());
        setShelfLifeInputFile(builder);
        String expectedResult = readFileContents(SHELF_LIFE_FREEZER_FILE_OUT);
        assertEquals(expectedResult, reformat(builder));
    }

    @Test
    public void reformatsFileStaticLabel() throws IOException, URISyntaxException {
        Builder builder = new Builder().padEquivalencesWithSpace().useStaticLabel("foodStorage");
        setShelfLifeInputFile(builder);
        String expectedResult = readFileContents(SHELF_LIFE_FILE_STATIC_LABEL);

        assertEquals(expectedResult, reformat(builder));
    }

    @Test
    public void outputStaticLabelAndEquals() throws IOException {
        // @formatter:off
        String expectedOutputText =
                "dryFood bakingPowder=365d\n" +
                "dryFood driedBeans=548d\n"+
                "dryFood bakingSoda=720d\n"+
                "dryFood coffeeGround=90d\n"+
                "dryFood coffeeWholeBean=183d\n"+
                "dryFood coffeeInstant=730d\n" +
                "dryFood twinkies=2147483647d\n";
        // @formatter:on

        Builder builder = new Builder();
        builder.useStaticLabel("dryFood");
        assertEquals(expectedOutputText, reformat(builder, INPUT_TEXT_WITHOUT_LABEL));
    }

    @Test
    public void outputQuotedLiteralAndColon() throws IOException {
        // @formatter:off
        String expectedOutputText =
                "\"bakingPowder\" : 365d\n" +
                "\"driedBeans\" : 548d\n" +
                "\"bakingSoda\" : 720d\n" +
                "\"coffeeGround\" : 90d\n" +
                "\"coffeeWholeBean\" : 183d\n" +
                "\"coffeeInstant\" : 730d\n" +
                "\"twinkies\" : 2147483647d\n";
        // @formatter:on

        Builder builder = new Builder().quoteLiterals('"').padEquivalencesWithSpace().useColonForEquivalence();
        builder.disableLabel();
        assertEquals(expectedOutputText, reformat(builder, INPUT_TEXT_WITHOUT_LABEL));
    }

    @Test
    public void propagatesEmptyLines() throws IOException {
        // @formatter:off
        String inputText = HEADER_WITH_LABEL + "\n" +
                "dryFood,bakingPowder,365d\n" +
                "\n" +
                "\n" +
                "dryFood,driedBeans,548d";

        String expectedOutputText =
                "dryFood bakingPowder=365d\n" +
                "\n" +
                "\n" +
                "dryFood driedBeans=548d\n";
        // @formatter:on

        Builder builder = new Builder();
        assertEquals(expectedOutputText, reformat(builder, inputText));
    }

    @Test
    public void propagatesCommentedLines() throws IOException {
        // @formatter:off
        String inputText = HEADER_WITH_LABEL + "\n" +
                "dryFood,bakingPowder,365d\n" +
                "\n" +
                "# Beans are Legumes\n" +
                "dryFood,driedBeans,548d";

        String expectedOutputText =
                "dryFood bakingPowder=365d\n" +
                "\n" +
                "<!-- Beans are Legumes-->\n" +
                "dryFood driedBeans=548d\n";
        // @formatter:on

        Builder builder = new Builder();

        assertEquals(expectedOutputText, reformat(builder, inputText));
    }

    @Test
    public void toUpperCaseLiterals() throws IOException {
        // @formatter:off
        String expectedOutputText =
                "dryFood BAKINGPOWDER=365d\n" +
                "dryFood DRIEDBEANS=548d\n"+
                "dryFood BAKINGSODA=720d\n"+
                "dryFood COFFEEGROUND=90d\n"+
                "dryFood COFFEEWHOLEBEAN=183d\n"+
                "dryFood COFFEEINSTANT=730d\n" +
                "dryFood TWINKIES=" + Integer.MAX_VALUE + "d\n";
        // @formatter:on

        Builder builder = new Builder();
        builder.toUpperCaseLiterals();
        assertEquals(expectedOutputText, reformat(builder, INPUT_TEXT_WITH_LABEL));
    }

    @Test
    public void toLowerCaseLiterals() throws IOException {
        // @formatter:off
        String expectedOutputText =
                "dryFood bakingpowder=365d\n" +
                "dryFood driedbeans=548d\n"+
                "dryFood bakingsoda=720d\n"+
                "dryFood coffeeground=90d\n"+
                "dryFood coffeewholebean=183d\n"+
                "dryFood coffeeinstant=730d\n" +
                "dryFood twinkies=2147483647d\n";
        // @formatter:on

        Builder builder = new Builder();
        builder.toLowerCaseLiterals();
        assertEquals(expectedOutputText, reformat(builder, INPUT_TEXT_WITH_LABEL));
    }

    @Test
    public void ignoresExtraColumns() throws IOException {
        // @formatter:off
        String expectedOutputText =
                "dryFood bakingPowder=365d\n" +
                "dryFood driedBeans=548d\n"+
                "dryFood bakingSoda=720d\n"+
                "dryFood coffeeGround=90d\n"+
                "dryFood coffeeWholeBean=183d\n"+
                "dryFood coffeeInstant=730d\n" +
                "dryFood twinkies=2147483647d\n";
        // @formatter:on

        Builder builder = new Builder();

        String inputWithExtraColumns = adjustEachLine(INPUT_TEXT_WITH_LABEL, item -> item + ",extra,stuff");
        assertEquals(expectedOutputText, reformat(builder, inputWithExtraColumns));
    }

    @Test
    public void ignoresLabel() throws IOException {
        // @formatter:off
        String expectedOutputText =
                "bakingPowder=365d\n" +
                "driedBeans=548d\n"+
                "bakingSoda=720d\n"+
                "coffeeGround=90d\n"+
                "coffeeWholeBean=183d\n"+
                "coffeeInstant=730d\n" +
                "twinkies=2147483647d\n";
        // @formatter:on

        Builder builder = new Builder();
        builder.disableLabel();

        String inputWithExtraColumns = adjustEachLine(INPUT_TEXT_WITH_LABEL, item -> item + ",extra,stuff");
        assertEquals(expectedOutputText, reformat(builder, inputWithExtraColumns));
    }

    @Test
    public void appliesOverrideWhenConfigured() throws IOException {
        // @formatter:off

        // add an override for driedBeans from 548d to 365d
        String input = prepareInputWithOverride();

        String expectedOutputText =
                "bakingPowder=365d\n" +
                "driedBeans=365d\n"+
                "bakingSoda=720d\n"+
                "coffeeGround=90d\n"+
                "coffeeWholeBean=183d\n"+
                "coffeeInstant=730d\n" +
                "twinkies=2147483647d\n";
        // @formatter:on

        Builder builder = new Builder();
        builder.useOverrides();
        builder.disableLabel();
        assertEquals(expectedOutputText, reformat(builder, input));
    }

    @Test
    public void ignoresOverrideWhenNotConfigured() throws IOException {
        // @formatter:off

        // add an override for driedBeans from 548d to 365d
        String input = prepareInputWithOverride();

        String expectedOutputText =
                "bakingPowder=365d\n" +
                "driedBeans=548d\n"+
                "bakingSoda=720d\n"+
                "coffeeGround=90d\n"+
                "coffeeWholeBean=183d\n"+
                "coffeeInstant=730d\n" +
                "twinkies=2147483647d\n";
        // @formatter:on

        // deliberately not building to use overrides
        assertEquals(expectedOutputText, reformat(new Builder().disableLabel(), input));
    }

    @Test(expected = IllegalStateException.class)
    public void failsWithMissingPatternToken() throws IOException {
        // @formatter:off
        String input = HEADER_WITHOUT_LABEL + "\n" +
                "bakingPowder, 365d\n" +
                "driedBeans,548d\n" +
                "\t720d\n" + // missing pattern
                "      coffeeGround       ,        90d\n          " +
                "coffeeWholeBean        ,183d\n" +
                "         coffeeInstant,730d\n";;
        // @formatter:on

        reformat(new Builder(), input);
    }

    @Test(expected = IllegalStateException.class)
    public void failsWithEmptyPatternToken() throws IOException {
        // @formatter:off
        String input = HEADER_WITHOUT_LABEL + "\n" +
                "bakingPowder, 365d\n" +
                "driedBeans,548d\n" +
                ",\t720d\n" + // empty pattern
                "      coffeeGround       ,        90d\n          " +
                "coffeeWholeBean        ,183d\n" +
                "         coffeeInstant,730d\n";;
        // @formatter:on

        reformat(new Builder(), input);
    }

    @Test(expected = IllegalStateException.class)
    public void failsWithMissingLabelToken() throws IOException {
        // @formatter:off
        String input = HEADER_WITH_LABEL + "\n" +
                "lbl,bakingPowder, 365d\n" +
                "lbl,driedBeans,548d\n" +
                "\t720d\n" + // missing label
                "    lbl,  coffeeGround       ,        90d\n          " +
                "lbl,coffeeWholeBean        ,183d\n" +
                "    lbl,     coffeeInstant,730d\n";;
        // @formatter:on

        reformat(new Builder(), input);
    }

    @Test(expected = IllegalStateException.class)
    public void failsWithEmptyLabelToken() throws IOException {
        // @formatter:off
        String input = HEADER_WITH_LABEL + "\n" +
                "lbl,bakingPowder, 365d\n" +
                "lbl,driedBeans,548d\n" +
                "   ,   coffeeGround       ,        90d\n          " + // empty label
                "lbl,coffeeWholeBean        ,183d\n" +
                "    lbl,     coffeeInstant,730d\n";;
        // @formatter:on

        reformat(new Builder(), input);
    }

    @Test(expected = IllegalStateException.class)
    public void failsWithMissingDurationToken() throws IOException {
        // @formatter:off
        String input = HEADER_WITHOUT_LABEL + "\n" +
                "bakingPowder, 365d\n" +
                "driedBeans\n" + // missing duration
                "      coffeeGround       ,        90d\n          " +
                "coffeeWholeBean        ,183d\n" +
                "         coffeeInstant,730d\n";;
        // @formatter:on

        reformat(new Builder(), input);
    }

    @Test(expected = IllegalStateException.class)
    public void failsWithMissingHeaderDurationToken() throws IOException {
        // @formatter:off
        String input = "bakingPowder, 365d\n";
        // @formatter:on

        reformat(new Builder(), input);
    }

    @Test(expected = IllegalStateException.class)
    public void failsWithEmptyDurationToken() throws IOException {
        // @formatter:off
        String input = HEADER_WITHOUT_LABEL + "\n" +
                "bakingPowder, 365d\n" +
                "driedBeans,\n" + // empty duration
                "      coffeeGround       ,        90d\n          " +
                "coffeeWholeBean        ,183d\n" +
                "         coffeeInstant,730d\n";;
        // @formatter:on

        Builder builder = new Builder();

        reformat(builder, input);
    }

    private String prepareInputWithOverride() {
        // add an override for driedBeans from 548d to 365d
        String inputWithOneOverride = adjustEachLine(INPUT_TEXT, line -> {
            if (line.contains("driedBeans")) {
                return line + ",365d";
            }
            return line + ",";
        });

        return HEADER_WITHOUT_LABEL + ",override" + "\n" + inputWithOneOverride;
    }

    private String readFileContents(String shelfLifeFileOut) throws IOException, URISyntaxException {
        List<String> expectedFileContents = Files.readAllLines(new File(this.getClass().getResource(shelfLifeFileOut).toURI()).toPath());
        return expectedFileContents.stream().collect(Collectors.joining("\n"));
    }

    static Builder setShelfLifeInputFile(Builder builder) throws IOException, URISyntaxException {
        File file = new File(AgeOffCsvToMatchPatternFormatterTest.class.getResource(SHELF_LIFE_FILE_IN).toURI());
        Iterator<String> lineIterator = Files.lines(file.toPath()).iterator();
        return builder.setInput(lineIterator);
    }

    private String reformat(Builder builder, String inputText) throws IOException {
        builder.setInput(inputText.lines().iterator());
        return reformat(builder);
    }

    private String reformat(Builder builder) throws IOException {
        StringWriter out = new StringWriter();
        AgeOffCsvToMatchPatternFormatter generator = new AgeOffCsvToMatchPatternFormatter(builder.build());
        generator.write(out);
        return out.toString();
    }

    private static String adjustEachLine(String input, Function<String,String> function) {
        return Arrays.stream(input.split("\\n")).map(item -> {
            return function.apply(item);
        }).collect(Collectors.joining("\n")) + "\n";
    }
}
