package datawave.microservice.configcheck;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import javax.xml.parsers.ParserConfigurationException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.DefaultApplicationArguments;
import org.xml.sax.SAXException;

public class ConfigCheckApplicationTest {
    
    public static String resourcesAbsolutePath;
    
    @BeforeAll
    public static void beforeAll() {
        resourcesAbsolutePath = new File("src/test/resources").getAbsolutePath();
    }
    
    @Test
    public void testRenderXmlFromYaml() throws IOException, SAXException, ParserConfigurationException {
        // @formatter:off
        String[] stringArgs = new String[]{
                "render",
                Path.of(resourcesAbsolutePath, "input/microservice/QueryLogicFactory.xml").toFile().getAbsolutePath(),
                "--configdir=" + Path.of(resourcesAbsolutePath, "input/microservice/yaml/"),
                "--yaml=" + "application-query.yml"
        };
        // @formatter:on
        
        ApplicationArguments args = new DefaultApplicationArguments(stringArgs);
        
        String output = new CommandRunner(args).run();
        
        String expectedOutput = Files.readString(Path.of(resourcesAbsolutePath, "rendered/microservice/QueryLogicFactory.xml"), StandardCharsets.UTF_8);
        
        assertEquals(expectedOutput, output);
    }
    
    @Test
    public void testRenderXmlFromProperties() throws IOException, SAXException, ParserConfigurationException {
        // @formatter:off
        String[] stringArgs = new String[]{
                "render",
                Path.of(resourcesAbsolutePath, "input/webservice/QueryLogicFactory.xml").toFile().getAbsolutePath(),
                "--configdir=" + Path.of(resourcesAbsolutePath, "input/webservice/properties/"),
                "--properties=" + "default.properties"
        };
        // @formatter:on
        
        ApplicationArguments args = new DefaultApplicationArguments(stringArgs);
        
        String output = new CommandRunner(args).run();
        
        String expectedOutput = Files.readString(Path.of(resourcesAbsolutePath, "rendered/webservice/QueryLogicFactory.xml"), StandardCharsets.UTF_8);
        
        assertEquals(expectedOutput, output);
    }
    
    @Test
    public void testXmlPropertyAnalyzerWithYaml() throws IOException {
        // @formatter:off
        String[] stringArgs = new String[]{
                "analyze",
                Path.of(resourcesAbsolutePath, "input/microservice/QueryLogicFactory.xml").toFile().getAbsolutePath(),
                "--configdir=" + Path.of(resourcesAbsolutePath, "input/microservice/yaml/"),
                "--yaml=" + "application-query.yml"
        };
        // @formatter:on
        
        ApplicationArguments args = new DefaultApplicationArguments(stringArgs);
        
        String output = new CommandRunner(args).run();
        
        String expectedOutput = Files.readString(Path.of(resourcesAbsolutePath, "rendered/microservice/analysis.txt"), StandardCharsets.UTF_8);
        
        assertEquals(expectedOutput, output);
    }
    
    @Test
    public void testXmlPropertyAnalyzerWithProperties() throws IOException {
        // @formatter:off
        String[] stringArgs = new String[]{
                "analyze",
                Path.of(resourcesAbsolutePath, "input/webservice/QueryLogicFactory.xml").toFile().getAbsolutePath(),
                "--configdir=" + Path.of(resourcesAbsolutePath, "input/webservice/properties/"),
                "--properties=" + "default.properties,database.properties"
        };
        // @formatter:on
        
        ApplicationArguments args = new DefaultApplicationArguments(stringArgs);
        
        String output = new CommandRunner(args).run();
        
        String expectedOutput = Files.readString(Path.of(resourcesAbsolutePath, "rendered/webservice/analysis.txt"), StandardCharsets.UTF_8);
        
        assertEquals(expectedOutput, output);
    }
    
    @Test
    public void testXmlPropertyAnalyzerFullReportWithYaml() throws IOException {
        // @formatter:off
        String[] stringArgs = new String[]{
                "analyze",
                Path.of(resourcesAbsolutePath, "input/microservice/QueryLogicFactory.xml").toFile().getAbsolutePath(),
                "--configdir=" + Path.of(resourcesAbsolutePath, "input/microservice/yaml/"),
                "--yaml=" + "application-query.yml",
                "--fullreport"
        };
        // @formatter:on
        
        ApplicationArguments args = new DefaultApplicationArguments(stringArgs);
        
        String output = new CommandRunner(args).run();
        
        String expectedOutput = Files.readString(Path.of(resourcesAbsolutePath, "rendered/microservice/fullReport.txt"), StandardCharsets.UTF_8);
        
        assertEquals(expectedOutput, output);
    }
    
    @Test
    public void testXmlPropertyAnalyzerFullReportWithProperties() throws IOException {
        // @formatter:off
        String[] stringArgs = new String[]{
                "analyze",
                Path.of(resourcesAbsolutePath, "input/webservice/QueryLogicFactory.xml").toFile().getAbsolutePath(),
                "--configdir=" + Path.of(resourcesAbsolutePath, "input/webservice/properties/"),
                "--properties=" + "default.properties,database.properties",
                "--fullreport"
        };
        // @formatter:on
        
        ApplicationArguments args = new DefaultApplicationArguments(stringArgs);
        
        String output = new CommandRunner(args).run();
        
        String expectedOutput = Files.readString(Path.of(resourcesAbsolutePath, "rendered/webservice/fullReport.txt"), StandardCharsets.UTF_8);
        
        assertEquals(expectedOutput, output);
    }
    
    @Test
    public void testKeyValueComparator() throws IOException {
        // @formatter:off
        String[] stringArgs = new String[]{
                "compare",
                Path.of(resourcesAbsolutePath, "rendered/microservice/analysis.txt").toFile().getAbsolutePath(),
                Path.of(resourcesAbsolutePath, "rendered/webservice/analysis.txt").toFile().getAbsolutePath()
        };
        // @formatter:on
        
        ApplicationArguments args = new DefaultApplicationArguments(stringArgs);
        
        String output = new CommandRunner(args).run();
        
        String expectedOutput = Files.readString(Path.of(resourcesAbsolutePath, "rendered/comparison.diff"), StandardCharsets.UTF_8);
        
        assertEquals(expectedOutput, output);
    }
}
