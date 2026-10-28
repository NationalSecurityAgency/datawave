package datawave.springframework.integration;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.log4j.Logger;

import datawave.configuration.spring.SpringContextProperties;

public class ContextHelper {

    private static final Logger log = Logger.getLogger(ContextHelper.class);
    private static final Path testClassesDir = Path.of(System.getProperty("user.dir"), "target").resolve("test-classes");
    public static final Path springXmlFilesDir = Path.of(System.getProperty("user.dir"), "target").resolve("springxmlfiles");

    static {
        // base directory in target for springXmlFiles
        assertTrue(Files.isDirectory(ContextHelper.springXmlFilesDir), "springXmlFilesDir does not exist: " + ContextHelper.springXmlFilesDir);
        assertTrue(Files.isDirectory(testClassesDir), "testClassesDir does not exist: " + testClassesDir);
    }

    public static void replaceTextInFile(String filePath, Map<String,String> replacements) throws IOException {
        Path path = Path.of(filePath);
        String fileContents = Files.readString(path);
        for (Map.Entry<String,String> entry : replacements.entrySet()) {
            fileContents = fileContents.replace(entry.getKey(), entry.getValue());
        }
        Files.writeString(path, fileContents);
    }

    public static List<Path> findFileInTargetDirectoryContaining(Path targetDir, String text) throws IOException {
        List<Path> matches = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(targetDir)) {
            paths.filter(Files::isRegularFile).forEach(path -> {
                try {
                    String fileContents = Files.readString(path);
                    if (fileContents.contains(text)) {
                        matches.add(path);
                    }
                } catch (IOException e) {
                    log.error("Error reading file: " + path, e);
                }
            });
            return matches;
        }
    }

    /*
     * fixes internal classpath references within this profile's directory
     */
    public static void fixClasspathReferencesInSpringConfigFiles(String profile, String datawavePath) throws IOException {

        try {
            Path profilePath = springXmlFilesDir.resolve("profiles").resolve(profile);
            assertTrue(Files.isDirectory(profilePath), "profile directory does not exist: " + profilePath);
            String profilePathUri = profilePath.toUri().toString();
            List<Path> matches = findFileInTargetDirectoryContaining(profilePath, "classpath:datawave");
            Map<String,String> replacements = new HashMap<>();

            replacements.put("classpath:datawave", String.format("%s%s", profilePathUri, datawavePath));
            for (Path match : matches) {
                replaceTextInFile(match.toString(), replacements);
            }
        } catch (IOException e) {
            log.error("Exception while replacing text in files: " + e.getMessage(), e);
            throw e;
        }
    }

    public static String writeBeanRefContextForSpringBootProperties(SpringContextProperties springContextProperties, String baseName, String profile)
                    throws IOException {
        Path beanRefContextPath = testClassesDir.resolve("beanRefContext-" + baseName + "-" + profile + ".xml");
        writeSpringContextPropertiesXml(springContextProperties, beanRefContextPath);
        return beanRefContextPath.getFileName().toString();
    }

    public static void writeSpringContextPropertiesXml(SpringContextProperties springContextProperties, Path outputPath) throws IOException {
        XMLOutputFactory outputFactory = XMLOutputFactory.newFactory();

        try (OutputStream outputStream = Files.newOutputStream(outputPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                        StandardOpenOption.WRITE)) {
            XMLStreamWriter writer = outputFactory.createXMLStreamWriter(outputStream, "UTF-8");
            try {
                writer.writeStartDocument("UTF-8", "1.0");
                writer.writeCharacters("\n\n");

                writer.writeStartElement("beans");
                writer.writeDefaultNamespace("http://www.springframework.org/schema/beans");
                writer.writeNamespace("xsi", "http://www.w3.org/2001/XMLSchema-instance");
                writer.writeAttribute("http://www.w3.org/2001/XMLSchema-instance", "schemaLocation",
                                "http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans-4.0.xsd");
                writer.writeAttribute("default-autowire", "no");

                writer.writeCharacters("\n    ");
                writer.writeStartElement("bean");
                writer.writeAttribute("class", SpringContextProperties.class.getName());
                writer.writeAttribute("autowire", "no");

                writer.writeCharacters("\n        ");
                writer.writeEmptyElement("property");
                writer.writeAttribute("name", "useBootstrapContext");
                writer.writeAttribute("value", String.valueOf(springContextProperties.getUseBootstrapContext()));

                writeListProperty(writer, "scanBasePackages", springContextProperties.getScanBasePackages());
                writeListProperty(writer, "sources", springContextProperties.getSources());

                writer.writeCharacters("\n    ");
                writer.writeEndElement();
                writer.writeCharacters("\n");
                writer.writeEndElement();
                writer.writeCharacters("\n");
                writer.writeEndDocument();
                writer.flush();
            } finally {
                writer.close();
            }
        } catch (XMLStreamException e) {
            throw new IOException("Failed to write SpringContextProperties XML to " + outputPath, e);
        }
    }

    public static void writeListProperty(XMLStreamWriter writer, String propertyName, List<String> values) throws XMLStreamException {
        writer.writeCharacters("\n        ");
        writer.writeStartElement("property");
        writer.writeAttribute("name", propertyName);

        writer.writeCharacters("\n            ");
        writer.writeStartElement("list");

        for (String value : values) {
            writer.writeCharacters("\n                ");
            writer.writeStartElement("value");
            writer.writeCharacters(value);
            writer.writeEndElement();
        }

        writer.writeCharacters("\n            ");
        writer.writeEndElement();
        writer.writeCharacters("\n        ");
        writer.writeEndElement();
    }
}
