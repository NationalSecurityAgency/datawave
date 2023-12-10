package datawave.microservice.configcheck;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static datawave.microservice.configcheck.util.ArgumentUtils.OUTPUT;
import static datawave.microservice.configcheck.util.ArgumentUtils.getFile;
import static datawave.microservice.configcheck.util.ArgumentUtils.getFileList;
import static datawave.microservice.configcheck.util.ArgumentUtils.getFiles;
import static datawave.microservice.configcheck.util.ArgumentUtils.getOutputPath;
import static datawave.microservice.configcheck.util.FileUtils.getFilePath;
import static datawave.microservice.configcheck.util.XmlRenderUtils.loadContent;
import static datawave.microservice.configcheck.util.XmlRenderUtils.loadProperties;
import static datawave.microservice.configcheck.util.XmlRenderUtils.loadYamlAsProperties;
import static datawave.microservice.configcheck.util.XmlRenderUtils.renderContent;
import static datawave.microservice.configcheck.util.XmlRenderUtils.valueToObject;

public class CommandRunner {
    private static Logger log = LoggerFactory.getLogger(ConfigCheckApplication.class);

    public static final String RENDER = "render";
    public static final String ANALYZE = "analyze";
    public static final String COMPARE = "compare";

    public static final String HELP = "help";
    public static final String CONFIGDIR = "configdir";
    public static final String PROPERTIES = "properties";
    public static final String YAML = "yaml";
    public static final String FULL_REPORT = "fullreport";

    private ApplicationArguments args;

    public CommandRunner(ApplicationArguments args) {
        this.args = args;
    }

    public String run() {
        String output = "";
        boolean help = args.containsOption(HELP);
        if (!args.getNonOptionArgs().isEmpty()) {
            switch (args.getNonOptionArgs().get(0)) {
                case RENDER:
                    try {
                        if (!help) {
                            output = runRenderCommand(args);
                        }
                    } catch (Exception e) {
                        log.error("Encountered exception during render", e);
                    } finally {
                        if (output == null || output.isEmpty()) {
                            output = "USAGE \n" +
                                    "configcheck " + RENDER + " [FILE] [--" + CONFIGDIR + "=[PATH]] --" + PROPERTIES + "=[FILE...] [--" + OUTPUT + "=[FILE]]\n" +
                                    "configcheck " + RENDER + " [FILE] [--" + CONFIGDIR + "=[PATH]] --" + YAML + "=[FILE...] [--" + OUTPUT + "=[FILE]]\n\n" +
                                    "NOTE\n" +
                                    "  Any file containing placeholders of the form '${}' can be rendered, but it is expected\n" +
                                    "    that .properties, .yaml, and .xml files will be rendered.\n\n" +
                                    "OPTIONS\n" +
                                    "  --" + CONFIGDIR + "=[PATH]\n" +
                                    "    The directory containing your configuration files\n\n" +
                                    "  --" + PROPERTIES + "=[FILE...]\n" +
                                    "    A comma-separated list of properties files to load.  If " + CONFIGDIR + " is specified, \n" +
                                    "      the files will be loaded relative to that path.  Configuration files are loaded in order, so \n" +
                                    "      properties in subsequent files will override prior files.\n\n" +
                                    "  --" + YAML + "=[FILE...]\n" +
                                    "    A comma-separated list of yaml files to load.  If " + CONFIGDIR + " is specified, \n" +
                                    "      the files will be loaded relative to that path.  Configuration files are loaded in order, so \n" +
                                    "      properties in subsequent files will override prior files.\n\n" +
                                    "  --" + OUTPUT + "=[FILE]\n" +
                                    "    The file where the output should be written.\n";
                        }
                    }
                    break;
                case ANALYZE:
                    try {
                        if (!help) {
                            output = runAnalyzeCommand(args);
                        }
                    } catch (Exception e) {
                        log.error("Encountered exception during analyze", e);
                    } finally {
                        if (output == null || output.isEmpty()) {
                            output = "USAGE \n" +
                                    "configcheck " + ANALYZE + " [FILE] [--" + FULL_REPORT + "] [--" + CONFIGDIR + "=[PATH]] --" + PROPERTIES + "=[FILE...] [--" + OUTPUT + "=[FILE]]\n" +
                                    "configcheck " + ANALYZE + " [FILE] [--" + FULL_REPORT + "] [--" + CONFIGDIR + "=[PATH]] --" + YAML + "=[FILE...] [--" + OUTPUT + "=[FILE]]\n\n" +
                                    "NOTE\n" +
                                    "  Only XML files can be analyzed.\n\n" +
                                    "OPTIONS\n" +
                                    "  --" + FULL_REPORT + "\n" +
                                    "    Generate a full report containing a mapping of keys to placeholders, values, and other useful " +
                                    "      information.\n\n" +
                                    "  --" + CONFIGDIR + "=[PATH]\n" +
                                    "    The directory containing your configuration files\n\n" +
                                    "  --" + PROPERTIES + "=[FILE...]\n" +
                                    "    A comma-separated list of properties files to load.  If " + CONFIGDIR + " is specified, \n" +
                                    "      the files will be loaded relative to that path.  Configuration files are loaded in order, so \n" +
                                    "      properties in subsequent files will override prior files.\n\n" +
                                    "  --" + YAML + "=[FILE...]\n" +
                                    "    A comma-separated list of yaml files to load.  If " + CONFIGDIR + " is specified, \n" +
                                    "      the files will be loaded relative to that path.  Configuration files are loaded in order, so \n" +
                                    "      properties in subsequent files will override prior files.\n\n" +
                                    "  --" + OUTPUT + "=[FILE]\n" +
                                    "    The file where the output should be written.\n";
                        }
                    }
                    break;
                case COMPARE:
                    try {
                        if (!help) {
                            output = runCompareCommand(args);
                        }
                    } catch (Exception e) {
                        log.error("Encountered exception during compare", e);
                    } finally {
                        if (output == null || output.isEmpty()) {
                            output = "USAGE \n" +
                                    "configcheck " + COMPARE + " [FILE] [FILE] [--" + OUTPUT + "=[FILE]]\n\n" +
                                    "NOTE\n" +
                                    "  Compares the key/values generated by the " + ANALYZE + " command.\n\n" +
                                    "OPTIONS\n" +
                                    "  --" + OUTPUT + "=[FILE]\n" +
                                    "    The file where the output should be written.\n";
                        }
                    }
                    break;
            }
        }

        if (output.isEmpty()) {
            output = "configcheck can be used to render, analyze and compare files which are configured with \n" +
                    "  property placeholders, such as QueryLogicFactory.xml.  \n\n" +
                    "Available Commands:\n" +
                    "  " + RENDER + "   Produces a rendered version of the given file, substituting placeholders using \n" +
                    "             the given configuration properties.\n\n" +
                    "  " + ANALYZE + "  Produces a normalized key/value mapping of xml bean properties to their associated \n" +
                    "             configuration property values.\n\n" +
                    "  " + COMPARE + "  Compares two analyses, and produces a git-merge-like diff showing value differences \n" +
                    "             between the two files.\n\n";
        }

        return output;
    }

    private String runRenderCommand(ApplicationArguments args) {
        // load the content
        String content = loadContent(getFile(args));

        // load the properties (properties, or yaml)
        Properties properties = getProperties(args);

        return handleOutput(renderContent(content, properties));
    }

    private String runAnalyzeCommand(ApplicationArguments args) {
        // load the xml
        String xmlContent = loadContent(getFile(args));

        // load the properties (properties, or yaml)
        Properties properties = getProperties(args);

        XmlPropertyAnalyzer analyzer = new XmlPropertyAnalyzer(xmlContent, properties);
        String output;
        if (args.containsOption(FULL_REPORT)) {
            output = analyzer.getReport();
        } else {
            output = analyzer.getKeyedValues();
        }
        return handleOutput(output);
    }

    private String runCompareCommand(ApplicationArguments args) {
        String output = null;
        String[] files = getFiles(args);
        if (files != null) {
            // load the content
            String first = loadContent(files[0]);
            String second = loadContent(files[1]);

            AnalysisComparator comparator = new AnalysisComparator(files[0], first, files[1], second);
            output = comparator.compareAnalyses();
        }
        return handleOutput(output);
    }

    private String getConfigdir(ApplicationArguments args) {
        String configdir = null;
        if (args.getOptionNames().contains(CONFIGDIR)) {
            configdir = args.getOptionValues(CONFIGDIR).get(0);
        }
        return configdir;
    }

    private Properties getProperties(ApplicationArguments args) {
        String configdir = getConfigdir(args);

        // load the properties (or yaml)
        Properties mergedProperties = null;
        if (args.getOptionNames().contains(PROPERTIES)) {
            mergedProperties = loadProperties(configdir, getFileList(args, PROPERTIES));
        } else if (args.getOptionNames().contains(YAML)) {
            mergedProperties = loadYamlAsProperties(configdir, getFileList(args, YAML));
        } else {
            log.info("No properties or yaml to render");
        }

        return mergedProperties;
    }

    private String handleOutput(String output) {
        String outputPath = getOutputPath(args);
        if (outputPath != null) {
            writeOutput(getFilePath(outputPath), output);
            output = null;
        }
        return output;
    }

    private void writeOutput(Path outputPath, String output) {
        if (output != null) {
            if (outputPath != null) {
                try {
                    Files.write(outputPath, output.getBytes(StandardCharsets.UTF_8));
                } catch (IOException e) {
                    log.error("Unable to write output", e);
                }
            }
        } else {
            log.error("No output");
        }
    }
}
