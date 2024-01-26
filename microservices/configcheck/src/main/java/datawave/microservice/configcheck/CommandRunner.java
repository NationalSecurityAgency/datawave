package datawave.microservice.configcheck;

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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;

/**
 * CommandRunner is used to parse the application arguments and figure out which command needs to be run.
 */
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
    
    public Output run() {
        Output output = new Output();
        boolean help = args.containsOption(HELP) || args.getNonOptionArgs().size() == 1;
        if (!args.getNonOptionArgs().isEmpty()) {
            switch (args.getNonOptionArgs().get(0)) {
                case RENDER:
                    try {
                        if (!help) {
                            runRenderCommand(args, output);
                        }
                    } catch (Exception e) {
                        log.error("Encountered exception during render", e);
                        output.setErrorMessage("Encountered ERROR running render command");
                    } finally {
                        if (output.getMessage() == null) {
                            output.setMessage("USAGE \n" + "configcheck " + RENDER + " [FILE] [--" + CONFIGDIR + "=[PATH]] --" + PROPERTIES + "=[FILE...] [--"
                                            + OUTPUT + "=[FILE]]\n" + "configcheck " + RENDER + " [FILE] [--" + CONFIGDIR + "=[PATH]] --" + YAML
                                            + "=[FILE...] [--" + OUTPUT + "=[FILE]]\n\n" + "NOTE\n"
                                            + "  Any file containing placeholders of the form '${}' can be rendered, but it is expected\n"
                                            + "    that .properties, .yaml, and .xml files will be rendered.\n\n" + "OPTIONS\n" + "  --" + CONFIGDIR
                                            + "=[PATH]\n" + "    The directory containing your configuration files\n\n" + "  --" + PROPERTIES + "=[FILE...]\n"
                                            + "    A comma-separated list of properties files to load.  If " + CONFIGDIR + " is specified, \n"
                                            + "      the files will be loaded relative to that path.  Configuration files are loaded in order, so \n"
                                            + "      properties in subsequent files will override prior files.\n\n" + "  --" + YAML + "=[FILE...]\n"
                                            + "    A comma-separated list of yaml files to load.  If " + CONFIGDIR + " is specified, \n"
                                            + "      the files will be loaded relative to that path.  Configuration files are loaded in order, so \n"
                                            + "      properties in subsequent files will override prior files.\n\n" + "  --" + OUTPUT + "=[FILE]\n"
                                            + "    The file where the output should be written.\n");
                        }
                    }
                    break;
                case ANALYZE:
                    try {
                        if (!help) {
                            runAnalyzeCommand(args, output);
                        }
                    } catch (Exception e) {
                        log.error("Encountered exception during analyze", e);
                        output.setErrorMessage("Encountered ERROR running analyze command");
                    } finally {
                        if (output.getMessage() == null) {
                            output.setMessage("USAGE \n" + "configcheck " + ANALYZE + " [FILE] [--" + FULL_REPORT + "] [--" + CONFIGDIR + "=[PATH]] --"
                                            + PROPERTIES + "=[FILE...] [--" + OUTPUT + "=[FILE]]\n" + "configcheck " + ANALYZE + " [FILE] [--" + FULL_REPORT
                                            + "] [--" + CONFIGDIR + "=[PATH]] --" + YAML + "=[FILE...] [--" + OUTPUT + "=[FILE]]\n\n" + "NOTE\n"
                                            + "  Only XML files can be analyzed.\n\n" + "OPTIONS\n" + "  --" + FULL_REPORT + "\n"
                                            + "    Generate a full report containing a mapping of keys to placeholders, values, and other useful "
                                            + "      information.\n\n" + "  --" + CONFIGDIR + "=[PATH]\n"
                                            + "    The directory containing your configuration files\n\n" + "  --" + PROPERTIES + "=[FILE...]\n"
                                            + "    A comma-separated list of properties files to load.  If " + CONFIGDIR + " is specified, \n"
                                            + "      the files will be loaded relative to that path.  Configuration files are loaded in order, so \n"
                                            + "      properties in subsequent files will override prior files.\n\n" + "  --" + YAML + "=[FILE...]\n"
                                            + "    A comma-separated list of yaml files to load.  If " + CONFIGDIR + " is specified, \n"
                                            + "      the files will be loaded relative to that path.  Configuration files are loaded in order, so \n"
                                            + "      properties in subsequent files will override prior files.\n\n" + "  --" + OUTPUT + "=[FILE]\n"
                                            + "    The file where the output should be written.\n");
                        }
                    }
                    break;
                case COMPARE:
                    try {
                        if (!help) {
                            runCompareCommand(args, output);
                        }
                    } catch (Exception e) {
                        log.error("Encountered exception during compare", e);
                        output.setErrorMessage("Encountered ERROR running compare command");
                    } finally {
                        if (output.getMessage() == null) {
                            output.setMessage("USAGE \n" + "configcheck " + COMPARE + " [FILE] [FILE] [--" + OUTPUT + "=[FILE]]\n\n" + "NOTE\n"
                                            + "  Compares the key/values generated by the " + ANALYZE + " command.\n\n" + "OPTIONS\n" + "  --" + OUTPUT
                                            + "=[FILE]\n" + "    The file where the output should be written.\n");
                        }
                    }
                    break;
                default:
                    if (!help) {
                        output.setErrorMessage("configcheck: '" + args.getNonOptionArgs().get(0) + "' is not a configcheck command.  See 'configcheck --help'");
                    }
                    break;
            }
        }
        
        if (output.getMessage() == null) {
            output.setMessage("configcheck can be used to render, analyze and compare files which are configured with \n"
                            + "  property placeholders, such as QueryLogicFactory.xml.  \n\n" + "Available Commands:\n" + "  " + RENDER
                            + "   Produces a rendered version of the given file, substituting placeholders using \n"
                            + "             the given configuration properties.\n\n" + "  " + ANALYZE
                            + "  Produces a normalized key/value mapping of xml bean properties to their associated \n"
                            + "             configuration property values.\n\n" + "  " + COMPARE
                            + "  Compares two analyses, and produces a git-merge-like diff showing value differences \n"
                            + "             between the two files.\n\n");
        }
        
        return output;
    }
    
    private void runRenderCommand(ApplicationArguments args, Output output) {
        if (args.getNonOptionArgs().size() != 2) {
            output.setErrorMessage("Invalid arguments for render command.  See 'configcheck render --help'");
            return;
        }
        
        // load the content
        String file = getFile(args);
        String content = loadContent(file);
        if (content == null || content.isEmpty()) {
            output.setErrorMessage("No content loaded for '" + file + "'");
            return;
        }
        
        // load the properties (properties, or yaml)
        Properties properties = getProperties(args);
        if (properties == null) {
            output.setErrorMessage("No properties/yaml loaded");
            return;
        }
        
        output.setMessage(handleOutput(renderContent(content, properties)));
    }
    
    private void runAnalyzeCommand(ApplicationArguments args, Output output) {
        if (args.getNonOptionArgs().size() != 2) {
            output.setErrorMessage("Invalid arguments for analyze command.  See 'configcheck analyze --help'");
            return;
        }
        
        // load the xml
        String file = getFile(args);
        String xmlContent = loadContent(file);
        if (xmlContent == null || xmlContent.isEmpty()) {
            output.setErrorMessage("No content loaded for '" + file + "'");
            return;
        }
        
        // load the properties (properties, or yaml)
        Properties properties = getProperties(args);
        if (properties == null) {
            output.setErrorMessage("No properties/yaml loaded");
            return;
        }
        
        XmlPropertyAnalyzer analyzer = new XmlPropertyAnalyzer(xmlContent, properties);
        String report;
        if (args.containsOption(FULL_REPORT)) {
            report = analyzer.getFullReport();
        } else {
            report = analyzer.getSimpleReport();
        }
        
        output.setMessage(handleOutput(report));
    }
    
    private void runCompareCommand(ApplicationArguments args, Output output) {
        if (args.getNonOptionArgs().size() != 3) {
            output.setErrorMessage("Invalid arguments for compare command.  See 'configcheck compare --help'");
            return;
        }
        
        String[] files = getFiles(args);
        if (files != null) {
            // load the content
            String first = loadContent(files[0]);
            if (first == null || first.isEmpty()) {
                output.setErrorMessage("No content loaded for '" + files[0] + "'");
                return;
            }
            
            String second = loadContent(files[1]);
            if (second == null || second.isEmpty()) {
                output.setErrorMessage("No content loaded for '" + files[1] + "'");
                return;
            }
            
            AnalysisComparator comparator = new AnalysisComparator(new Analysis(files[0], first), new Analysis(files[1], second));
            output.setMessage(comparator.compareAnalyses());
        } else {
            output.setErrorMessage("No files to compare");
        }
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
            output = "";
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
