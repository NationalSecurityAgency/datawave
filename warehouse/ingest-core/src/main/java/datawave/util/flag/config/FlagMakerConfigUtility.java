package datawave.util.flag.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.sax.SAXSource;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.util.Arrays;

/**
 * Simple util to generate sample flag config file and has utility methods to serialize/deserialize <code>FlagMakerConfig</code> objects.
 */
public class FlagMakerConfigUtility {
    private static final Logger LOG = LoggerFactory.getLogger(FlagMakerConfigUtility.class);

    public static final String SAMPLE_FILE_NAME = "SampleFlagConfig.xml";

    public static void main(String[] args) throws Exception {
        createSample();
        System.exit(0);
    }

    static void createSample() throws Exception {
        File f = new File(SAMPLE_FILE_NAME);
        if (f.exists()) {
            LOG.info("{} already exists", SAMPLE_FILE_NAME);
            return;
        }
        LOG.info("Generating flag sample: {}", f.getName());
        FlagMakerConfig flags = new FlagMakerConfig();
        FlagDataTypeConfig cfg = new FlagDataTypeConfig("myDataName", Arrays.asList("dataFolder1", "/absolute/path/to/data2"), 200, "-data.name.override=Junk");
        flags.addFlagConfig(cfg);
        FlagDataTypeConfig defCfg = new FlagDataTypeConfig();
        defCfg.setIngestPool("bulk");
        defCfg.setMaxFlags(100);
        defCfg.setReducers(25);
        defCfg.setScript("/opt/datawave-ingest/current/bin/ingest/bulk-ingest.sh");
        flags.setDefaultCfg(defCfg);
        saveXmlObject(flags, f);
    }

    // Unmarshalling xml into java classes
    public static <T> T getXmlObject(Class<T> clazz, Reader reader) throws IOException {
        SAXParserFactory factory = SAXParserFactory.newInstance();
        SAXSource xmlSource = null;
        try {
            factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            xmlSource = new SAXSource(factory.newSAXParser().getXMLReader(), new InputSource(reader));
            JAXBContext jc = JAXBContext.newInstance(clazz);
            Unmarshaller um = jc.createUnmarshaller();
            return um.unmarshal(xmlSource, clazz).getValue();

        } catch (JAXBException | SAXException | ParserConfigurationException e) {
            throw new RuntimeException(e);
        } finally {
            if (xmlSource != null)
                xmlSource.getInputSource().getCharacterStream().close();
        }
    }

    // marshalling java classes into xml
    public static void saveXmlObject(Object o, File file) throws Exception {
        try (OutputStream outputStream = new FileOutputStream(file)) {
            saveXmlObject(o, outputStream);
        }
    }

    public static void saveXmlObject(Object o, OutputStream outputStream) throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(o.getClass());
        // jaxb util marshall
        Marshaller m = jc.createMarshaller();
        m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        m.marshal(o, outputStream);
    }

    /**
     * Creates a FlagMakerConfig object from command line arguments
     */
    public static FlagMakerConfig parseArgs(String[] args) throws Exception {
        String flagConfigName = null;

        // overrides
        String baseHDFSDirOverride = null;
        String extraIngestArgsOverride = null;
        String flagFileDirectoryOverride = null;
        String flagMakerClass = null;
        String flagMetricsDirectory = null;

        for (int i = 0; i < args.length; i++) {
            if ("-flagConfig".equals(args[i])) {
                flagConfigName = args[++i];
                LOG.info("Using flagConfig of {}", flagConfigName);
            } else if ("-baseHDFSDirOverride".equals(args[i])) {
                baseHDFSDirOverride = args[++i];
                LOG.info("Will override baseHDFSDir with {}", baseHDFSDirOverride);
            } else if ("-extraIngestArgsOverride".equals(args[i])) {
                extraIngestArgsOverride = args[++i];
                LOG.info("Will override extraIngestArgs with {}", extraIngestArgsOverride);
            } else if ("-flagFileDirectoryOverride".equals(args[i])) {
                flagFileDirectoryOverride = args[++i];
                LOG.info("Will override flagFileDirectory with {}", flagFileDirectoryOverride);
            } else if ("-flagMakerClass".equals(args[i])) {
                flagMakerClass = args[++i];
                LOG.info("will override flagMakerClass with {}", flagMakerClass);
            } else if ("-flagMetricsDirectory".equals(args[i])) {
                flagMetricsDirectory = args[++i];
                LOG.info("will override flagMetricsDirectory with {}", flagMetricsDirectory);
            }
        }
        if (flagConfigName == null) {
            flagConfigName = "FlagMakerConfig.xml";
            LOG.warn("No flag config file specified, attempting to use default file: {}", flagConfigName);
        }

        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(flagConfigName));

        if (null != baseHDFSDirOverride) {
            flagMakerConfig.setBaseHDFSDir(baseHDFSDirOverride);
        }
        if (null != flagFileDirectoryOverride) {
            flagMakerConfig.setFlagFileDirectory(flagFileDirectoryOverride);
        }
        if (null != extraIngestArgsOverride) {
            for (FlagDataTypeConfig flagDataTypeConfig : flagMakerConfig.getFlagConfigs()) {
                flagDataTypeConfig.setExtraIngestArgs(extraIngestArgsOverride);
            }
            flagMakerConfig.getDefaultCfg().setExtraIngestArgs(extraIngestArgsOverride);
        }
        if (null != flagMakerClass) {
            flagMakerConfig.setFlagMakerClass(flagMakerClass);
        }
        if (null != flagMetricsDirectory) {
            flagMakerConfig.setFlagMetricsDirectory(flagMetricsDirectory);
        }

        LOG.debug("before validate {}", flagMakerConfig.toString());
        flagMakerConfig.validate();
        LOG.debug("after validate() {}", flagMakerConfig.toString());
        return flagMakerConfig;
    }

    public static FileSystem getHadoopFS(FlagMakerConfig flagMakerConfig) throws IOException {
        Configuration hadoopConfiguration = new Configuration();
        hadoopConfiguration.set("fs.defaultFS", flagMakerConfig.getHdfs());
        try {
            return FileSystem.get(hadoopConfiguration);
        } catch (IOException ex) {
            LOG.error("Unable to connect to HDFS. Exiting");
            throw ex;
        }
    }

}
