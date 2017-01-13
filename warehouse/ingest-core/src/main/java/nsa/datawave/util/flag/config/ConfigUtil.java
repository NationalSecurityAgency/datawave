/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nsa.datawave.util.flag.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

/**
 * Simple util to generate sample flag config file and has utility methods to serialize/deserialize <code>FlagMakerConfig</code> objects.
 */
public class ConfigUtil {
    private static final String sampleFileName = "SampleFlagConfig.xml";
    
    public static void main(String[] args) throws Exception {
        createSample();
        System.exit(0);
    }
    
    static void createSample() throws Exception {
        File f = new File(sampleFileName);
        if (f.exists()) {
            System.out.println(sampleFileName + " already exists");
            return;
        }
        System.out.println("Generating flag sample: " + f.getName());
        FlagMakerConfig flags = new FlagMakerConfig();
        FlagDataTypeConfig cfg = new FlagDataTypeConfig("myDataName", Arrays.asList("dataFolder1", "/absolute/path/to/data2"), 200, "-data.name.override=Junk");
        flags.addFlagConfig(cfg);
        FlagDataTypeConfig defCfg = new FlagDataTypeConfig();
        defCfg.setIngestPool("onehr");
        defCfg.setMaxFlags(100);
        defCfg.setReducers(25);
        defCfg.setScript("/opt/datawave-ingest/current/bin/ingest/one-hr-ingest.sh");
        flags.setDefaultCfg(defCfg);
        saveXmlObject(flags, f);
    }
    
    // Unmarshalling xml into java classes
    public static <T> T getXmlObject(Class<T> claz, String file) throws JAXBException, IOException {
        // jaxb util unmarshall
        JAXBContext jc = JAXBContext.newInstance(claz);
        Unmarshaller um = jc.createUnmarshaller();
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(file);
            return (T) um.unmarshal(fis);
        } finally {
            if (fis != null) {
                fis.close();
            }
        }
    }
    
    // marshalling java classes into xml
    public static void saveXmlObject(Object o, File file) throws Exception {
        // open output stream
        OutputStream fos = null;
        try {
            fos = new FileOutputStream(file);
            // jaxb util marshall
            Marshaller m;
            JAXBContext jc = JAXBContext.newInstance(o.getClass());
            m = jc.createMarshaller();
            m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
            m.marshal(o, fos);
        } finally {
            if (fos != null) {
                fos.close();
            }
        }
    }
    
}
