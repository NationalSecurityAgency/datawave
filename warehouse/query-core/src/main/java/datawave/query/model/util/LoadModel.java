package datawave.query.model.util;

import java.io.InputStream;
import java.util.Collection;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Source;
import javax.xml.transform.sax.SAXSource;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.xml.sax.InputSource;

import datawave.query.model.FieldMapping;
import datawave.query.model.QueryModel;
import datawave.webservice.model.Model;

/**
 * Utility class to load a model
 */
public class LoadModel {

    private static final Logger log = LogManager.getLogger(LoadModel.class);

    public static QueryModel loadModelFromXml(InputStream stream) throws Exception {

        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setFeature("http://xml.org/sax/features/external-general-entities", false);
        spf.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
        spf.setNamespaceAware(true);

        Source xmlSource = new SAXSource(spf.newSAXParser().getXMLReader(), new InputSource(stream));

        JAXBContext ctx = JAXBContext.newInstance(Model.class);
        Unmarshaller um = ctx.createUnmarshaller();
        Model xmlModel = (Model) um.unmarshal(xmlSource);

        if (log.isDebugEnabled()) {
            log.debug(xmlModel.getName());
            for (FieldMapping fieldMapping : xmlModel.getFields()) {
                log.debug(fieldMapping.toString());
            }
        }

        return loadModelFromFieldMappings(xmlModel.getFields());
    }

    public static QueryModel loadModelFromFieldMappings(Collection<FieldMapping> fieldMappings) {
        QueryModel model = new QueryModel();
        for (FieldMapping mapping : fieldMappings) {
            if (mapping.isFieldMapping()) {
                switch (mapping.getDirection()) {
                    case FORWARD:
                        model.addTermToModel(mapping.getModelFieldName(), mapping.getFieldName());
                        break;
                    case REVERSE:
                        model.addTermToReverseModel(mapping.getFieldName(), mapping.getModelFieldName());
                        break;
                    default:
                        log.error("Unknown direction: " + mapping.getDirection());
                }
            } else {
                model.setModelFieldAttributes(mapping.getModelFieldName(), mapping.getAttributes());
            }

        }

        if (model.getForwardQueryMapping().isEmpty() && model.getReverseQueryMapping().isEmpty()) {
            throw new IllegalArgumentException("The resulting, loaded query model was empty.");
        }

        return model;
    }

    /**
     * Simple factory method to load a query model from the specified classpath resource
     *
     * @param queryModelXml
     *            the model xml
     * @return QueryModel instance
     * @throws Exception
     *             if there are issues
     */
    public static QueryModel loadModelFromXml(String queryModelXml) throws Exception {
        QueryModel model;
        try (InputStream modelStream = LoadModel.class.getResourceAsStream(queryModelXml)) {
            model = loadModelFromXml(modelStream);
        }
        return model;
    }
}
