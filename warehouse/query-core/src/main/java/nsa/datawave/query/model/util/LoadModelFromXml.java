package nsa.datawave.query.model.util;

import java.io.InputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import nsa.datawave.query.model.QueryModel;
import nsa.datawave.webservice.model.FieldMapping;
import nsa.datawave.webservice.model.Model;

import org.apache.log4j.Logger;

/**
 * Utility class to load a model from XML using jaxb objects generated in web service
 */
public class LoadModelFromXml {
    
    private static final Logger log = Logger.getLogger(LoadModelFromXml.class);
    
    public static QueryModel loadModelFromXml(InputStream stream) throws JAXBException {
        JAXBContext jaxbContext = JAXBContext.newInstance(Model.class);
        Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
        Model xmlModel = (Model) jaxbUnmarshaller.unmarshal(stream);
        if (log.isDebugEnabled()) {
            log.debug(xmlModel.getName());
            for (FieldMapping fieldMapping : xmlModel.getFields()) {
                log.debug(fieldMapping.toString());
            }
        }
        
        QueryModel model = new QueryModel();
        for (FieldMapping mapping : xmlModel.getFields()) {
            switch (mapping.getDirection()) {
                case FORWARD:
                    model.addTermToModel(mapping.getModelFieldName(), mapping.getFieldName());
                    if (mapping.getIndexOnly()) {
                        model.addUnevaluatedField(mapping.getFieldName());
                    }
                    break;
                case REVERSE:
                    model.addTermToReverseModel(mapping.getFieldName(), mapping.getModelFieldName());
                    break;
                default:
                    log.error("Unknown direction: " + mapping.getDirection());
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
     * @return QueryModel instance
     */
    public static QueryModel loadModel(String queryModelXml) throws Exception {
        QueryModel model = null;
        try (InputStream modelStream = LoadModelFromXml.class.getResourceAsStream(queryModelXml)) {
            model = loadModelFromXml(modelStream);
        } catch (Throwable t) {
            throw t;
        }
        return model;
    }
}
