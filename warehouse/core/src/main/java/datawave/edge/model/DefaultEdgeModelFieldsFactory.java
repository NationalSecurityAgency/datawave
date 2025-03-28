package datawave.edge.model;

import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

public class DefaultEdgeModelFieldsFactory implements EdgeModelFieldsFactory {

    /** required bean context */
    /** common default locations for locating bean xml */
    static final String[] EDGE_MODEL_CONTEXT = {"classpath*:EdgeModelContext.xml"};
    /** required bean name */
    static final String BASE_MODEL_BEAN = "baseFieldMap";
    /** required bean name */
    static final String KEYUTIL_MODEL_BEAN = "keyUtilFieldMap";
    /** required bean name */
    static final String TRANSFORM_MODEL_BEAN = "transformFieldMap";

    private static Logger log = Logger.getLogger(DefaultEdgeModelFieldsFactory.class);

    @Override
    public EdgeModelFields createFields() {
        EdgeModelFields fields = new EdgeModelFields();
        AbstractApplicationContext context = null;
        try {
            String contextOverride = System.getProperty("edge.model.context.path");
            if (null != contextOverride) {
                context = new FileSystemXmlApplicationContext(contextOverride);
            } else {
                ClassLoader thisClassLoader = EdgeModelFields.class.getClassLoader();
                ClassPathXmlApplicationContext cpContext = new ClassPathXmlApplicationContext();
                cpContext.setClassLoader(thisClassLoader);
                cpContext.setConfigLocations(EDGE_MODEL_CONTEXT);
                cpContext.refresh();
                context = cpContext;
            }

            // now load the maps
            fields.setBaseFieldMap((Map<String,String>) context.getBean(BASE_MODEL_BEAN));
            fields.setKeyUtilFieldMap((Map<String,String>) context.getBean(KEYUTIL_MODEL_BEAN));
            fields.setTransformFieldMap((Map<String,String>) context.getBean(TRANSFORM_MODEL_BEAN));
        } catch (Throwable t) {
            log.fatal("Edge model configuration not loaded!! Edge queries will fail until this issue is corrected.");
            log.fatal(String.format("Ensure that the Spring config file '%s' is on the classpath and contains bean names '%s', '%s', and '%s'",
                            EDGE_MODEL_CONTEXT, BASE_MODEL_BEAN, KEYUTIL_MODEL_BEAN, TRANSFORM_MODEL_BEAN), t);
        } finally {
            if (context != null) {
                context.close();
            }
        }

        return fields;
    }

    @SuppressWarnings("unchecked")
    private void loadMaps(ApplicationContext context) {}

}
