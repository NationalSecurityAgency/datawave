package datawave.microservice.query.edge;

import java.util.Map;

import datawave.edge.model.EdgeModelFields;
import datawave.edge.model.EdgeModelFieldsFactory;
import datawave.microservice.query.edge.config.EdgeModelProperties;

public class EdgeModelFieldsFactoryImpl implements EdgeModelFieldsFactory {
    
    private Map<String,String> baseFieldMap;
    private Map<String,String> keyUtilFieldMap;
    private Map<String,String> transformFieldMap;
    
    public EdgeModelFieldsFactoryImpl(EdgeModelProperties edgeModelProperties) {
        this.baseFieldMap = edgeModelProperties.getBaseFieldMap();
        this.keyUtilFieldMap = edgeModelProperties.getKeyUtilFieldMap();
        this.transformFieldMap = edgeModelProperties.getTransformFieldMap();
    }
    
    @Override
    public EdgeModelFields createFields() {
        EdgeModelFields fields = new EdgeModelFields();
        // now load the maps
        fields.setBaseFieldMap(getBaseFieldMap());
        fields.setKeyUtilFieldMap(getKeyUtilFieldMap());
        fields.setTransformFieldMap(getTransformFieldMap());
        
        return fields;
    }
    
    public Map<String,String> getBaseFieldMap() {
        return baseFieldMap;
    }
    
    public void setBaseFieldMap(Map<String,String> baseFieldMap) {
        this.baseFieldMap = baseFieldMap;
    }
    
    public Map<String,String> getKeyUtilFieldMap() {
        return keyUtilFieldMap;
    }
    
    public void setKeyUtilFieldMap(Map<String,String> keyUtilFieldMap) {
        this.keyUtilFieldMap = keyUtilFieldMap;
    }
    
    public Map<String,String> getTransformFieldMap() {
        return transformFieldMap;
    }
    
    public void setTransformFieldMap(Map<String,String> transformFieldMap) {
        this.transformFieldMap = transformFieldMap;
    }
}
