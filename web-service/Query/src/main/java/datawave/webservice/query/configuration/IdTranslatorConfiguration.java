package datawave.webservice.query.configuration;

import java.util.List;
import java.util.ArrayList;

import datawave.query.data.UUIDType;

import org.springframework.stereotype.Component;

@Component("idTranslatorConfiguration")
public class IdTranslatorConfiguration {
    
    private List<UUIDType> uuidTypes = null;
    private String columnVisibility = null;
    private String beginDate = null;
    
    public String getBeginDate() {
        return this.beginDate;
    }
    
    public String getColumnVisibility() {
        return this.columnVisibility;
    }
    
    public List<UUIDType> getUuidTypes() {
        return this.uuidTypes;
    }
    
    public void setBeginDate(String beginDate) {
        this.beginDate = beginDate;
    }
    
    public void setColumnVisibility(String columnVisibility) {
        this.columnVisibility = columnVisibility;
    }
    
    public void setUuidTypes(List<UUIDType> uuidTypes) {
        List<UUIDType> goodTypes = new ArrayList<UUIDType>();
        if (uuidTypes != null) {
            for (UUIDType uuidType : uuidTypes) {
                if (uuidType.getDefinedView().equalsIgnoreCase("LuceneUUIDEventQuery")) {
                    goodTypes.add(uuidType);
                }
            }
        }
        this.uuidTypes = goodTypes;
    }
}
