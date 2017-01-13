package nsa.datawave.audit;

import java.util.List;
import nsa.datawave.webservice.query.Query;

public interface SelectorExtractor {
    
    public List<String> extractSelectors(Query query);
}
