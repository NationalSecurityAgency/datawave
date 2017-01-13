package nsa.datawave.audit;

import java.util.ArrayList;
import java.util.List;
import nsa.datawave.webservice.query.Query;

public class EdgeTFIDFSelectorExtractor implements SelectorExtractor {
    
    @Override
    public List<String> extractSelectors(Query query) throws IllegalArgumentException {
        
        List<String> selectorList = new ArrayList<>();
        String queryStr = query.getQuery();
        String parts[] = queryStr.split("\0");
        int maxSelectorIndex = parts.length - 4;
        for (int x = 0; x <= maxSelectorIndex; x++) {
            selectorList.add(parts[x]);
        }
        return selectorList;
    }
}
