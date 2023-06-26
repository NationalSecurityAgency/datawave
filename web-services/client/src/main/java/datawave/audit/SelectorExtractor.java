package datawave.audit;

import java.util.List;

import datawave.webservice.query.Query;

public interface SelectorExtractor {

    List<String> extractSelectors(Query query);
}
