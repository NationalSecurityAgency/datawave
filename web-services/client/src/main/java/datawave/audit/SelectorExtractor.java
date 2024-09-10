package datawave.audit;

import java.util.List;

import datawave.microservice.query.Query;

public interface SelectorExtractor {

    List<String> extractSelectors(Query query);
}
