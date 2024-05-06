package datawave.query.jexl.util;

public interface QueryGenerator {

    String getQuery();

    String getQuery(int minTerms, int maxTerms);

    String getQuery(int numTerms);

    void validateOptions();

    QueryGenerator enableIntersections();

    QueryGenerator disableIntersections();

    QueryGenerator enableUnions();

    QueryGenerator disableUnions();

    QueryGenerator enableNegations();

    QueryGenerator disableNegations();

    QueryGenerator enableRegexes();

    QueryGenerator disableRegexes();

    QueryGenerator enableFilterFunctions();

    QueryGenerator disableFilterFunctions();

    QueryGenerator enableContentFunctions();

    QueryGenerator disableContentFunctions();

    QueryGenerator enableGroupingFunctions();

    QueryGenerator disableGroupingFunctions();

    QueryGenerator enableAllOptions();

    QueryGenerator disableAllOptions();
}
