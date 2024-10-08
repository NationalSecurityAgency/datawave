package datawave.core.query.logic;

import datawave.webservice.result.BaseQueryResponse;

public interface ResponseEnricher {
    BaseQueryResponse enrichResponse(BaseQueryResponse response);
}
