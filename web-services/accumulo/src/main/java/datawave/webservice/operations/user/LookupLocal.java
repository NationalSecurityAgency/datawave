package datawave.webservice.operations.user;

import javax.ws.rs.core.MultivaluedMap;

import datawave.webservice.response.LookupResponse;

public interface LookupLocal {
    
    /**
     * Look up one or more entries in Accumulo by table, row, and optionally colFam and colQual
     */
    LookupResponse lookup(String table, String row, MultivaluedMap<String,String> queryParameters);
}
