package nsa.datawave.webservice.query.logic;

import java.security.Principal;
import java.util.List;

public interface QueryLogicFactory {
    
    /**
     * 
     * @param name
     *            name of query logic
     * @return new instance of QueryLogic class
     * @throws IllegalArgumentException
     *             if query logic name does not exist
     */
    QueryLogic<?> getQueryLogic(String name, Principal principal) throws IllegalArgumentException, CloneNotSupportedException;
    
    List<QueryLogic<?>> getQueryLogicList();
}
