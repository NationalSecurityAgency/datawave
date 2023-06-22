package datawave.webservice.query.logic;

import java.security.Principal;
import java.util.List;

public interface QueryLogicFactory {

    /**
     *
     * @param name
     *            name of query logic
     * @param principal
     *            the principal
     * @return new instance of QueryLogic class
     * @throws IllegalArgumentException
     *             if query logic name does not exist
     * @throws CloneNotSupportedException
     *             if the clone is not supported
     */
    QueryLogic<?> getQueryLogic(String name, Principal principal) throws IllegalArgumentException, CloneNotSupportedException;

    List<QueryLogic<?>> getQueryLogicList();
}
