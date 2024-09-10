package datawave.webservice.websocket.messages;

import javax.ws.rs.core.MultivaluedMap;

/**
 * A query message for clients to send to indicate a query on this websocket should be executed. The client should send a JSON request representing the query
 * parameters required for {@link datawave.webservice.query.runner.QueryExecutor#createQuery(String, MultivaluedMap)}. For example, the following request will
 * create a query. Note that the logic name is part of the websocket endpoint.
 *
 * <pre>
 * {@code
 *
 * {
 *     "query: "MY_FIELD =~ \'somevalueprefix.*\'",
 *     "queryName": "myQuery",
 *     "persistence": "TRANSIENT",
 *     "overrideCache": "true",
 *     "begin": "20150101 000000",
 *     "end": "20150131 235959",
 *     "query.syntax": "JEXL",
 *     "auths": "auth1,auth2,auth3",
 *     "columnVisibility": "A&B|C"
 * }
 * }
 * </pre>
 */
public class CreateQueryMessage implements QueryMessage {
    private MultivaluedMap<String,String> parameters;

    public CreateQueryMessage(MultivaluedMap parameters) {
        this.parameters = parameters;
    }

    public MultivaluedMap<String,String> getParameters() {
        return parameters;
    }

    @Override
    public Type getType() {
        return Type.CREATE;
    }
}
