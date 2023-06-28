package datawave.webservice.examples;

import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import datawave.webservice.result.DefaultEventQueryResponse;
import org.apache.http.message.BasicNameValuePair;

class Options {
    @Parameter(names = {"-b", "--baseURI"}, required = true, description = "The base URI for running queries.")
    String baseURI;

    @Parameter(names = {"-l", "--logic"}, required = true, description = "The query logic to use.")
    String queryLogic;

    @Parameter(names = {"-q", "--query"}, required = true, description = "The query to run.")
    String query;

    @Parameter(names = {"-a", "--auths"}, required = true, description = "Comma-separated list of query authorizations.")
    String auths;

    @Parameter(names = {"-v", "--columnVisibility"}, required = true, description = "Column-visibility expression marking the visibility of the query.")
    String colVis;

    @Parameter(names = {"-r", "--responseClass"}, description = "The name of the response class.")
    String responseClass = DefaultEventQueryResponse.class.getName();

    @Parameter(names = {"-p", "--queryParam"}, variableArity = true, converter = NameValueConverter.class,
                    description = "Optional query parameters specified as \"name:value\".")
    List<BasicNameValuePair> additionalParameters = new ArrayList<>();

    @Parameter(names = {"-s", "--pageSize"}, description = "The page size for query results.")
    int pageSize = 100;

    @Parameter(names = {"-h", "-help", "--help", "-?"}, help = true, description = "Print this message.")
    boolean help;

    public static class NameValueConverter implements IStringConverter<BasicNameValuePair> {
        @Override
        public BasicNameValuePair convert(String value) {
            int idx = value.indexOf(':');
            if (idx < 0) {
                throw new ParameterException(value + " is not a name:value pair");
            }
            return new BasicNameValuePair(value.substring(0, idx), value.substring(idx + 1));
        }
    }
}
