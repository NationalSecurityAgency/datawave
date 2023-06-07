package datawave.webservice.examples;

import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import javax.net.ssl.SSLContext;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import datawave.webservice.query.result.EdgeQueryResponseBase;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.EventQueryResponseBase;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.VoidResponse;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.ssl.SSLContextBuilder;

/**
 * An example showing a simple query to the DATAWAVE web service, using the client classes to simplify parsing the query response.
 */
public class JAXBQueryExample {
    private static final String CREATE_PATH = "{0}/DataWave/Query/{1}/create";
    private static final String NEXT_PATH = "{0}/DataWave/Query/{1}/next";
    private static final String CLOSE_PATH = "{0}/DataWave/Query/{1}/close";
    
    public static void main(String[] args) throws Exception {
        Options options = new Options();
        JCommander jCommander = new JCommander(options);
        try {
            jCommander.parse(args);
        } catch (ParameterException e) {
            System.err.println(e.getMessage());
            jCommander.usage();
            System.exit(-1);
        }
        if (options.help) {
            jCommander.usage();
            System.exit(-1);
        }
        
        if (args.length < 1) {
            System.err.println("usage: " + JacksonQueryExample.class.getName() + " baseURI [pagesize]");
            System.exit(-1);
        }
        
        String baseURI = options.baseURI;
        if (baseURI.endsWith("/"))
            baseURI = baseURI.substring(0, baseURI.length() - 1);
        
        KeyStore ts = KeyStore.getInstance(System.getProperty("javax.net.ssl.trustStoreType"));
        ts.load(new FileInputStream(System.getProperty("javax.net.ssl.trustStore")), System.getProperty("javax.net.ssl.trustStorePassword").toCharArray());
        
        SSLContext sslContext = SSLContextBuilder.create().setProtocol("TLSv1.2").setKeyStoreType(System.getProperty("javax.net.ssl.keyStoreType"))
                        .loadKeyMaterial(new File(System.getProperty("javax.net.ssl.keyStore")),
                                        System.getProperty("javax.net.ssl.keyStorePassword").toCharArray(),
                                        System.getProperty("javax.net.ssl.keyStorePassword").toCharArray())
                        .loadTrustMaterial(ts, new TrustSelfSignedStrategy()).build();
        
        Class<? extends BaseQueryResponse> responseClass = Class.forName(options.responseClass).asSubclass(BaseQueryResponse.class);
        
        // Set up the JAXB context--we have to tell it about the root of the class hierarchies we want to deserialize
        JAXBContext jaxbContext = JAXBContext.newInstance(GenericResponse.class, VoidResponse.class, responseClass);
        Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
        
        CloseableHttpClient client = HttpClients.custom().setSSLContext(sslContext).setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE).build();
        
        // Create the query
        HttpPost create = new HttpPost(MessageFormat.format(CREATE_PATH, baseURI, options.queryLogic));
        List<NameValuePair> formparams = new ArrayList<>();
        formparams.add(new BasicNameValuePair("pagesize", Integer.toString(options.pageSize)));
        formparams.add(new BasicNameValuePair("query", options.query));
        formparams.add(new BasicNameValuePair("auths", options.auths));
        formparams.add(new BasicNameValuePair("queryName", "exampleQuery"));
        formparams.add(new BasicNameValuePair("persistence", "TRANSIENT"));
        formparams.add(new BasicNameValuePair("columnVisibility", options.colVis));
        for (BasicNameValuePair pair : options.additionalParameters) {
            System.out.println("Adding parameter: " + pair);
            formparams.add(pair);
        }
        UrlEncodedFormEntity entity = new UrlEncodedFormEntity(formparams);
        create.setEntity(entity);
        
        HttpResponse response = client.execute(create);
        int responseCode = response.getStatusLine().getStatusCode();
        if (responseCode != HttpStatus.SC_OK) {
            System.err.println("Create method failed with code: " + responseCode);
            response.getEntity().writeTo(System.err);
            System.err.println();
            System.exit(-1);
        }
        
        @SuppressWarnings("unchecked")
        GenericResponse<String> createResponse = (GenericResponse<String>) unmarshaller.unmarshal(response.getEntity().getContent());
        String queryId = createResponse.getResult();
        System.out.println("Query ID: " + queryId);
        
        // Iterate over results
        HttpGet next = new HttpGet(MessageFormat.format(NEXT_PATH, baseURI, queryId));
        do {
            response = client.execute(next);
            responseCode = response.getStatusLine().getStatusCode();
            if (responseCode == HttpStatus.SC_NO_CONTENT) {
                break;
            }
            if (responseCode != HttpStatus.SC_OK) {
                System.err.println("Next method failed with code: " + responseCode);
                response.getEntity().writeTo(System.err);
                System.err.println();
                break;
            } else {
                BaseQueryResponse baseQueryResponse = (BaseQueryResponse) unmarshaller.unmarshal(response.getEntity().getContent());
                System.out.println("====== NEXT ======");
                if (baseQueryResponse instanceof EventQueryResponseBase) {
                    System.out.println(((EventQueryResponseBase) baseQueryResponse).getEvents());
                } else if (baseQueryResponse instanceof EdgeQueryResponseBase) {
                    System.out.println(((EdgeQueryResponseBase) baseQueryResponse).getEdges());
                } else {
                    System.out.println(baseQueryResponse);
                }
            }
        } while (true);
        
        // Close the query to release server resources.
        HttpPut close = new HttpPut(MessageFormat.format(CLOSE_PATH, baseURI, queryId));
        response = client.execute(close);
        responseCode = response.getStatusLine().getStatusCode();
        if (responseCode != HttpStatus.SC_OK) {
            System.err.println("Close method failed with code: " + responseCode);
            response.getEntity().writeTo(System.err);
            System.err.println();
            System.exit(-1);
        }
        VoidResponse closeResponse = (VoidResponse) unmarshaller.unmarshal(response.getEntity().getContent());
        System.out.println("Close completed with the following messages: " + closeResponse.getMessages());
    }
}
