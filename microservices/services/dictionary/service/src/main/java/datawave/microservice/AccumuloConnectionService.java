package datawave.microservice;

import java.util.List;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import datawave.accumulo.util.security.UserAuthFunctions;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.dictionary.config.DataDictionaryProperties;
import datawave.security.authorization.DatawaveUser;
import datawave.security.util.ScannerHelper;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

/**
 * Provides accumlo functionality for use by the dictionary controllers. Keeps the accumulo interface separate from the controllers.
 *
 * If other services (other than dictionary/model) are going to need to connect to accumulo, should we move this into the accumulo service and pass the
 * individual properties (instead of a properties object and calling getters)
 */
@Slf4j
@Service
public class AccumuloConnectionService {
    
    private final AccumuloClient accumuloClient;
    private final DataDictionaryProperties dataDictionaryConfiguration;
    private final UserAuthFunctions userAuthFunctions;
    
    // These were already hardcodeded values in ModelBean -> ModelController therefore they weren't made configurable
    private static final long BATCH_WRITER_MAX_LATENCY = 1000L;
    private static final long BATCH_WRITER_MAX_MEMORY = 10845760;
    private static final int BATCH_WRITER_MAX_THREADS = 2;
    
    public AccumuloConnectionService(DataDictionaryProperties dataDictionaryConfiguration, UserAuthFunctions userAuthFunctions,
                    @Qualifier("warehouse") AccumuloClient accumuloClient) {
        this.dataDictionaryConfiguration = dataDictionaryConfiguration;
        this.userAuthFunctions = userAuthFunctions;
        this.accumuloClient = accumuloClient;
    }
    
    /**
     * Return a basic connection to accumulo (as configured by spring)
     *
     * @return a basic connection to accumulo
     */
    public Connection getConnection() {
        Connection connection = new Connection();
        connection.setAccumuloClient(accumuloClient);
        return connection;
    }
    
    /**
     * Returns a Connection representing the connection to accumulo configured with the specified parameters
     *
     * @param modelTable
     *            the name of the model table to use in the connection
     * @param modelName
     *            the name of the model to use in the connection
     * @param user
     *            the user (and the authorities and other security aspects related to the user)
     *            
     * @return a Connection representing the connection to accumulo
     */
    public Connection getConnection(String modelTable, String modelName, DatawaveUserDetails user) {
        return getConnection(dataDictionaryConfiguration.getMetadataTableName(), modelTable, modelName, user);
    }
    
    /**
     * Returns a Connection representing the connection to accumulo configured with the specified parameters
     *
     * @param metadataTable
     *            the name of the metadata table to use in the connection
     * @param modelTable
     *            the name of the model table to use in the connection
     * @param modelName
     *            the name of the model to use in the connection
     * @param user
     *            the user (and the authorities and other security aspects related to the user)
     *            
     * @return a Connection representing the connection to accumulo
     */
    public Connection getConnection(String metadataTable, String modelTable, String modelName, DatawaveUserDetails user) {
        Connection connection = new Connection();
        connection.setMetadataTable(getSupplierValueIfBlank(metadataTable, dataDictionaryConfiguration::getMetadataTableName));
        connection.setModelTable(getSupplierValueIfBlank(modelTable, dataDictionaryConfiguration::getModelTableName));
        connection.setModelName(getSupplierValueIfBlank(modelName, dataDictionaryConfiguration::getModelName));
        connection.setAuths(getAuths(user));
        connection.setAccumuloClient(accumuloClient);
        return connection;
    }
    
    private String getSupplierValueIfBlank(final String value, final Supplier<String> supplier) {
        return StringUtils.isBlank(value) ? supplier.get() : value;
    }
    
    /**
     * Return the accumulo authorizations for the user
     *
     * @param currentUser
     *            the user for whom to get the authorizations
     *            
     * @return the accumulo authorizations for the user
     */
    public Set<Authorizations> getAuths(DatawaveUserDetails currentUser) {
        //@formatter:off
        return currentUser.getProxiedUsers().stream()
                .map(DatawaveUser::getAuths)
                .map(a -> new Authorizations(a.toArray(new String[0])))
                .collect(Collectors.toSet());
        //@formatter:on
    }
    
    /**
     * Return the authorizations for the specified user downgraded to the specified level
     *
     * @param requestedAuthorizations
     *            the level of authorizations to downgrade to
     * @param currentUser
     *            the user for whom to downgrade the authorizations
     *            
     * @return the authorizations for the specified user downgraded to the specified level
     */
    public Set<Authorizations> getDowngradedAuthorizations(String requestedAuthorizations, DatawaveUserDetails currentUser) {
        DatawaveUser primaryUser = currentUser.getPrimaryUser();
        return userAuthFunctions.mergeAuthorizations(userAuthFunctions.getRequestedAuthorizations(requestedAuthorizations, primaryUser),
                        currentUser.getProxiedUsers(), u -> u != primaryUser);
    }
    
    /**
     * Return the keys. If a regexTerm is specified, the keys will be filtered according to that term, otherwise all the keys in the specified table that the
     * user has authorizations for will be returned.
     *
     * @param modelTable
     *            the model table to get the keys of
     * @param currentUser
     *            the user requesting the keys. Used for authorizations purposes
     * @param regexTerm
     *            the regex (can be just a string) to apply to the IteratorSetting
     *            
     * @return the keys of hte specified table that the specified user has the authorzations to access.
     * @throws TableNotFoundException
     *             Thrown if the table is not found
     */
    public List<Key> getKeys(String modelTable, DatawaveUserDetails currentUser, String regexTerm) throws TableNotFoundException {
        Scanner scanner = ScannerHelper.createScanner(this.accumuloClient, modelTable, this.getAuths(currentUser));
        if (!regexTerm.isEmpty()) {
            IteratorSetting cfg = new IteratorSetting(21, "colfRegex", RegExFilter.class.getName());
            cfg.addOption(RegExFilter.COLF_REGEX, "^" + regexTerm + "(\\x00.*)?");
            scanner.addScanIterator(cfg);
        }
        
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(scanner.iterator(), Spliterator.ORDERED), false).map(entry -> entry.getKey())
                        .collect(Collectors.toList());
    }
    
    /**
     * Modify (insert, delete, etc) the mappings. Throws an exception if not successful.
     *
     * @param mutations
     *            the mutations (insertions, deletions, etc) to perform
     * @param modelTable
     *            the model table
     * @param modelName
     *            the model name
     * @param user
     *            the user requesting the changes
     *            
     * @return A QueryException if anything is unsuccessful, and null if everything is successful.
     */
    public QueryException modifyMappings(List<Mutation> mutations, String modelTable, String modelName, DatawaveUserDetails user) {
        QueryException exception = null;
        Mutation mute = null;
        
        try {
            @Cleanup
            BatchWriter writer = getDefaultBatchWriter(modelTable, modelName, user);
            
            for (Mutation m : mutations) {
                mute = m; // make the specific mapping available outside this scope for logging purposes.
                writer.addMutation(m);
            }
        } catch (TableNotFoundException e) {
            log.error("The " + modelTable + " could not be found to write to ", e);
            exception = new QueryException(DatawaveErrorCode.TABLE_NOT_FOUND, e);
        } catch (MutationsRejectedException e) {
            log.error("Could not modify mapping  -- " + mute, e);
            exception = new QueryException(DatawaveErrorCode.INSERT_MAPPING_ERROR, e);
        }
        
        return exception;
    }
    
    private BatchWriter getDefaultBatchWriter(String modelTable, String modelName, DatawaveUserDetails user) throws TableNotFoundException {
        AccumuloClient accumuloClient = getConnection(modelTable, modelName, user).getAccumuloClient();
        // TODO Do we need a new instance of BatchWriterConfig each time, or can this be a static or bean object?
        return accumuloClient.createBatchWriter(modelTable, new BatchWriterConfig().setMaxLatency(BATCH_WRITER_MAX_LATENCY, TimeUnit.MILLISECONDS)
                        .setMaxMemory(BATCH_WRITER_MAX_MEMORY).setMaxWriteThreads(BATCH_WRITER_MAX_THREADS));
    }
}
