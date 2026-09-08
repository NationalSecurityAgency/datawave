package datawave.microservice.annotation.writers.accumulo;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.Test;

import datawave.annotation.data.v1.AccumuloAnnotationSerializer;
import datawave.annotation.data.v1.AccumuloAnnotationSourceSerializer;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.microservice.annotation.writers.accumulo.config.AccumuloAnnotationWriterProperties;

/**
 * Unit tests (mock-based, no in-memory Accumulo or Spring context) for the resource-lifecycle behaviors of {@link AccumuloAnnotationWriter}: returning the
 * borrowed Accumulo client on {@link AccumuloAnnotationWriter#close()}, returning it on constructor-failure paths, and tolerating a concurrent
 * {@link TableExistsException} during table creation.
 */
class AccumuloAnnotationWriterLifecycleTest {

    private AccumuloClient mockClient(TableOperations tableOperations) throws Exception {
        AccumuloClient client = mock(AccumuloClient.class);
        SecurityOperations securityOperations = mock(SecurityOperations.class);
        when(client.whoami()).thenReturn("testUser");
        when(client.securityOperations()).thenReturn(securityOperations);
        when(securityOperations.getUserAuthorizations("testUser")).thenReturn(new Authorizations());
        when(client.tableOperations()).thenReturn(tableOperations);
        return client;
    }

    private TableOperations mockTableOperationsTablesAbsent() throws Exception {
        TableOperations tableOperations = mock(TableOperations.class);
        when(tableOperations.exists(anyString())).thenReturn(false);
        return tableOperations;
    }

    @Test
    void testCloseReturnsClientToConnectionFactory() throws Exception {
        TableOperations tableOperations = mockTableOperationsTablesAbsent();
        AccumuloClient client = mockClient(tableOperations);
        AccumuloConnectionFactory connectionFactory = mock(AccumuloConnectionFactory.class);
        when(connectionFactory.getClient(anyString(), anyCollection(), anyString(), any(AccumuloConnectionFactory.Priority.class), anyMap()))
                        .thenReturn(client);

        AccumuloAnnotationWriter writer = new AccumuloAnnotationWriter(connectionFactory, new AccumuloAnnotationWriterProperties(),
                        new AccumuloAnnotationSerializer(), new AccumuloAnnotationSourceSerializer());

        verify(connectionFactory, never()).returnClient(any());

        writer.close();

        verify(connectionFactory, times(1)).returnClient(client);
    }

    @Test
    void testConstructorReturnsBorrowedClientOnFailureAfterBorrow() throws Exception {
        TableOperations tableOperations = mock(TableOperations.class);
        // simulate a genuine (non-race) failure while creating a table, after the client has already been borrowed
        when(tableOperations.exists(anyString())).thenReturn(false);
        org.mockito.Mockito.doThrow(new AccumuloException("simulated failure")).when(tableOperations).create(anyString());
        AccumuloClient client = mockClient(tableOperations);
        AccumuloConnectionFactory connectionFactory = mock(AccumuloConnectionFactory.class);
        when(connectionFactory.getClient(anyString(), anyCollection(), anyString(), any(AccumuloConnectionFactory.Priority.class), anyMap()))
                        .thenReturn(client);

        assertThrows(RuntimeException.class, () -> new AccumuloAnnotationWriter(connectionFactory, new AccumuloAnnotationWriterProperties(),
                        new AccumuloAnnotationSerializer(), new AccumuloAnnotationSourceSerializer()));

        // even though construction failed, the borrowed client must not be orphaned
        verify(connectionFactory, times(1)).returnClient(client);
    }

    @Test
    void testConstructorDoesNotReturnClientWhenBorrowItselfFails() throws Exception {
        AccumuloConnectionFactory connectionFactory = mock(AccumuloConnectionFactory.class);
        when(connectionFactory.getClient(anyString(), anyCollection(), anyString(), any(AccumuloConnectionFactory.Priority.class), anyMap()))
                        .thenThrow(new RuntimeException("pool exhausted"));

        assertThrows(RuntimeException.class, () -> new AccumuloAnnotationWriter(connectionFactory, new AccumuloAnnotationWriterProperties(),
                        new AccumuloAnnotationSerializer(), new AccumuloAnnotationSourceSerializer()));

        // no client was ever successfully borrowed, so there is nothing to return
        verify(connectionFactory, never()).returnClient(any());
    }

    @Test
    void testConstructorToleratesConcurrentTableExistsExceptionDuringTableCreation() throws Exception {
        TableOperations tableOperations = mock(TableOperations.class);
        // simulate the TOCTOU race: exists() reports false (another instance hadn't yet finished creating), but the subsequent
        // create() call fails because that other instance concurrently finished creating the table first.
        when(tableOperations.exists(anyString())).thenReturn(false);
        org.mockito.Mockito.doThrow(new TableExistsException("id", "truthmark", "concurrently created")).when(tableOperations).create(anyString());
        AccumuloClient client = mockClient(tableOperations);
        AccumuloConnectionFactory connectionFactory = mock(AccumuloConnectionFactory.class);
        when(connectionFactory.getClient(anyString(), anyCollection(), anyString(), any(AccumuloConnectionFactory.Priority.class), anyMap()))
                        .thenReturn(client);

        assertDoesNotThrow(() -> new AccumuloAnnotationWriter(connectionFactory, new AccumuloAnnotationWriterProperties(), new AccumuloAnnotationSerializer(),
                        new AccumuloAnnotationSourceSerializer()));

        // construction succeeded despite the race, so the client was retained (not returned)
        verify(connectionFactory, never()).returnClient(any());
    }
}
