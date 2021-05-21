package datawave.microservice.common.storage.queue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import datawave.microservice.common.storage.QueryPool;
import datawave.microservice.common.storage.QueryQueueListener;
import datawave.microservice.common.storage.QueryQueueManager;
import datawave.microservice.common.storage.QueryTask;
import datawave.microservice.common.storage.QueryTaskNotification;
import datawave.microservice.common.storage.Result;
import datawave.microservice.common.storage.TaskKey;
import datawave.microservice.common.storage.config.QueryStorageProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.config.DirectRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerEndpoint;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.support.converter.MessagingMessageConverter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;

import static datawave.microservice.common.storage.queue.RabbitQueryQueueManager.RABBIT;

@Component
@ConditionalOnProperty(name = "query.storage.backend", havingValue = RABBIT)
@ConditionalOnMissingBean(type = "QueryQueueManager")
public class RabbitQueryQueueManager implements QueryQueueManager {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    public static final String RABBIT = "rabbit";
    
    private final QueryStorageProperties properties;
    private final RabbitAdmin rabbitAdmin;
    private final RabbitTemplate rabbitTemplate;
    private final RabbitListenerEndpointRegistry rabbitListenerEndpointRegistry;
    private final ConnectionFactory connectionFactory;
    
    // A mapping of exchange/queue names to routing keys
    private Map<String,String> exchanges = new HashMap<>();
    
    public RabbitQueryQueueManager(QueryStorageProperties properties, RabbitAdmin rabbitAdmin, RabbitTemplate rabbitTemplate,
                    RabbitListenerEndpointRegistry rabbitListenerEndpointRegistry, ConnectionFactory connectionFactory) {
        this.properties = properties;
        this.rabbitAdmin = rabbitAdmin;
        this.rabbitTemplate = rabbitTemplate;
        this.rabbitListenerEndpointRegistry = rabbitListenerEndpointRegistry;
        this.connectionFactory = connectionFactory;
    }
    
    /**
     * Create a listener for a specified listener id
     *
     * @param listenerId
     *            The listener id
     * @param queueName
     *            The queue to listen to
     * @return a query queue listener
     */
    @Override
    public QueryQueueListener createListener(String listenerId, String queueName) {
        QueryQueueListener listener = new RabbitQueueListener(listenerId, queueName);
        return listener;
    }
    
    /**
     * Ensure a queue is created for a pool. This will create an exchange, a queue, and a binding between them for the query pool.
     *
     * @param queryPool
     *            the query poll
     */
    @Override
    public void ensureQueueCreated(QueryPool queryPool) {
        QueryTaskNotification testMessage = new QueryTaskNotification(new TaskKey(UUID.randomUUID(), queryPool, UUID.randomUUID(), "NA"),
                        QueryTask.QUERY_ACTION.TEST);
        ensureQueueCreated(queryPool.getName(), testMessage.getTaskKey().toRoutingKey(), testMessage.getTaskKey().toKey());
    }
    
    /**
     * A mechanism to delete a queue for a pool.
     *
     * @param queryPool
     *            the query pool
     */
    @Override
    public void deleteQueue(QueryPool queryPool) {
        deleteQueue(queryPool.getName());
    }
    
    /**
     * A mechanism to empty a queues messages for a pool
     *
     * @param queryPool
     *            the query pool
     */
    @Override
    public void emptyQueue(QueryPool queryPool) {
        emptyQueue(queryPool.getName());
    }
    
    /**
     * Ensure a queue is created for a query results queue. This will create an exchange, a queue, and a binding between them for the results queue.
     *
     * @param queryId
     *            the query ID
     */
    @Override
    public void ensureQueueCreated(UUID queryId) {
        ensureQueueCreated(queryId.toString(), queryId.toString(), queryId.toString());
    }
    
    /**
     * Delete a queue for a query
     *
     * @param queryId
     *            the query ID
     */
    @Override
    public void deleteQueue(UUID queryId) {
        deleteQueue(queryId.toString());
    }
    
    /**
     * Empty a queue for a query
     *
     * @param queryId
     *            the query ID
     */
    @Override
    public void emptyQueue(UUID queryId) {
        emptyQueue(queryId.toString());
    }
    
    /**
     * This will send a result message. This will call ensureQueueCreated before sending the message.
     * <p>
     * TODO Should the result be more strongly typed?
     *
     * @param queryId
     *            the query ID
     * @param result
     *            the result
     */
    @Override
    public void sendMessage(UUID queryId, Result result) {
        ensureQueueCreated(queryId);
        
        String exchangeName = queryId.toString();
        if (log.isDebugEnabled()) {
            log.debug("Publishing message to " + exchangeName);
        }
        try {
            rabbitTemplate.convertAndSend(exchangeName, queryId.toString(), result);
        } catch (Exception e) {
            throw new RuntimeException("Unable to serialize results", e);
        }
    }
    
    /**
     * Add a queue to a listener
     *
     * @param listenerId
     *            The listenerid
     * @param queueName
     *            The queue name
     */
    private void addQueueToListener(String listenerId, String queueName) {
        if (log.isDebugEnabled()) {
            log.debug("adding queue : " + queueName + " to listener with id : " + listenerId);
        }
        if (!checkQueueExistOnListener(listenerId, queueName)) {
            AbstractMessageListenerContainer listener = getMessageListenerContainerById(listenerId);
            if (listener != null) {
                if (log.isTraceEnabled()) {
                    log.trace("Found listener for " + listenerId);
                }
                listener.addQueueNames(queueName);
                if (log.isTraceEnabled()) {
                    log.trace("added queue : " + queueName + " to listener with id : " + listenerId);
                }
            }
        } else {
            if (log.isTraceEnabled()) {
                log.trace("given queue name : " + queueName + " already exists on given listener id : " + listenerId);
            }
        }
    }
    
    /**
     * Remove a queue from a listener
     *
     * @param listenerId
     *            The listener id
     * @param queueName
     *            The query name
     */
    private void removeQueueFromListener(String listenerId, String queueName) {
        if (log.isInfoEnabled()) {
            log.info("removing queue : " + queueName + " from listener : " + listenerId);
        }
        if (checkQueueExistOnListener(listenerId, queueName)) {
            this.getMessageListenerContainerById(listenerId).removeQueueNames(queueName);
        } else {
            if (log.isTraceEnabled()) {
                log.trace("given queue name : " + queueName + " not exist on given listener id : " + listenerId);
            }
        }
    }
    
    /**
     * Check whether a queue exists on a listener
     *
     * @param listenerId
     *            The listener id
     * @param queueName
     *            The query name
     * @return true if listening, false if not
     */
    private boolean checkQueueExistOnListener(String listenerId, String queueName) {
        try {
            if (log.isTraceEnabled()) {
                log.trace("checking queueName : " + queueName + " exist on listener id : " + listenerId);
            }
            AbstractMessageListenerContainer container = this.getMessageListenerContainerById(listenerId);
            String[] queueNames = (container == null ? null : container.getQueueNames());
            if (queueNames != null) {
                if (log.isTraceEnabled()) {
                    log.trace("checking " + queueName + " exist on active queues " + Arrays.toString(queueNames));
                }
                for (String name : queueNames) {
                    if (name.equals(queueName)) {
                        log.trace("queue name exist on listener, returning true");
                        return true;
                    }
                }
                log.trace("queue name does not exist on listener, returning false");
                return false;
            } else {
                log.trace("there is no queue exist on listener");
                return false;
            }
        } catch (Exception e) {
            log.error("Error on checking queue exist on listener", e);
            return false;
        }
    }
    
    /**
     * Get the listener given a listener id
     *
     * @param listenerId
     *            The listener id
     * @return the listener
     */
    private AbstractMessageListenerContainer getMessageListenerContainerById(String listenerId) {
        if (log.isTraceEnabled()) {
            log.trace("getting message listener container by id : " + listenerId);
        }
        return ((AbstractMessageListenerContainer) this.rabbitListenerEndpointRegistry.getListenerContainer(listenerId));
    }
    
    /**
     * Ensure a queue is created for a given pool
     *
     * @param exchangeQueueName
     *            The name of the exchange and the queue
     * @param routingPattern
     *            The routing pattern used to bind the exchange to the queue
     * @param routingKey
     *            A routing key to use for a test message
     */
    private void ensureQueueCreated(String exchangeQueueName, String routingPattern, String routingKey) {
        if (exchanges.get(exchangeQueueName) == null) {
            synchronized (exchanges) {
                if (exchanges.get(exchangeQueueName) == null) {
                    if (log.isInfoEnabled()) {
                        log.debug("Creating exchange/queue " + exchangeQueueName + " with routing pattern " + routingPattern);
                    }
                    TopicExchange exchange = new TopicExchange(exchangeQueueName, properties.isSynchStorage(), false);
                    Queue queue = new Queue(exchangeQueueName, properties.isSynchStorage(), false, false);
                    Binding binding = BindingBuilder.bind(queue).to(exchange).with(routingPattern);
                    rabbitAdmin.declareExchange(exchange);
                    rabbitAdmin.declareQueue(queue);
                    rabbitAdmin.declareBinding(binding);
                    
                    exchanges.put(exchangeQueueName, routingPattern);
                }
            }
        }
    }
    
    private void deleteQueue(String exchangeQueueName) {
        try {
            rabbitAdmin.deleteExchange(exchangeQueueName);
            rabbitAdmin.deleteQueue(exchangeQueueName);
            exchanges.remove(exchangeQueueName);
        } catch (AmqpIOException e) {
            log.error("Failed to delete queue " + exchangeQueueName, e);
        }
    }
    
    private void emptyQueue(String exchangeQueueName) {
        try {
            rabbitAdmin.purgeQueue(exchangeQueueName);
        } catch (AmqpIOException e) {
            // log an continue
            log.error("Failed to empty queue " + exchangeQueueName, e);
        }
    }
    
    private boolean containsCause(Throwable e, Class<? extends Exception> exceptionClass) {
        while (e != null && !exceptionClass.isInstance(e)) {
            e = e.getCause();
        }
        return e != null;
    }
    
    /**
     * A listener for local queues
     */
    public class RabbitQueueListener implements QueryQueueListener {
        private java.util.Queue<org.springframework.messaging.Message<Result>> messageQueue = new ArrayBlockingQueue<>(100);
        private final String listenerId;
        private Thread thread = null;
        
        public RabbitQueueListener(String listenerId, String queueName) {
            this.listenerId = listenerId;
            MessageListenerAdapter listenerAdapter = new MessageListenerAdapter(this, "receiveMessage");
            listenerAdapter.setMessageConverter(new MessagingMessageConverter());
            SimpleRabbitListenerEndpoint endpoint = new SimpleRabbitListenerEndpoint();
            endpoint.setAdmin(rabbitAdmin);
            endpoint.setAutoStartup(true);
            endpoint.setId(listenerId);
            endpoint.setMessageListener(listenerAdapter);
            endpoint.setQueueNames(queueName);
            endpoint.setGroup(queueName);
            DirectRabbitListenerContainerFactory listenerContainerFactory = new DirectRabbitListenerContainerFactory();
            listenerContainerFactory.setConnectionFactory(connectionFactory);
            rabbitListenerEndpointRegistry.registerListenerContainer(endpoint, listenerContainerFactory, true);
        }
        
        @Override
        public String getListenerId() {
            return listenerId;
        }
        
        @Override
        public void stop() {
            AbstractMessageListenerContainer container = (AbstractMessageListenerContainer) rabbitListenerEndpointRegistry
                            .getListenerContainer(getListenerId());
            if (container != null) {
                container.removeQueueNames(container.getQueueNames());
                container.stop();
                rabbitListenerEndpointRegistry.unregisterListenerContainer(getListenerId());
            }
        }
        
        public void receiveMessage(Message<byte[]> message) {
            try {
                messageQueue.add(new GenericMessage<Result>(new ObjectMapper().readerFor(Result.class).readValue(message.getPayload()), message.getHeaders()));
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Failed to deserialize payload as a Result", e);
            } catch (IOException e) {
                throw new RuntimeException("Failed to deserialize payload as a Result", e);
            }
        }
        
        @Override
        public Message<Result> receive(long waitMs) {
            long start = System.currentTimeMillis();
            int count = 0;
            while (messageQueue.isEmpty() && ((System.currentTimeMillis() - start) < waitMs)) {
                count++;
                try {
                    Thread.sleep(1L);
                } catch (InterruptedException e) {
                    break;
                }
            }
            if (log.isTraceEnabled()) {
                log.trace("Cycled " + count + " rounds looking for message");
            }
            if (messageQueue.isEmpty()) {
                return null;
            } else {
                return messageQueue.remove();
            }
        }
    }
    
}
