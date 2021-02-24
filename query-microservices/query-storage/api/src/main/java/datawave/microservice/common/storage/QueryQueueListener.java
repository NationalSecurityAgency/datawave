package datawave.microservice.common.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.core.Message;

import java.io.IOException;

public interface QueryQueueListener {
    public static final long WAIT_MS = 100L;

    String getListenerId();

    default QueryTaskNotification receiveTaskNotification() throws IOException {
        return receiveTaskNotification(100L);
    }

    default QueryTaskNotification receiveTaskNotification(long waitMs) throws IOException {
        Message message = receive(waitMs);
        if (message != null) {
            return new ObjectMapper().readerFor(QueryTaskNotification.class).readValue(message.getBody());
        }
        return null;
    }

    default Message receive() {
        return receive(WAIT_MS);
    }

    Message receive(long waitMs);

    void start();

    void stop();
}
