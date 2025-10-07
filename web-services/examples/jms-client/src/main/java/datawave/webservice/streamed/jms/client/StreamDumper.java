package datawave.webservice.streamed.jms.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;

import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.DefaultEventQueryResponse;
import datawave.webservice.result.EventQueryResponseBase;
import datawave.webservice.result.MetadataQueryResponseBase;

/**
 * A simple JMS query client to use for inspecting a topic
 *
 * To use this mvn package and then run
 *
 * ./bin/run.sh from the Examples directory
 *
 * You will probably need to modify the src/main/resource/jndi.properties file before build
 */
public class StreamDumper {

    private Context ic;
    private ConnectionFactory cf;
    private Connection connection;
    Session session;
    MessageConsumer consumer;

    public StreamDumper() {
        try {
            ic = new InitialContext(); // reads from jndi.properties, so make sure it is correct
            cf = (ConnectionFactory) ic.lookup("java:/XAConnectionFactory");
            connection = cf.createConnection("DATAWAVE", "secret"); // TODO: read this from somewhere
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            connection.start();
        } catch (NamingException e) {
            e.printStackTrace();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public void streamQueueToStdout(String destination) {
        try {
            Queue queue = getQueue(destination);
            MessageConsumer consumer = getConsumer(queue);
            ObjectMessage msg = getMessage(consumer);
            JAXBContext ctx = JAXBContext.newInstance(BaseQueryResponse.class, EventQueryResponseBase.class, EventQueryResponseBase.class,
                            DefaultEventQueryResponse.class, MetadataQueryResponseBase.class);
            if (null != msg) {
                BaseQueryResponse results = (BaseQueryResponse) msg.getObject();
                Marshaller m = ctx.createMarshaller();
                boolean hasResults = true;
                do {
                    hasResults = results.getHasResults();
                    m.marshal(results, System.out);
                    msg = getMessage(consumer);
                    if (null == msg) {
                        hasResults = false; // we are done
                    } else {
                        results = (BaseQueryResponse) msg.getObject(); // get next message
                    }
                    System.out.println("---------------------------------");
                } while (hasResults);
            } else {
                System.out.println("No message.  Try a different queue?");
            }
        } catch (JMSException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String args[]) {
        String destination_id = null;
        if (args.length == 0) {
            destination_id = getQueueIdFromUser();
        } else if (args.length == 1) {
            destination_id = args[0];
        } else {
            throw new IllegalArgumentException("You may only provide one queue id at a time");
        }

        // if you are debugging remotely, start the breakpoint here or later
        StreamDumper sr = new StreamDumper();
        sr.streamQueueToStdout(destination_id);
    }

    private Queue getQueue(String queueName) {
        try {
            return (Queue) ic.lookup("/queue/" + queueName);
        } catch (NamingException e) {
            e.printStackTrace(); // To change body of catch statement use File | Settings | File Templates.
            return null;
        }
    }

    private MessageConsumer getConsumer(Queue queue) {
        try {
            if (null == consumer) {
                consumer = session.createConsumer(queue);
                connection.start();
            }

        } catch (JMSException e) {
            e.printStackTrace();
        }
        return consumer;
    }

    private ObjectMessage getMessage(MessageConsumer consumer) {
        return getMessage(consumer, 10000l); // 10 second timeout be default
    }

    private ObjectMessage getMessage(MessageConsumer consumer, long timeout) {
        try {
            return (ObjectMessage) consumer.receive(timeout);
        } catch (JMSException e) {
            e.printStackTrace();
        }
        throw new RuntimeException("Getting message timed out, time was " + timeout + "ms");
    }

    private static String getQueueIdFromUser() {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String destination_id = null;
        System.out.println("Enter the destination id (should be jms.queue.[UUID]):");

        try {
            destination_id = br.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (destination_id.equals("")) {
            throw new IllegalArgumentException("The destination id can not be null.  Try again");
        }
        return destination_id;
    }

}
