package datawave.util.flag;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Listens on a Socket for messages and delivers them to a message queue.
 * Intended to run within its own Thread.
 */
public class FlagSocket implements Runnable {
    // instructs the FlagMaker to stop running
    public static final String SHUTDOWN_MESSAGE = "shutdown";
    // instructs the FlagMaker to produce a flag file with currently loaded files for specified datatype, e.g. "kick datatype"
    public static final String KICK_MESSAGE = "kick";

    private static final Logger log = LoggerFactory.getLogger(FlagSocket.class);
    private static final int SOCKET_TIMEOUT = 30000;

    // socket used to receive messages
    private final ServerSocket serverSocket;
    private volatile boolean running = true;

    // queue to which messages will be delivered
    private final LinkedBlockingQueue<String> messageQueue;

    public FlagSocket(int port, LinkedBlockingQueue<String> messageQueue) throws IOException {
        this.serverSocket = new ServerSocket(port);
        this.messageQueue = messageQueue;
    }

    @Override
    public void run() {
        log.info("Listening for shutdown commands on port {}", serverSocket.getLocalPort());
        while (running) {
            try {
                Socket socket = serverSocket.accept();
                log.info("{} connected to the shutdown port", socket.getRemoteSocketAddress());
                awaitMessageOrTimeout(socket);
            } catch (SocketException e) {
                if (running) {
                    log.info("Socket Exception occurred: {}", e.getMessage(), e);
                }
            } catch (IOException e) {
                log.error("Error waiting for shutdown connection: {}", e.getMessage(), e);
            }
        }
    }

    private void awaitMessageOrTimeout(Socket socket) throws IOException {
        try {
            socket.setSoTimeout(SOCKET_TIMEOUT);
            receiveMessageFromSocket(socket);
        } catch (SocketTimeoutException e) {
            log.info("Timed out waiting for input from {}", socket.getRemoteSocketAddress());
        }
    }

    private void receiveMessageFromSocket(Socket socket) throws IOException {
        try (BufferedReader rdr = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            String line = rdr.readLine();
            socket.close();
            log.info("Add message to queue: {}", line);
            if (null != line) {
                messageQueue.add(line);
                checkForActionableMessages(line);
            }
        }
    }

    private void checkForActionableMessages(String message) {
        if (SHUTDOWN_MESSAGE.equals(message)) {
            log.info("Shutdown call received. Socket exiting.");
            running = false;
            try {
                serverSocket.close();
            } catch (IOException ex) {
                log.info("Failed to close server socket on shutdown", ex);
            }
        }
    }
}
