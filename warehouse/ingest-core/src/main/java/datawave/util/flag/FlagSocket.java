package datawave.util.flag;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Observable;
import java.util.Observer;
import org.apache.log4j.Logger;

/**
 *
 */
public class FlagSocket extends Observable implements Runnable, Observer {

    private static final Logger log = Logger.getLogger(FlagSocket.class);
    private ServerSocket serverSocket;
    private volatile boolean running = true;

    public FlagSocket(int port) throws IOException {
        serverSocket = new ServerSocket(port);
    }

    @Override
    public void run() {
        // register ourselves to observe...
        addObserver(this);
        log.info("Listening for shutdown commands on port " + serverSocket.getLocalPort());
        while (running) {
            try {
                Socket s = serverSocket.accept();
                SocketAddress remoteAddress = s.getRemoteSocketAddress();
                try {
                    log.info(remoteAddress + " connected to the shutdown port");
                    s.setSoTimeout(30000);
                    InputStream is = s.getInputStream();
                    BufferedReader rdr = new BufferedReader(new InputStreamReader(is));
                    String line = rdr.readLine();
                    is.close();
                    s.close();
                    setChanged();
                    notifyObservers(line);
                } catch (SocketTimeoutException e) {
                    log.info("Timed out waiting for input from " + remoteAddress);
                }
            } catch (SocketException e) {
                if (running) {
                    log.info("Socket Exception occurred: " + e.getMessage(), e);
                }
            } catch (IOException e) {
                log.error("Error waiting for shutdown connection: " + e.getMessage(), e);
            }
        }
    }

    @Override
    public void update(Observable o, Object arg) {
        if (this == o) {
            if ("shutdown".equals(arg)) {
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

}
