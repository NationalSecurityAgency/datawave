package datawave.webservice.common.remote;

import static java.lang.Thread.sleep;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class RemoteServiceUtil {
    // special IP from the TEST-NET-1 ip range 192.0.2.0/24 that should never be assigned
    public static final String NON_ROUTABLE_HOST = "192.0.2.255";

    // system picks
    private static final int PORT = 0;

    private HttpServer server;
    private final int port;

    public RemoteServiceUtil() {
        this(PORT);
    }

    public RemoteServiceUtil(int port) {
        this.port = port;
    }

    public void initialize() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(null);
        server.start();
    }

    public void updateRoute(String route, HttpHandler handler) {
        if (!isInitialized()) {
            throw new IllegalStateException("must call initialize first");
        }

        server.removeContext(route);
        server.createContext(route, handler);
    }

    public void addRoute(String route, HttpHandler handler) {
        if (!isInitialized()) {
            throw new IllegalStateException("must call initialize first");
        }

        server.createContext(route, handler);
    }

    public boolean isInitialized() {
        return server != null;
    }

    public void stop() {
        if (server != null) {
            server.stop(1);
        }
    }

    public int getPort() {
        if (!isInitialized()) {
            throw new IllegalStateException("must call initialize first");
        }
        return server.getAddress().getPort();
    }

    public static class ForeverHandler implements HttpHandler {
        private final AtomicBoolean interrupt;

        public ForeverHandler(AtomicBoolean interrupt) {
            this.interrupt = interrupt;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            while (true) {
                try {
                    sleep(50);
                    if (interrupt.get()) {
                        throw new InterruptedException();
                    }
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }
        }
    }
}
