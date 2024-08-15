package datawave.webservice.common.health;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.security.PermitAll;
import javax.annotation.security.RunAs;
import javax.ejb.LocalBean;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.deltaspike.core.api.config.ConfigProperty;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.apache.deltaspike.core.api.jmx.JmxManaged;
import org.apache.deltaspike.core.api.jmx.MBean;
import org.jboss.resteasy.annotations.GZIP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.management.OperatingSystemMXBean;

import datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.webservice.result.GenericResponse;

@PermitAll
@Path("/Common/Health")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_XML})
@GZIP
@RunAs("InternalUser")
@LocalBean
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
@MBean
@Exclude(ifProjectStage = DatawaveEmbeddedProjectStageHolder.DatawaveEmbedded.class)
public class HealthBean {
    private static final Logger LOG = LoggerFactory.getLogger(HealthBean.class);
    private static final OperatingSystemMXBean OPERATING_SYSTEM_MX_BEAN = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    private static boolean shutdownInProgress = false;
    private static String status = "ready";

    @Inject
    private AccumuloConnectionFactory accumuloConnectionFactoryBean;

    @Inject
    @ConfigProperty(name = "dw.health.connection.percent.limit", defaultValue = "200")
    private int maxUsedPercent;

    @Inject
    @ConfigProperty(name = "dw.health.shutdown.check.interval.ms", defaultValue = "15000")
    private long queryCompletionWaitIntervalMillis;

    @Inject
    private Instance<HealthInfoContributor> healthInfos;

    /**
     * Returns a {@link ServerHealth} object, which indicates the current health of the server. This object contains information regarding the operating system
     * load and swap usage, along with information regarding how many of the available Accumulo connections are in use. When the connection usage exceeds the
     * percentage allowable by the value of the property {@code dw.health.connection.percent.limit}, then this method will return the http status of 503 to
     * indicate that no new queries should be submitted (or other methods called that need to acquire an Accumulo connection).
     *
     * @return a {@link ServerHealth} object containing health data
     *
     * @HTTP 200 success
     * @HTTP 503 service unavailable - if new query slots are currently unavailable
     */
    @GET
    @Path("/health")
    @JmxManaged
    public Response health() {
        ServerHealth health = new ServerHealth();
        health.status = status;
        health.connectionUsagePercent = accumuloConnectionFactoryBean.getConnectionUsagePercent();
        if (health.connectionUsagePercent >= maxUsedPercent) {
            health.details = health.connectionUsagePercent + " of connections used [>= max of " + maxUsedPercent + " allowed]";
            return Response.status(Status.SERVICE_UNAVAILABLE).entity(health).build();
        } else if (shutdownInProgress) {
            health.details = "Shutdown in progress -- no new query connections allowed.";
            return Response.status(Status.SERVICE_UNAVAILABLE).entity(health).build();
        } else {
            return Response.ok().entity(health).build();
        }
    }

    /**
     * Updates the status to be returned to {@code newStatus}.
     *
     * @param newStatus
     *            the new status to return from {@link #health()}
     * @param request
     *            the servlet request
     * @return the current/previous status
     */
    @PUT
    @Path("/status")
    @Consumes(MediaType.TEXT_PLAIN)
    @JmxManaged
    public Response updateStatus(String newStatus, @Context HttpServletRequest request) {
        newStatus = newStatus.trim();
        GenericResponse<String> response = new GenericResponse<>();
        response.setResult(status);

        // We're only allowed to call updateStatus from the loopback interface, when a shutdown is not in progress.
        if (!"127.0.0.1".equals(request.getRemoteAddr())) {
            LOG.error("Status update to {} requested from {}. Denying access since the request was not from localhost.", newStatus, request.getRemoteAddr());
            response.setResult("status update calls must be made on the local host.");
            return Response.status(Status.FORBIDDEN).entity(response).build();
        } else if (shutdownInProgress) {
            LOG.error("Shutdown in progress. Status change from {} to {} is not allowed.", status, newStatus);
            response.setResult("Cannot change status during a shutdown.");
            return Response.status(Status.FORBIDDEN).entity(response).build();
        } else {
            status = newStatus;
            return Response.ok(response).build();
        }
    }

    /**
     * This method initiates a shutdown. This will set internal state such that the {@link #health()} method will return a {@link Status#SERVICE_UNAVAILABLE}
     * error to prevent new queries from coming in. After that, it will wait up to {@code timeoutMinutes} for queries to complete before initiating a shutdown
     * of the web server.
     *
     * <strong>NOTE:</strong> This method is restricted to users calling from the local host, or via the JMX interface.
     *
     * TODO: Is there a way to "cancel" any requests that are waiting for an Accumulo connection? It would be nice if such requests could be rejected somehow
     * and the load balancer could automatically redirect to a different web server.
     *
     * @param timeoutMinutes
     *            the number of minutes to wait for queries to complete before continuing with the shutdown operation anyway
     * @param request
     *            the request to send
     * @return a message indicating whether or not shutdown was attempted
     */
    @GET
    @Path("/shutdown")
    @JmxManaged
    public Response waitForQueryCompletion(@QueryParam("timeoutMinutes") @DefaultValue("75") int timeoutMinutes, @Context HttpServletRequest request) {
        GenericResponse<String> response = new GenericResponse<>();
        // We're only allowed to call shutdown from the loopback interface.
        if (!"127.0.0.1".equals(request.getRemoteAddr())) {
            LOG.error("Shutdown requested from {}. Denying access since the request was not from localhost.", request.getRemoteAddr());
            response.setResult("Shutdown calls must be made on the local host.");
            return Response.status(Status.FORBIDDEN).entity(response).build();
        }
        LOG.warn("Shutdown requested from {}. Waiting up to {} minutes for queries to complete.", request.getRemoteAddr(), timeoutMinutes);

        // Wait for queries to complete
        long timeoutMillis = TimeUnit.MINUTES.toMillis(timeoutMinutes);
        shutdownInProgress = true;
        status = "drain";
        long startTime = System.currentTimeMillis();
        int connectionUsage;
        while ((connectionUsage = accumuloConnectionFactoryBean.getConnectionUsagePercent()) > 0) {
            long delta = System.currentTimeMillis() - startTime;
            if (delta > timeoutMillis) {
                LOG.warn("Timeout of {} minutes exceeded while waiting for queries to complete. Shutting down anyway.", timeoutMinutes);
                break;
            }
            LOG.info("Connection usage is {}%. Waiting for queries to complete.", connectionUsage);

            try {
                Thread.sleep(queryCompletionWaitIntervalMillis);
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while waiting for queries to complete.");
            }
        }

        if (connectionUsage <= 0) {
            response.setResult("All queries completed. Shutting down.");
        } else {
            response.setResult("Gave up waiting for queries to complete. Shutting down with pool usage percentage of " + connectionUsage + ".");
        }

        // Initiate a server shutdown using the management JMX bean
        try {
            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            ObjectName objectName = new ObjectName("jboss.as:management-root=server");
            mBeanServer.invoke(objectName, "shutdown", new Object[] {false, 0, 0},
                            new String[] {boolean.class.getName(), int.class.getName(), int.class.getName()});
        } catch (MalformedObjectNameException | ReflectionException | InstanceNotFoundException | MBeanException e) {
            LOG.warn("Error shutting down: {}", e);
        }

        return Response.ok().entity(response).build();
    }

    @GET
    @Path("/info")
    @JmxManaged
    public List<VersionInfo> versionInfo() {
        ArrayList<VersionInfo> versionInfos = new ArrayList<>();
        for (HealthInfoContributor contributor : healthInfos) {
            versionInfos.add(contributor.versionInfo());
        }
        return versionInfos;
    }

    @XmlRootElement
    @XmlAccessorType(XmlAccessType.NONE)
    public static class ServerHealth {
        @XmlElement(name = "ConnectionUsagePercent")
        private int connectionUsagePercent;
        @XmlElement(name = "Status")
        private String status;
        @XmlElement(name = "Details")
        private String details;
        @XmlElement(name = "SystemLoad")
        private double load;
        @XmlElement(name = "SwapBytesUsed")
        private long swapBytesUsed;

        public ServerHealth() {
            load = OPERATING_SYSTEM_MX_BEAN.getSystemCpuLoad();
            swapBytesUsed = OPERATING_SYSTEM_MX_BEAN.getTotalSwapSpaceSize() - OPERATING_SYSTEM_MX_BEAN.getFreeSwapSpaceSize();
        }

        public int getConnectionUsagePercent() {
            return connectionUsagePercent;
        }

        public String getStatus() {
            return status;
        }

        public String getDetails() {
            return details;
        }

        public double getLoad() {
            return load;
        }

        public long getSwapBytesUsed() {
            return swapBytesUsed;
        }
    }

    @XmlRootElement
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class VersionInfo {
        private String name;
        private BuildInfo build;
        private GitInfo git;

        public VersionInfo() {
            // Constructor required for JAX-B
        }

        public VersionInfo(String name, BuildInfo build, GitInfo git) {
            this.name = name;
            this.build = build;
            this.git = git;
        }

        public String getName() {
            return name;
        }

        public BuildInfo getBuild() {
            return build;
        }

        public GitInfo getGit() {
            return git;
        }
    }

    @XmlRootElement
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class BuildInfo {
        private String version;
        private String time;

        public BuildInfo() {
            // Constructor required for JAX-B
        }

        public BuildInfo(String version, String time) {
            this.version = version;
            this.time = time;
        }

        public String getVersion() {
            return version;
        }

        public String getTime() {
            return time;
        }
    }

    @XmlRootElement
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class GitInfo {
        private CommitInfo commit;
        private String branch;

        public GitInfo() {
            // Constructor required for JAX-B
        }

        public GitInfo(CommitInfo commit, String branch) {
            this.commit = commit;
            this.branch = branch;
        }

        public CommitInfo getCommit() {
            return commit;
        }

        public String getBranch() {
            return branch;
        }
    }

    @XmlRootElement
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class CommitInfo {
        private String time;
        private String id;

        public CommitInfo() {
            // Constructor required for JAX-B
        }

        public CommitInfo(String time, String id) {
            this.time = time;
            this.id = id;
        }

        public String getTime() {
            return time;
        }

        public String getId() {
            return id;
        }
    }
}
