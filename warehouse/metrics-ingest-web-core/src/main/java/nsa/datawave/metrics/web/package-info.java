/**
 * Contains web software to display poller/ingest/mapFile metrics.
 * 
 * Some notes:
 * 
 * 1) It's possible for local testing to have the server look at a static file system vs a war file. Use the following code as a template in the start() method of MetricsServer:
 * 
 * <pre>
 * {@code
 *   Context staticC = new Context(webServer, "/metrics");
 *   staticC.setResourceBase(<path-to-files>);
 *   staticC.addServlet(DefaultServlet.class, "/*");
 *   webServer.addHandler(staticC);
 * }
 * </pre>
 */
package nsa.datawave.metrics.web;