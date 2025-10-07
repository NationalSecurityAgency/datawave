package datawave.security.authorization;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

import javax.ws.rs.core.MediaType;

import datawave.webservice.util.AbstractHtmlProviderMessageBodyWriter;

public class DatawaveUserHtmlMessageBodyWriter extends AbstractHtmlProviderMessageBodyWriter<DatawaveUser> {
    protected static final String TITLE = "Datawave User", EMPTY = "";

    @Override
    public boolean isWriteable(Class<?> aClass, Type type, Annotation[] annotations, MediaType mediaType) {
        return DatawaveUser.class.isAssignableFrom(aClass);
    }

    @Override
    public String getTitle(DatawaveUser datawaveUser) {
        return TITLE;
    }

    @Override
    public String getHeadContent(DatawaveUser datawaveUser) {
        return EMPTY;
    }

    @Override
    public String getPageHeader(DatawaveUser datawaveUser) {
        return TITLE + " - " + datawaveUser.getCommonName();
    }

    @Override
    public String getMainContent(DatawaveUser datawaveUser) {
        StringBuilder builder = new StringBuilder();

        builder.append("<table>\n");
        builder.append("<tr><td>Subject DN:</td><td>").append(datawaveUser.getDn().subjectDN()).append("</td></tr>\n");
        builder.append("<tr class='highlight'><td>User DN:</td><td>").append(datawaveUser.getDn().issuerDN()).append("</td></tr>\n");
        builder.append("<tr><td>User Type:</td><td>").append(datawaveUser.getUserType()).append("</td></tr>\n");
        builder.append("<tr class='highlight'><td>Creation Time:</td><td>").append(datawaveUser.getCreationTime()).append("</td></tr>\n");
        builder.append("<tr><td>Expiration Time:</td><td>").append(datawaveUser.getExpirationTime()).append("</td></tr>\n");
        builder.append("</table>\n");

        builder.append("<h2>Roles</h2>\n");
        generateTable(datawaveUser.getRoles(), builder);

        builder.append("<h2>Authorizations</h2>\n");
        generateTable(datawaveUser.getAuths(), builder);

        builder.append("<h2>Role To Auth Mapping</h2>\n");
        builder.append("<table>\n");
        builder.append("<tr><th>Role</th><th>Auth(s)</th><th>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</th><th>Role</th><th>Auth(s)</th></tr>");
        boolean highlight = false;
        for (Iterator<Map.Entry<String,Collection<String>>> iter = datawaveUser.getRoleToAuthMapping().asMap().entrySet().iterator(); iter.hasNext(); /*
                                                                                                                                                       * empty
                                                                                                                                                       */) {
            if (highlight) {
                builder.append("<tr class=\"highlight\">");
            } else {
                builder.append("<tr>");
            }
            highlight = !highlight;

            int cols = 2;
            for (int i = 0; i < cols && iter.hasNext(); i++) {
                if (iter.hasNext()) {
                    Map.Entry<String,Collection<String>> entry = iter.next();
                    builder.append("<td>").append(entry.getKey()).append("</td>");
                    builder.append("<td>").append(entry.getValue().stream().collect(Collectors.joining(", "))).append("</td>");
                } else {
                    builder.append("<td></td>");
                }
                if (i < (cols - 1))
                    builder.append("<td>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>");
            }
        }
        builder.append("</table>\n");

        return builder.toString();
    }

    private void generateTable(Collection<String> entries, StringBuilder builder) {
        builder.append("<table>\n");
        boolean highlight = false;
        for (Iterator<String> iter = entries.iterator(); iter.hasNext(); /* empty */) {
            if (highlight) {
                builder.append("<tr class=\"highlight\">");
            } else {
                builder.append("<tr>");
            }
            highlight = !highlight;

            for (int i = 0; i < 4 && iter.hasNext(); i++) {
                builder.append("<td>");
                builder.append(iter.hasNext() ? iter.next() : "");
                builder.append("</td>");
            }
            builder.append("</tr>");
        }
        builder.append("</table>\n");
    }
}
