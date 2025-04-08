package datawave.query.iterator.logic;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import datawave.query.Constants;
import datawave.query.table.parser.ContentKeyValueFactory;

/**
 * This class contains the functionality to generate summaries.
 * <p>
 * </p>
 * Just need to call "createSummary()" after creation.
 */
public class SummaryCreator {
    private final List<String> viewSummaryOrder;
    Map<String,byte[]> foundContent;
    int summarySize;

    public SummaryCreator(List<String> viewSummaryOrder, Map<String,byte[]> foundContent, int summarySize) {
        this.viewSummaryOrder = viewSummaryOrder;
        this.foundContent = foundContent;
        this.summarySize = summarySize;
    }

    /**
     * this method attempts to create a summary out of the found views
     *
     * @return the created summary
     */
    public String createSummary() {
        // check each potential view name we could make summaries for
        for (String name : viewSummaryOrder) {
            if (name.endsWith("*")) {
                // strip wildcard from view name
                name = name.substring(0, name.length() - 1);

                String endingWildcardSummary = getEndingWildcardSummary(name, foundContent, summarySize);
                if (endingWildcardSummary != null) {
                    return endingWildcardSummary;
                }
            } else {
                String simpleSummary = getSimpleSummary(name, foundContent, summarySize);
                if (simpleSummary != null) {
                    return simpleSummary;
                }
            }
        }
        return null;
    }

    /** for matching and creating summaries when view names have trailing wildcards */
    private static String getEndingWildcardSummary(String currentViewName, Map<String,byte[]> foundContent, int summarySize) {
        // if we have a view name that matches the list...
        Map<String,String> summaries = new HashMap<>();
        for (Map.Entry<String,byte[]> entry : foundContent.entrySet()) {
            // first part is view, second part is if compressed still
            String[] temp = entry.getKey().split(Constants.COLON);
            if (temp[0].startsWith(currentViewName)) {
                summaries.put(temp[0], getSummaryForView(entry.getValue(), summarySize, Boolean.parseBoolean(temp[1])));
            }
        }
        if (!summaries.isEmpty()) {
            // return the view name and summary separated by a new line character
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String,String> entry : summaries.entrySet()) {
                sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
            }
            return sb.toString().trim();
        }
        return null;
    }

    /** a straight-up match between view names */
    private static String getSimpleSummary(String currentViewName, Map<String,byte[]> foundContent, int summarySize) {
        for (Map.Entry<String,byte[]> entry : foundContent.entrySet()) {
            // first part is view, second part is if compressed still
            String[] temp = entry.getKey().split(Constants.COLON);
            if (temp[0].equals(currentViewName)) {
                return currentViewName + ": " + getSummaryForView(entry.getValue(), summarySize, Boolean.parseBoolean(temp[1]));
            }
        }
        return null;
    }

    private static String getSummaryForView(byte[] content, int summarySize, boolean needsDecompressing) {
        String summary;
        if (needsDecompressing) {
            // decode and decompress the content
            summary = new String(ContentKeyValueFactory.decodeAndDecompressContent(content));
        } else {
            summary = new String(content);
        }
        // // if the content is longer than the specified length, truncate it
        // if (summary.length() > summarySize) {
        // summary = summary.substring(0, summarySize);
        // }
        Gson gson = new Gson();

        String url = "http://localhost:11434/api/generate";
        String s = "{" + "\"model\": \"phi3:3.8b-mini-128k-instruct-q4_K_M\","
                        + "\"prompt\": \"create a summary of up to four sentences using the most important information from the following text: " + summary
                        + "\"" + "}";

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = null;
        try {
            request = HttpRequest.newBuilder(new URI(url)).POST(HttpRequest.BodyPublishers.ofString(s)).build();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        AtomicBoolean done = new AtomicBoolean(false);

        client.sendAsync(request, HttpResponse.BodyHandlers.ofInputStream()).thenAccept(response -> {
            try (var in = response.body()) {
                byte[] buffer = new byte[1024];
                int numRead;
                while ((numRead = in.read(buffer)) != -1) {
                    String chunk = new String(buffer, 0, numRead);
                    JsonObject jsonObject = gson.fromJson(chunk, JsonObject.class);
                    if (jsonObject.get("done").getAsBoolean()) {
                        done.set(true);
                        break;
                    }
                    System.out.println("Received chunk: " + jsonObject.get("response").getAsString());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        while (!done.get()) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        System.out.println("SUMMARY: " + summary);
        return summary;
    }
}
