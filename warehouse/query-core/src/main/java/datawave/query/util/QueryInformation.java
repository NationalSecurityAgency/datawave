package datawave.query.util;

import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import datawave.microservice.query.Query;

/**
 *
 */
public class QueryInformation {

    public static final String QUERY_ID = "queryId", QUERY_LOGIC_NAME = "queryLogicName", QUERY_NAME = "queryName", QUERY_STRING = "queryString",
                    QUERY_USER = "queryUser", COMMENT = "comment";
    public static final int MAX_COMMENT_LENGTH = 256;

    private static final String EQ = "=", SEP = ", ", QUOTE = "\"", NONE_PROVIDED = "None provided";

    private final String queryId, queryLogicName, queryName, queryString, queryUser;
    private String comment = NONE_PROVIDED;

    public QueryInformation(Query query) {
        this(query, null);
    }

    public QueryInformation(Query query, String queryStr) {
        Preconditions.checkNotNull(query);
        queryId = testAndSetOption(query.getId());
        queryLogicName = testAndSetOption(query.getQueryLogicName());
        queryName = testAndSetOption(query.getQueryName());
        if (null == queryStr)
            queryString = testAndSetOption(query.getQuery());
        else
            queryString = testAndSetOption(queryStr);
        queryUser = testAndSetOption(query.getOwner());
    }

    protected static String testAndSetOption(final Object objStr) {
        String option;
        if (null != objStr) {
            option = objStr.toString();
            if (!option.isEmpty())
                return option;

        }

        return NONE_PROVIDED;

    }

    public QueryInformation(Map<String,String> options) {
        Preconditions.checkNotNull(options);

        if (options.containsKey(QUERY_ID)) {
            if (StringUtils.isEmpty(options.get(QUERY_ID))) {
                queryId = NONE_PROVIDED;
            } else {
                queryId = options.get(QUERY_ID);
            }
        } else {
            queryId = NONE_PROVIDED;
        }

        if (options.containsKey(QUERY_LOGIC_NAME)) {
            if (StringUtils.isEmpty(options.get(QUERY_LOGIC_NAME))) {
                queryLogicName = NONE_PROVIDED;
            } else {
                queryLogicName = options.get(QUERY_LOGIC_NAME);
            }
        } else {
            queryLogicName = NONE_PROVIDED;
        }

        if (options.containsKey(QUERY_NAME)) {
            if (StringUtils.isEmpty(options.get(QUERY_NAME))) {
                queryName = NONE_PROVIDED;
            } else {
                queryName = options.get(QUERY_NAME);
            }
        } else {
            queryName = NONE_PROVIDED;
        }

        if (options.containsKey(QUERY_STRING)) {
            if (StringUtils.isEmpty(options.get(QUERY_STRING))) {
                queryString = NONE_PROVIDED;
            } else {
                queryString = options.get(QUERY_STRING);
            }
        } else {
            queryString = NONE_PROVIDED;
        }

        if (options.containsKey(QUERY_USER)) {
            if (StringUtils.isEmpty(options.get(QUERY_USER))) {
                queryUser = NONE_PROVIDED;
            } else {
                queryUser = options.get(QUERY_USER);
            }
        } else {
            queryUser = NONE_PROVIDED;
        }

        if (options.containsKey(COMMENT)) {
            if (StringUtils.isEmpty(options.get(COMMENT))) {
                comment = NONE_PROVIDED;
            } else {
                comment = options.get(COMMENT);
            }
        } else {
            comment = NONE_PROVIDED;
        }
    }

    public String getQueryId() {
        return queryId;
    }

    public String getQueryLogicName() {
        return queryLogicName;
    }

    public String getQueryName() {
        return queryName;
    }

    public String getQueryString() {
        return queryString;
    }

    public String getQueryUser() {
        return queryUser;
    }

    public String getComment() {
        return (comment);
    }

    /**
     * @param comment
     *            Comment string to be used. Must be less then 256 characters.
     */
    public void setComment(final String comment) {
        if (comment.length() > MAX_COMMENT_LENGTH) {
            throw (new IllegalArgumentException("Comment must be less then " + MAX_COMMENT_LENGTH + " characters"));
        }

        this.comment = comment;
    }

    public Map<String,String> toMap() {
        return ImmutableMap.<String,String> builder().put(QueryInformation.QUERY_ID, queryId).put(QueryInformation.QUERY_LOGIC_NAME, queryLogicName)
                        .put(QueryInformation.QUERY_NAME, queryName).put(QueryInformation.QUERY_STRING, queryString).put(QueryInformation.QUERY_USER, queryUser)
                        .put(QueryInformation.COMMENT, comment).build();
    }

    @Override
    public String toString() {
        return (new StringBuilder(400)).append(QUERY_ID).append(EQ).append(QUOTE).append(queryId).append(QUOTE).append(SEP).append(QUERY_LOGIC_NAME).append(EQ)
                        .append(QUOTE).append(queryLogicName).append(QUOTE).append(SEP).append(QUERY_NAME).append(EQ).append(QUOTE).append(queryName)
                        .append(QUOTE).append(SEP).append(QUERY_STRING).append(EQ).append(QUOTE).append(queryString).append(QUOTE).append(SEP)
                        .append(QUERY_USER).append(EQ).append(QUOTE).append(queryUser).append(QUOTE).append(SEP).append(COMMENT).append(EQ).append(QUOTE)
                        .append(comment).append(QUOTE).toString();
    }
}
