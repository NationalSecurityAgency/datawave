package datawave.webservice.websocket.messages;

import com.fasterxml.jackson.annotation.JsonProperty;

import datawave.webservice.result.BaseResponse;

/**
 * A message wrapper that allows websocket clients to determine the type of message that was received. The message includes a response type, as well as optional
 * string message and optional {@link BaseResponse}. The {@link #getResponseType()} property indicates the message type and therefore what other optional fields
 * can be expected.
 */
public class QueryResponseMessage {
    public enum ResponseType {
        /** The query was created successfully */
        CREATED,
        /** There was an error during query creation. Expect {@link #getBaseResponse()} to return a response. */
        CREATION_FAILURE,
        /** The query is complete (either successfully, or due to error). */
        COMPLETED,
        /** Query results are available. Expect {@link #getBaseResponse()} to return a response. */
        RESULTS,
        /** There was an error during query execution. Expect {@link #getBaseResponse()} to return a response. */
        ERROR
    }

    @JsonProperty("type")
    private ResponseType responseType;

    @JsonProperty("message")
    private String message;

    @JsonProperty("response")
    private BaseResponse baseResponse;

    public QueryResponseMessage(ResponseType responseType) {
        this.responseType = responseType;
    }

    public QueryResponseMessage(ResponseType responseType, String message) {
        this.responseType = responseType;
        this.message = message;
    }

    public QueryResponseMessage(ResponseType responseType, BaseResponse response) {
        this.responseType = responseType;
        this.baseResponse = response;
    }

    public QueryResponseMessage(ResponseType responseType, String message, BaseResponse response) {
        this.responseType = responseType;
        this.message = message;
        this.baseResponse = response;
    }

    public ResponseType getResponseType() {
        return responseType;
    }

    public String getMessage() {
        return message;
    }

    public BaseResponse getBaseResponse() {
        return baseResponse;
    }
}
