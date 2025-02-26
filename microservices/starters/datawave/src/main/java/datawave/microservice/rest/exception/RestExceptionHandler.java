package datawave.microservice.rest.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.Nullable;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;
import org.springframework.web.util.WebUtils;

import datawave.microservice.config.web.Constants;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.result.VoidResponse;

@ControllerAdvice
@ConditionalOnClass(QueryException.class)
public class RestExceptionHandler extends ResponseEntityExceptionHandler {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    @Override
    protected ResponseEntity<Object> handleExceptionInternal(Exception ex, @Nullable Object body, HttpHeaders headers, HttpStatus status, WebRequest request) {
        logger.debug("Handling exception {}", ex.getMessage(), ex);
        if (HttpStatus.INTERNAL_SERVER_ERROR.equals(status)) {
            request.setAttribute(WebUtils.ERROR_EXCEPTION_ATTRIBUTE, ex, WebRequest.SCOPE_REQUEST);
        }
        VoidResponse vr = new VoidResponse();
        vr.addException(ex);
        return new ResponseEntity<>(vr, headers, status);
    }
    
    @Override
    protected ResponseEntity<Object> handleMissingServletRequestParameter(MissingServletRequestParameterException ex, HttpHeaders headers, HttpStatus status,
                    WebRequest request) {
        // Overrides the parent in order to include the exception message in the body.
        return handleExceptionInternal(ex, ex.getMessage(), headers, status, request);
    }
    
    /**
     * Handle exceptions of type {@link QueryException}. We will pull out the error code from the exception and add a header with the code. Then the exception
     * will be turned into a {@link VoidResponse} and returned.
     */
    @ExceptionHandler({QueryException.class})
    protected ResponseEntity<Object> handleQueryException(QueryException e, WebRequest request) {
        HttpStatus status = HttpStatus.resolve(e.getBottomQueryException().getStatusCode());
        if (status == null) {
            status = HttpStatus.INTERNAL_SERVER_ERROR;
        }
        
        HttpHeaders headers = new HttpHeaders();
        headers.set(Constants.ERROR_CODE_HEADER, e.getBottomQueryException().getErrorCode());
        
        return handleExceptionInternal(e, null, headers, status, request);
    }
    
    /**
     * Handle a spring {@link AccessDeniedException} by re-throwing the exception. Otherwise, the exception would have been handled in
     * {@link #handleGenericException(Exception, WebRequest)} and turned into a 500 error rather than a 403. While we could return a {@link VoidResponse} here
     * too, we want the default spring processing to handle the access denied error instead.
     */
    @ExceptionHandler({AccessDeniedException.class})
    public ResponseEntity<Object> handleAccessDeniedException(AccessDeniedException ex, WebRequest request) {
        throw ex;
    }
    
    /**
     * This is a catch-all handler for exceptions that aren't caught anywhere else. We simply turn the exception into a {@link VoidResponse} and return it.
     */
    @ExceptionHandler({Exception.class})
    public ResponseEntity<Object> handleGenericException(Exception ex, WebRequest request) {
        return handleExceptionInternal(ex, null, new HttpHeaders(), HttpStatus.INTERNAL_SERVER_ERROR, request);
    }
}
