package com.abhishek.hookrelay.api.error;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    public record ErrorResponse(String error, String message) {
    }

    @ExceptionHandler(ApiException.class)
    public ResponseEntity<ErrorResponse> handleApi(ApiException e) {
        return ResponseEntity.status(e.getStatus())
                .body(new ErrorResponse(e.getCode(), e.getMessage()));
    }

    @ExceptionHandler(MissingRequestHeaderException.class)
    public ResponseEntity<ErrorResponse> handleMissingHeader(MissingRequestHeaderException e) {
        return ResponseEntity.badRequest()
                .body(new ErrorResponse("missing_header",
                        "required header is missing: " + e.getHeaderName()));
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<ErrorResponse> handleTypeMismatch(MethodArgumentTypeMismatchException e) {
        return ResponseEntity.badRequest()
                .body(new ErrorResponse("invalid_value",
                        "value for '" + e.getName() + "' is not well formed"));
    }

    /** Malformed JSON, or a body that cannot be bound to the request record. */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> handleUnreadable(HttpMessageNotReadableException e) {
        return ResponseEntity.badRequest()
                .body(new ErrorResponse("malformed_json", "request body is not valid JSON"));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleUnexpected(Exception e) {
        log.error("unhandled exception", e);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ErrorResponse("internal_error", "unexpected server error"));
    }
}
