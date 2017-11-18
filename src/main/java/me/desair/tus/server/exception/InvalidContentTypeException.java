package me.desair.tus.server.exception;

import javax.servlet.http.HttpServletResponse;

/**
 * Exception thrown when the request has an invalid content type.
 */
public class InvalidContentTypeException extends TusException {
    public InvalidContentTypeException(final String message) {
        super(HttpServletResponse.SC_NOT_ACCEPTABLE, message);
    }
}