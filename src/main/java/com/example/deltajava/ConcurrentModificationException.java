package com.example.deltajava;

/**
 * Exception thrown when a transaction detects a conflict with another concurrent transaction.
 * This happens when the optimistic concurrency control assumption is violated.
 */
public class ConcurrentModificationException extends RuntimeException {
    
    /**
     * Creates a new exception with the specified message.
     *
     * @param message the error message
     */
    public ConcurrentModificationException(String message) {
        super(message);
    }
    
    /**
     * Creates a new exception with the specified message and cause.
     *
     * @param message the error message
     * @param cause the cause of this exception
     */
    public ConcurrentModificationException(String message, Throwable cause) {
        super(message, cause);
    }
} 