package com.application.exception;


public class ApplicationAlreadyExistsException extends RuntimeException {
    public ApplicationAlreadyExistsException(String msg) {
        super(msg);
    }
}
