package com.example.exception;


/**
* Thrown when batch operations are invoked in an invalid order or on invalid handles.
*/
public class BatchException extends Exception {
 public BatchException(String message) {
     super(message);
 }
}