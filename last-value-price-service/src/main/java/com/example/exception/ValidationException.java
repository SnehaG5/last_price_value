package com.example.exception;

/**
* Thrown when inputs violate constraints (e.g., chunk size > 1000).
*/
public class ValidationException extends Exception {
 public ValidationException(String message) {
     super(message);
 }
}