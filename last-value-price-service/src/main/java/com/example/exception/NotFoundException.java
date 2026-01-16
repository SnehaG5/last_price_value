package com.example.exception;

/**
* Optional exception for explicit not-found cases (not used in Optional-returning API).
*/
public class NotFoundException extends Exception {
 public NotFoundException(String message) {
     super(message);
 }
}
