package com.cts.demo.java17;
import java.time.LocalDate;
public record Transaction(String txnId, String accountId, String type, double amount, LocalDate date) {}
