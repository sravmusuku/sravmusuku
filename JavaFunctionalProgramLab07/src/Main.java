package com.cts.demo.java17;
import java.util.List;
import java.util.*;
import java.util.stream.*;
import java.util.stream.Collectors;
import java.util.Comparator;
import java.time.LocalDate;
public class Main {
    public static void main(String[] args) {
// Filter High-Value Customers Using Records and Streams
        List<Customer> customers = List.of(
                new Customer("C001", "Amit", 95000),
                new Customer("C002", "Neha", 120000),
                new Customer("C003", "Ravi", 180000),
                new Customer("C004", "Meena", 85000)
        );
        customers.stream()
                .filter(c -> c.balance() > 100000)
                .forEach(c -> System.out.println("VIP Customer: " + c.name() + " ₹" + c.balance()));
// Aggregate Daily Transactions per Account
        List<Transaction> transactions = List.of(
                new Transaction("T001", "ACC001", "CREDIT", 5000, LocalDate.of(2025, 4, 12)),
                new Transaction("T002", "ACC001", "DEBIT", 2000, LocalDate.of(2025, 4, 5)),
                new Transaction("T003", "ACC001", "CREDIT", 3000, LocalDate.of(2025, 4, 20))
        );
        Map<String, Double> totalCredits = transactions.stream()
                .filter(t -> t.type().equals("CREDIT"))
                .collect(Collectors.groupingBy(
                        Transaction::accountId,
                        Collectors.summingDouble(Transaction::amount)
                ));
        totalCredits.forEach((acc, total) -> System.out.println("Account: " + acc + " | Total CREDIT: ₹" + total));
// Generate Sorted Statement of Transactions by Date
        transactions.stream()
                .sorted(Comparator.comparing(Transaction::date))
                .forEach(t -> System.out.println(t.date() + " | " + t.type() + " | ₹" + t.amount()));
// Top N Customers by Balance
        customers.stream()
                .sorted(Comparator.comparing(Customer::balance).reversed())
                .limit(2)
                .forEach(c -> System.out.println("Top Customer: " + c.name() + " ₹" + c.balance()));
    }
}
