package com.practice.hol;

public class Main {

    public static void main(String[] args) {

        TransactionManager txnManager = new TransactionManager();
        txnManager.addTransaction(new Transaction(1, 5000, "credit", "2025-04-01"));
        txnManager.addTransaction(new Transaction(2, 1500, "debit", "2025-04-02"));
        txnManager.addTransaction(new Transaction(1, 5000, "credit", "2025-04-01"));
        txnManager.displayTrasactions();
    }

}

