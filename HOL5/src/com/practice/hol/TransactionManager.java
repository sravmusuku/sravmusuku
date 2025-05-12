package com.practice.hol;

import java.util.HashSet;

public class TransactionManager {

    HashSet<Transaction> transactions= new HashSet<>();

    public boolean addTransaction(Transaction t) {

        if(transactions.contains(t)) {
            System.out.println("Duplicate transaction");
            return false;
        }
        transactions.add(t);
        return true;
    }
    public void displayTrasactions() {
        for(Transaction t: transactions) {
            System.out.println(t);
        }
    }

}

