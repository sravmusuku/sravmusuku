package com.practice.hol;

import java.util.HashMap;

public class AccountManager {

    HashMap<Integer,Double> accountBalances=new HashMap<>();

    public void addCustomer(int customerId,double initialBalance) {
        accountBalances.put(customerId, initialBalance);
    }

    public void deposit(int customerId,double amount) {
        accountBalances.put(customerId, accountBalances.getOrDefault(customerId,0.0)+amount);
    }

    public void withdraw(int customerId, double amount) {

        double currentBalance=accountBalances.getOrDefault(customerId, 0.0);
        if(amount>currentBalance) {
            System.out.println("Insufficient balance");
        }
        else {
            accountBalances.put(customerId, currentBalance-amount);


        }


    }
    public void displayBalance(int customerId) {
        System.out.println("Balance of "+customerId+" is:"+accountBalances.getOrDefault(customerId, 0.0));
    }

    public void displayAllBalances() {
        for(int id:accountBalances.keySet()) {
            System.out.println("Customer "+id+"->balance:"+accountBalances.get(id));

        }
    }
}
