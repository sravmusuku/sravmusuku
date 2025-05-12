package com.practice.hol;

public class Main {

        public static void main(String[] args) {
            AccountManager am = new AccountManager();
            am.addCustomer(101, 1000);
            am.addCustomer(102, 2000);
            am.deposit(101, 500);
            am.withdraw(102, 1000);
            am.withdraw(101, 3000);
            am.displayBalance(101);
            am.displayAllBalances();

        }

    }

