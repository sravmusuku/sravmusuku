package com.example.hol;

public class Main {

    public static void main(String[] args) {

        CustomerManager item=new CustomerManager();
        item.addCustomer(new Customer(1,"sravani","sravani@gmail.com"));
        item.addCustomer(new Customer(2,"sirisha","siri@gmail.com"));
        item.addCustomer(new Customer(3,"shekar","shekar@gmail.com"));

        item.removeCustomer(3);
        item.stringByName("sravani");
        item.displayCustomers();

    }

}
