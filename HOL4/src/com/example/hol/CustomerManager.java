package com.example.hol;

import java.util.ArrayList;

public class CustomerManager {
    ArrayList<Customer> customerList=new ArrayList<>();
    public void addCustomer(Customer customer) {
        customerList.add(customer);
    }

    public boolean removeCustomer(int customerID) {
        return customerList.removeIf(c -> c.customerId == customerID);
    }
    public Customer stringByName(String name) {
        for (Customer c : customerList) {
            if (c.name.equalsIgnoreCase(name)) {
                return c;
            }
        }
        return null;
    }
    public void displayCustomers() {

        for(Customer c: customerList) {
            System.out.println(c);
        }
    }


}



