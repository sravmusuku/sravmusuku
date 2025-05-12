package com.example.hol;

public class Customer {

    int customerId;
    String name;
    String email;
    String trd;


    public Customer(int customerId,String name,String email) {
        this.customerId=customerId;
        this.name=name;
        this.email=email;
    }

    public String toString() {

        return "ID:"+customerId+",Name:"+name+", Email:"+email;

    }
}
