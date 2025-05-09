//package com.cts.demos;
package com.cts.demos;

import java.time.LocalDate;

public class Main {
    public static void main(String[] args){
        Customer c01=new Customer();
        System.out.println(c01.getCustomerid());
        System.out.println(c01.getCustomername());
        System.out.println(c01.getEmail());
        Customer c02=new Customer(1,"sravani","sravani@gmail.com");
        System.out.println(c02.getCustomerid());
        System.out.println(c02.getCustomername());
        System.out.println(c02.getEmail());
        Customer c03=new Customer(c02);
        System.out.println(c03.getCustomerid());
        System.out.println(c03.getCustomername());
        System.out.println(c03.getEmail());

      System.out.println("Using record feature");
        Customers cust01=new Customers(1,"sravani","sravani@gmail.com");
        CustomerTransaction trans=new CustomerTransaction(1,cust01,"Deposit", LocalDate.of(2024,04,21),20000.0);
        System.out.println(trans.transid());
        System.out.println(trans.custrecord().customername());
        System.out.println(trans.transtype());
        System.out.println(trans.transdate());
        System.out.println(trans.amount());
        System.out.println("transaction is an higher value:,"+trans.isHigherValue());


    }
}
