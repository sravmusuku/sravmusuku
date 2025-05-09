package com.cts.demos;

public class Customer {

    private int customerid;
    private String customername;
    private String email;

    public Customer(){
        //Implicit constructor
        this.customerid=100;
        this.customername="dummy";
        this.email="dummy@gmail.com";
    }

    public Customer(int id,String customername,String email){
        //Explicit Customer
        this.customerid=id;
        this.customername=customername;
        this.email=email;
    }

    public Customer(Customer c1){
        //Explicit Customer
        this.customerid=c1.customerid;
        this.customername=c1.customername;
        this.email=c1.email;
    }

public void displayCustomers()
{
    System.out.println(customerid);
    System.out.println(customername);
    System.out.println(email);
}
public int getCustomerid(){
        return customerid;
}
public String getCustomername(){
    return customername;
    }
    public String getEmail(){
     return email;
    }
    public void setCustomername(String cname){
        this.customername=cname;

    }
}
