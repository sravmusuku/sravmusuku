package com.practice.hol;


import java.util.Objects;

public class Transaction {

    int TransactionId;
    double amount;
    String type;
    String date;

    public Transaction(int TransactionId,double amount,String type,String date) {

        this.TransactionId=TransactionId;
        this.amount=amount;
        this.type=type;
        this.date=date;
    }
    public boolean equals(Object o) {
        if(this==o) return true;
        if(!(o instanceof Transaction)) return false;
        Transaction that=(Transaction)o;
        return TransactionId==that.TransactionId;
    }
    public int hashCode() {
        return Objects.hash(TransactionId);
    }
    public String toString() {
        return "TransactionID:"+TransactionId+",amount:"+amount+",type:"+type+",date:"+date;
    }

}
