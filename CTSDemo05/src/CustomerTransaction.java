package com.cts.demos;

import java.time.LocalDate;

public record CustomerTransaction(int transid, Customers custrecord, String transtype, LocalDate transdate,double amount) {

    public boolean isHigherValue(){
        if(amount>100000)
            return true;
        else return false;
    }
};
