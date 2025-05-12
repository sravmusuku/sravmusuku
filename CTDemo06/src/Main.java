package com.cts.demos;
public class Main {
    public static void main(String[] args){
        MathOperation addMethod=(x,y)-> x+y;
        MathOperation subMethod=(x,y)-> x-y;
        MathOperation multMethod=(x,y)-> x*y;
        MathOperation divisionMethod=(x,y)-> x%y;
        System.out.println(addMethod.operation(10,20));
        System.out.println(subMethod.operation(10,20));
        System.out.println(multMethod.operation(10,20));
        System.out.println(divisionMethod.operation(10,20));




    }
}
