//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
/*Common memory - > static firstname
Main m1 = new Main();
Main m2 = new Main();
m1.firstname ="Lebron";
System.out.println(m2.firstname);*/

class SimpleHello
{
    String firstname = "";

    //Constructor Implicit , Explicit and Copy constructor
    public SimpleHello(String firstname) {

        this.firstname = firstname;

    }

    public void sayHello(String name)
    {

        System.out.println("Hello and welcome Simple Hell Class - !"+name);
    }

    public void convert()
    {

        this.firstname = this.firstname.toUpperCase();
    }



}

public class CTSDemo01 {

    static String firstname = "";

    //Constructor Implicit , Explicit and Copy constructor
    public CTSDemo01() {

        this.firstname = "David Convoy";

    }


    public static void main(String[] args) {
        //TIP Press <shortcut actionId="ShowIntentionActions"/> with your caret at the highlighted text
        // to see how IntelliJ IDEA suggests fixing it.

        CTSDemo01 m3 = new CTSDemo01();

        m3.sayHello("Lebron James"); //with class object

        sayHello("Antony Davis"); //static method invocation without object

        CTSDemo01 m4 = new CTSDemo01();
        m4.sayHello(m4.firstname); //David Convey
        m4 = m3;
        m4.sayHello(m4.firstname); //Leborn James

        SimpleHello s1 = new SimpleHello("Satya");
        s1.sayHello(s1.firstname);
        System.out.println("-----------------------------------------------------");

        SimpleHello s2 = new SimpleHello("Prasanna");
        s2.sayHello(s2.firstname);
        s1 = s2; //reference of the s2 into s1 which means its a same memoty location shared
        s1.sayHello(s1.firstname);
        s2.convert();
        s1.sayHello(s1.firstname);




        for (int i = 0; i < 5; i++) {
            //TIP Press <shortcut actionId="Debug"/> to start debugging your code. We have set one <icon src="AllIcons.Debugger.Db_set_breakpoint"/> breakpoint
            // for you, but you can always add more by pressing <shortcut actionId="ToggleLineBreakpoint"/>.
            System.out.println("the value of i = " + i);
        }
    }

    private static void sayHello(String name)
    {

        System.out.println("Hello and welcome!"+name);
    }
}
