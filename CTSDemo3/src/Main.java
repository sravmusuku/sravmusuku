import java.util.List;
import java.util.ArrayList;
public class Main {

    public static void main(String[] args) {
        try{
        String[] customerNames = new String[5];
        customerNames[0] = "Sravani";
        customerNames[1] = "Sirisha";
        customerNames[2] = "Shekar";
        customerNames[3] = "Pushpa";
        customerNames[4] = "venkat";
        for (int i = 0; i < customerNames.length; i++) {
            System.out.println(customerNames[i]);
        }
        List<String> customerCities = new ArrayList<>();
        customerCities.add("Hyderabad");
        customerCities.add("Banglore");
        customerCities.add("Dubai");
        customerCities.add("Mumbai");
        customerCities.add("Delhi");
        System.out.println(customerCities);
        System.out.println("Element in position 3:," + customerCities.get(3));
        customerCities.set(3, "pune");
        System.out.println(customerCities);
        customerCities.add("Dublin");
        System.out.println(customerCities);
        boolean isAvailable = customerCities.contains("Dubai");
        System.out.println(isAvailable);
        System.out.println("Number of cities:" + customerCities.size());
        customerCities.remove(4);
        customerCities.remove("Dubai");
        System.out.println(customerCities);
        for (String city : customerCities) {
            System.out.println(city + ",");
        }
        customerCities.clear();
        System.out.println(customerCities);
    } catch (Exception e) {
            System.out.println("something went wrong,"+e.getMessage());
        }




    }
}
