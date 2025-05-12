//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Demo02 {
    public static void main(String[] args) {
        String sampletext = "apple";
        // StringBuilder result = new StringBuilder();
        String result;
        Demo02 m1 = new Demo02();
        result = m1.toUpperCase(sampletext);
        System.out.println(result);
    }

    // Method names and variable names follows camel case
    //pascal for interface & class
    public String toUpperCase(String word){
        //String lower_case = "prasanna";
        StringBuilder upperWord = new StringBuilder();
        for(int i = 0;i<word.length();i++){
            char letter = word.charAt(i);
            if(letter >= 'a' && letter <= 'z'){
                letter = (char)(letter-32);
            }
            upperWord.append(letter);
        }
        //word = "kondoju";
              /*String new_word = "James";
                word = word.toUpperCase();
                System.out.println(word);
                new_word = new_word+" "+"Gosling";
                System.out.println(new_word);*/

        return upperWord.toString();
    }

}
