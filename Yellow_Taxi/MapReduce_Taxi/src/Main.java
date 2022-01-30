import java.util.Scanner;

public class Main {
    public static void main(String... a) throws Exception {
        Scanner user_input = new Scanner(System.in);
        System.out.println("Provide your new project name: ");
        String project_name = user_input.next();
        Preparation.main(new String[]{project_name});
        PassengerCount.main(new String[]{project_name+"/input/*.csv", project_name+"/output/"});
    }
}
