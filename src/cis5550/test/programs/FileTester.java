package cis5550.test.programs;

import java.io.File;

public class FileTester {
    public static void main(String[] args) {
        System.out.println(System.getProperty("user.dir"));
        File file = new File("./frontend/SearchPage.html");
        System.out.println(file.exists());
    }
}
