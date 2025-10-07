package com.Konopka;



import java.io.*;
import java.util.LinkedHashSet;
import java.util.Scanner;
import java.util.Set;

public class    Main {
    public static void main(String[] args) {
        boolean stop = true;
        while(stop){
            System.out.println("1: zainicjalizuj bazeSql\n" + "2: zainicjalizuj bazeNoSql\n" + "3: zamknij program\n" + "wybierz opcje:");
            Scanner sc = new Scanner(System.in);
            int choice = sc.nextInt();
            switch (choice){
                case 1:
                    deleteBranches_EngDuplicates();
                    PostgreSql.getInstance(1);
                    break;
                case 2:
                    PostgreSql.getInstance(2);
                    Cassandra.getInstance();
                    break;
                case 3:
                    stop = false;
                    break;
            }
        }

    }

    //nadpisuje pozycje które miały zduplikowane id
    public static void deleteBranches_EngDuplicates() {
        Set<String> lines = new LinkedHashSet<>();
        Set<String> ids = new LinkedHashSet<>();

        try (BufferedReader br = new BufferedReader(new FileReader(
                "C:\\Users\\Jarek\\Documents\\projekty\\sqlAndNoSql_speedTest\\data\\Branches.csv"))) {
            String line;
            while ((line = br.readLine()) != null) {
                if(ids.add(line.split(",")[0])) {
                    lines.add(line);
                }
            }
        } catch (IOException e) {
            System.out.println("Connection failure: " + e.getMessage());
            return;
        }

        File csvOutputFile = new File(
                "C:\\Users\\Jarek\\Documents\\projekty\\sqlAndNoSql_speedTest\\data\\Branches.csv"
        );
        try (PrintWriter pw = new PrintWriter(csvOutputFile)) {
            lines.forEach(pw::println);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        System.out.println("Duplicates removed. Output saved to: " + csvOutputFile.getAbsolutePath());
    }
}
