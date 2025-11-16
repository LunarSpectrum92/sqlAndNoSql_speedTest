package com.Konopka;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.Scanner;
import java.util.Set;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {
    public static void main(String[] args) throws IOException {
        boolean stop = true;
        while(stop){
            System.out.println("1: zainicjalizuj pliki1\n" + "2: zainicjalizuj postgresql\n" + "3: zainicjalizuj bazeNoSql (żeby być w stanie zainicjalizować cassandre wykonaj najpierw krok 1)\n" + "4: zainicjalizuj cassandra old\n" + "5: zainicjalizuj mysql\n" + "6: zamknij program\n" + "wybierz opcje:");
            Scanner sc = new Scanner(System.in);

            int choice = sc.nextInt();
            switch (choice){
                case 1:
                    deleteBranches_EngDuplicates();
                    processFile("C:\\Users\\Jarek\\Documents\\projekty\\sqlAndNoSql_speedTest\\import\\Orders.csv", "C:\\Users\\Jarek\\Documents\\projekty\\sqlAndNoSql_speedTest\\import\\Orders_final.csv");
                    processFile("C:\\Users\\Jarek\\Documents\\projekty\\sqlAndNoSql_speedTest\\import\\Order_Details.csv", "C:\\Users\\Jarek\\Documents\\projekty\\sqlAndNoSql_speedTest\\import\\Order_Details_final.csv");
                    break;
                case 2:
                    PostgreSql.getInstance(1);
                    break;
                case 3:
                    PostgreSql.getInstance(2);
                    Cassandra.getInstance();
                    break;
                case 4:
                    PostgreSql.getInstance(2);
                    Cassandra_old.getInstance();
                    break;
                case 5:
                    MySql.getInstance();
                    break;
                case 6:
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
                "C:\\Users\\Jarek\\Documents\\projekty\\sqlAndNoSql_speedTest\\import\\Branches.csv"))) {
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
                "C:\\Users\\Jarek\\Documents\\projekty\\sqlAndNoSql_speedTest\\import\\Branches.csv"
        );
        try (PrintWriter pw = new PrintWriter(csvOutputFile)) {
            lines.forEach(pw::println);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        System.out.println("Duplicates removed. Output saved to: " + csvOutputFile.getAbsolutePath());
    }


    public static void processFile(String inputFile, String outputFile) throws IOException {
        Pattern pattern = Pattern.compile("\"([^\"]*)\"");

        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile));
             BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {

            String line;
            while ((line = reader.readLine()) != null) {

                if (line.startsWith("ORDERID")) {
                    writer.write(line);
                    writer.newLine();
                    continue;
                }

                Matcher matcher = pattern.matcher(line);
                StringBuffer processedLineBuffer = new StringBuffer();

                while (matcher.find()) {
                    String contentInQuotes = matcher.group(1);
                    String replacedContent = contentInQuotes.replace(',', '.');
                    matcher.appendReplacement(processedLineBuffer, replacedContent);
                }

                matcher.appendTail(processedLineBuffer);

                writer.write(processedLineBuffer.toString());
                writer.newLine();
            }
        }
    }

}




