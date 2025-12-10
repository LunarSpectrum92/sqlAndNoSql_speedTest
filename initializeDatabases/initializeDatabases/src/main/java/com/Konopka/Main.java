package com.Konopka;

import java.io.*;
import java.util.LinkedHashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {

    public static void main(String[] args) throws IOException {

        boolean stop = true;

        while (stop) {
            System.out.println("1: zainicjalizuj pliki1\n" +
                    "2: zainicjalizuj postgresql\n" +
                    "3: zainicjalizuj bazeNoSql\n" +
                    "4: zainicjalizuj cassandra old\n" +
                    "5: zainicjalizuj mysql\n" +
                    "6: zamknij program\n" +
                    "wybierz opcje:");

            Scanner sc = new Scanner(System.in);
            int choice = sc.nextInt();

            switch (choice) {
                case 1:
                    deleteBranches_EngDuplicates();
                    processFile(
                            Config.get("csv.orders"),
                            Config.get("csv.orders.final")
                    );
                    processFile(
                            Config.get("csv.orderdetails"),
                            Config.get("csv.orderdetails.final")
                    );
                    break;
                case 2:
                    PostgreSql.getInstance();
                    break;

                case 3:
                    CassandraNew.getInstance();
                    break;

                case 4:
                    CassandraOld.getInstance();
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

    public static void deleteBranches_EngDuplicates() {

        String inputPath = Config.get("csv.branches");

        Set<String> lines = new LinkedHashSet<>();
        Set<String> ids = new LinkedHashSet<>();

        try (BufferedReader br = new BufferedReader(new FileReader(inputPath))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (ids.add(line.split(",")[0])) {
                    lines.add(line);
                }
            }
        } catch (IOException e) {
            System.out.println("Błąd podczas odczytu Branches: " + e.getMessage());
            return;
        }

        try (PrintWriter pw = new PrintWriter(new File(inputPath))) {
            lines.forEach(pw::println);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        System.out.println("Duplicates removed. Output saved to: " + inputPath);
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
