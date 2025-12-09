package com.Konopka;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConvertToCsv {



    public List<String> listMillisecondsToTime(List<Long> nanoseconds) {
        if (nanoseconds == null || nanoseconds.isEmpty()) {
            return null;
        }
        List<String> list = new ArrayList<>();
        for (Long millisecond : nanoseconds) {
            if (millisecond == null) {
                list.add("BŁĄD");
            } else {
                list.add(nanosecondsToTime(millisecond));
            }
        }
        return list;
    }


    public String nanosecondsToTime(Long nanoseconds) {
        if (nanoseconds == null || nanoseconds < 0) {
            return "BŁĄD,0,0";
        }

        long NANOS_IN_SECOND = 1_000_000_000L;
        long NANOS_IN_MINUTE = NANOS_IN_SECOND * 60;

        long minutes = nanoseconds / NANOS_IN_MINUTE;
        long remainingNanos = nanoseconds % NANOS_IN_MINUTE;

        long seconds = remainingNanos / NANOS_IN_SECOND;
        remainingNanos = remainingNanos % NANOS_IN_SECOND;

        long milliseconds = remainingNanos / 1_000_000L;
        long remainingNanosFinal = remainingNanos % 1_000_000L;

        return String.format("%d,%d,%03d", minutes, seconds, milliseconds);
    }
    public String escapeSpecialCharacters(String data) {
        if (data == null) {
            throw new IllegalArgumentException("Input data cannot be null");
        }
        String escapedData = data.replaceAll("\\R", " ");
        if (escapedData.contains(",") || escapedData.contains("\"") || escapedData.contains("'")) {
            escapedData = escapedData.replace("\"", "\"\"");
            escapedData = "\"" + escapedData + "\"";
        }
        return escapedData;
    }



    public void addStringListToCsv(List<String> dataLines) throws IOException {

        try (PrintWriter pw = new PrintWriter(
                new OutputStreamWriter(new FileOutputStream("C:\\Users\\Jarek\\Documents\\projekty\\sqlAndNoSql_speedTest\\testDatabasesSpeed\\testDatabasesSpeed\\src\\main\\java\\com\\Konopka\\Wyniki.csv", true), StandardCharsets.UTF_8)
        )
        ) {
            if (dataLines != null) {
                pw.println(convertToCSV(dataLines));
            }
        }
    }


    public String convertToCSV(List<String> data) {
        return data.stream()
                .map(this::escapeSpecialCharacters)
                .collect(Collectors.joining(";"));
    }


}
