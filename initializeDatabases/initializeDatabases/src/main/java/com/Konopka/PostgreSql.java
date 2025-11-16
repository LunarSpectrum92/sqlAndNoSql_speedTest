package com.Konopka;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;

public class PostgreSql {
    private static final String URL = "jdbc:postgresql://localhost:5432/SqlSpeedTest";
    private static final String USER = "admin";
    private static final String PASSWORD = "admin123";
    private static PostgreSql instance;
    private static Connection conn;

    private PostgreSql(int x) {
        if(x == 2){
            getConnection();
        }else{
            getConnection();
            //dropTables();
            //Branches();
            //Categories_ENG();
            //Customers_ENG();
            //Orders();
            Order_Details();
        }
    }

    public static Connection getConnection() {
        if (conn == null) {
            try {
                conn = DriverManager.getConnection(URL, USER, PASSWORD);
                System.out.println("Połączono z bazą!");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return conn;
    }


    public static PostgreSql getInstance(int x) {
        if (instance == null) {
            instance = new PostgreSql(x);
            System.out.println("instancja wywolana");
        }
        return instance;
    }


    public static void closeConnection() {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void Branches(){
        String createTable = """
    CREATE TABLE Branches (
        BRANCH_ID VARCHAR(100) PRIMARY KEY,
        REGION VARCHAR(100),
        CITY VARCHAR(100),
        TOWN VARCHAR(100),
        BRANCH_TOWN VARCHAR(100),
        LAT BIGINT,
        LON BIGINT
    )
""";

        try {
            Statement stmt = conn.createStatement();
            int rs = stmt.executeUpdate(createTable);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        loadCsv("Branches", "/tmp/import/Branches.csv", ",");
//
//
//
//        try{
//            CopyManager copyManager = new CopyManager((BaseConnection) conn);
//
//        try (BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Jarek\\Documents\\projekty\\sqlAndNoSql_speedTest\\data\\Branches.csv"))) {
//                copyManager.copyIn("COPY Branches FROM STDIN WITH CSV HEADER DELIMITER ','", br);
//                System.out.println("Data has been copied from CSV to the table successfully.");
//            } catch (Exception e) {
//                System.out.println("Error reading CSV file: " + e.getMessage());
//            }
//        } catch (SQLException e) {
//            System.out.println("Connection failure: " + e.getMessage());
//        }
    }



    public static void Categories_ENG(){
        String createTable = """
    CREATE TABLE Categories_ENG (
           ITEMID SERIAL PRIMARY KEY,
                                  CATEGORY1 VARCHAR(100),
                                  CATEGORY1_ID VARCHAR(50),
                                  CATEGORY2 VARCHAR(100),
                                  CATEGORY2_ID VARCHAR(50),
                                  CATEGORY3 VARCHAR(100),
                                  CATEGORY3_ID VARCHAR(50),
                                  CATEGORY4 VARCHAR(100),
                                  CATEGORY4_ID VARCHAR(50),
                                  BRAND VARCHAR(100),
                                  ITEMCODE VARCHAR(100),
                                  ITEMNAME VARCHAR(100)
    )
""";


        try {
            Statement stmt = conn.createStatement();
            int rs = stmt.executeUpdate(createTable);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        loadCsv("Categories_ENG", "/tmp/import/Categories_ENG.csv", ";");


        //
//        try{
//            CopyManager copyManager = new CopyManager((BaseConnection) conn);
//
//            try (BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Jarek\\Documents\\projekty\\sqlAndNoSql_speedTest\\data\\Categories_ENG.csv"))) {
//                copyManager.copyIn("COPY Categories_ENG FROM STDIN WITH CSV HEADER DELIMITER ';'", br);
//                System.out.println("Data has been copied from CSV to the table successfully.");
//            } catch (Exception e) {
//                System.out.println("Error reading CSV file: Categories_ENG" + e.getMessage());
//            }
//        } catch (SQLException e) {
//            System.out.println("Connection failure: Categories_ENG " + e.getMessage());
//        }
//

    }

    public static void Customers_ENG(){
        String createTable = """
    CREATE TABLE Customers_ENG (
        USERID SERIAL PRIMARY KEY,
        USERNAME_ VARCHAR(100),
        NAMESURNAME VARCHAR(100),
        STATUS_ BOOLEAN,
        USERGENDER CHAR(1),
        USERBIRTHDATE DATE,
        REGION VARCHAR(50),
        CITY VARCHAR(50),
        TOWN VARCHAR(50),
        DISTRICT VARCHAR(50),
        ADDRESSTEXT TEXT
    )
""";

        try {
            Statement stmt = conn.createStatement();
            int rs = stmt.executeUpdate(createTable);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        loadCsv("Customers_ENG", "/tmp/import/Customers_ENG.csv", ";");


//
//        try{
//            CopyManager copyManager = new CopyManager((BaseConnection) conn);
//
//            try (BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Jarek\\Documents\\projekty\\sqlAndNoSql_speedTest\\data\\Customers_ENG.csv"))) {
//                copyManager.copyIn("COPY Customers_ENG FROM STDIN WITH CSV HEADER DELIMITER ';'", br);
//                System.out.println("Data has been copied from CSV to the table successfully.");
//            } catch (Exception e) {
//                System.out.println("Error reading CSV file: " + e.getMessage());
//            }
//        } catch (SQLException e) {
//            System.out.println("Connection failure: " + e.getMessage());
//        }

    }




    public static void Orders(){
        String createTable = """
    CREATE TABLE Orders (
        ORDERID BIGINT PRIMARY KEY,
        BRANCH_ID VARCHAR(20) REFERENCES Branches(BRANCH_ID),
        DATE_ TIMESTAMP,
        USERID BIGINT REFERENCES Customers_ENG(USERID),
        NAMESURNAME VARCHAR(100),
        TOTALBASKET VARCHAR(50)
    )
""";



        try {
            Statement stmt = conn.createStatement();
            int rs = stmt.executeUpdate(createTable);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        loadCsv("Orders", "/tmp/import/Orders_final.csv", ",");


//
//        try{
//            CopyManager copyManager = new CopyManager((BaseConnection) conn);
//        try (BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Jarek\\Documents\\projekty\\sqlAndNoSql_speedTest\\data\\Orders.csv"))) {
//            copyManager.copyIn("COPY Orders FROM STDIN WITH CSV HEADER DELIMITER ','", br);
//            System.out.println("Data has been copied from CSV to the table successfully.");
//        } catch (Exception e) {
//            System.out.println("Error reading CSV file: " + e.getMessage());
//        }
//    } catch (SQLException e) {
//        System.out.println("Connection failure: " + e.getMessage());
//    }

    }

    public static void Order_Details(){
        String createTable = """
    CREATE TABLE Order_Details (
        ORDERID BIGINT REFERENCES Orders(ORDERID),
        ORDERDETAILID BIGINT PRIMARY KEY,
        AMOUNT INT,
        UNITPRICE VARCHAR(50),
        TOTALPRICE VARCHAR(50),
        ITEMID BIGINT REFERENCES Categories_ENG(ITEMID),
        ITEMCODE BIGINT
    )
""";

        try {
            Statement stmt = conn.createStatement();
            int rs = stmt.executeUpdate(createTable);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        loadCsv("Order_Details", "/tmp/import/Order_Details_final.csv", ",");


//
//        try{
//            CopyManager copyManager = new CopyManager((BaseConnection) conn);
//
//            try (BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Jarek\\Documents\\projekty\\sqlAndNoSql_speedTest\\data\\Order_Details.csv"))) {
//                copyManager.copyIn("COPY Order_Details FROM STDIN WITH CSV HEADER DELIMITER ','", br);
//                System.out.println("Data has been copied from CSV to the table successfully.");
//            } catch (Exception e) {
//                System.out.println("Error reading CSV file: " + e.getMessage());
//            }
//        } catch (SQLException e) {
//            System.out.println("Connection failure: " + e.getMessage());
//        }
    }


    private static void loadCsv(String tableName, String filePath, String delimiter) {
        String copyCommand = String.format(
                "COPY %s FROM '%s' WITH (FORMAT csv, HEADER true, DELIMITER '%s')",
                tableName, filePath, delimiter
        );

        try (Statement stmt = conn.createStatement()) {

            stmt.execute(copyCommand);
            System.out.println("PostgreSQL successfully loaded CSV from: " + filePath);

        } catch (SQLException e) {
            System.out.println("COPY error: " + e.getMessage());
        }
    }





    public static void dropTables() {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("DROP TABLE IF EXISTS Order_Details CASCADE");
            stmt.executeUpdate("DROP TABLE IF EXISTS Orders CASCADE");
            stmt.executeUpdate("DROP TABLE IF EXISTS Branches CASCADE");
            stmt.executeUpdate("DROP TABLE IF EXISTS Categories_ENG CASCADE");
            stmt.executeUpdate("DROP TABLE IF EXISTS Customers_ENG CASCADE");
            System.out.println("All tables dropped successfully.");
        } catch (SQLException e) {
            throw new RuntimeException("Error dropping tables: " + e.getMessage(), e);
        }
    }


}
