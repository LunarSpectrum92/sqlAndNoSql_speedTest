package com.Konopka.Connectors;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class PostgreConnection {

    private static final String URL = "jdbc:postgresql://localhost:5432/SqlSpeedTest";
    private static final String USER = "admin";
    private static final String PASSWORD = "admin123";
    private static PostgreConnection instance;
    private static Connection conn;

    public static Connection getConn() {
        return conn;
    }


    private PostgreConnection() {
            getConnection();
    }

    private static void getConnection() {
        if (conn == null) {
            try {
                conn = DriverManager.getConnection(URL, USER, PASSWORD);
                System.out.println("Połączono z bazą!");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


    public static PostgreConnection getInstance() {
        if (instance == null) {
            instance = new PostgreConnection();
            System.out.println("instancja wywolana");
        }
        return instance;
    }



}
