package com.Konopka.MySql;

import com.Konopka.Interfaces.ConnectInterface;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectMySql implements ConnectInterface{

    private static final String URL = "jdbc:mysql://localhost:3306/SqlSpeedTest?allowLoadLocalInfile=true";
    private static final String USER = "root";
    private static final String PASSWORD = "admin";
    private static Connection conn;

    public ConnectMySql(){
        ConnectToDatabase();
    }

    public Connection getConnection(){
        return conn;
    }

    @Override
    public void ConnectToDatabase() {
            try {
                conn = DriverManager.getConnection(URL, USER, PASSWORD);
                System.out.println("Połączono z bazą!");
            } catch (SQLException e) {
                System.err.println("Nie udalo sie polaczyc z baza danych");
                e.printStackTrace();
            }
    }

    @Override
    public void CloseConnection() {
        try {
            conn.close();
            System.out.println("Close connection!");
        } catch (SQLException e) {
            System.err.println("nie udało sie zamknac polaczenia");
            e.printStackTrace();
        }
    }
}
