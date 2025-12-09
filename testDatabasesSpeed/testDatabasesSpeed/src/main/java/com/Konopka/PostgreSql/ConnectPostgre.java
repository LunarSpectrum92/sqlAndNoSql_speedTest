package com.Konopka.PostgreSql;

import com.Konopka.Connectors.PostgreConnection;
import com.Konopka.Interfaces.ConnectInterface;
import com.datastax.oss.driver.api.core.CqlSession;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectPostgre implements ConnectInterface{

    private static final String URL = "jdbc:postgresql://localhost:5432/SqlSpeedTest";
    private static final String USER = "admin";
    private static final String PASSWORD = "admin123";
    private static Connection conn;

    public ConnectPostgre(){
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
