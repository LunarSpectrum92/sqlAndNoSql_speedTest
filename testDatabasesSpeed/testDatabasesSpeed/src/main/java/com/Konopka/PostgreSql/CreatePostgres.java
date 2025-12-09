package com.Konopka.PostgreSql;

import com.Konopka.Interfaces.CreateInterface;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class CreatePostgres implements CreateInterface {

    public static Connection connection;

    public CreatePostgres(Connection connection) {
        CreatePostgres.connection = connection;
    }

    @Override
    public Long CreateEmployee() {
        try {
            System.out.println("start tworzenia tabeli employees_by_branch");
            Long startTime = System.nanoTime();
            var statement = connection.createStatement();
            statement.execute("""
                CREATE TABLE IF NOT EXISTS employees_by_branch (
                    branch_id VARCHAR(255),
                    employeeid UUID,
                    first_name TEXT,
                    last_name TEXT,
                    position TEXT,
                    salary NUMERIC, 
                    email TEXT,
                    phone_number TEXT,
                    PRIMARY KEY (employeeid),
                    FOREIGN KEY (branch_id) REFERENCES branches(branch_id)
                );
                """);
            statement.close();
            Long endTime = System.nanoTime();
            System.out.println("koniec tworzenia tabeli employees_by_branch, czas dzialanie programu wyniosl: " + (endTime - startTime) / 1_000_000.0 + " ms");
            return endTime - startTime;
        } catch (SQLException e) {
            System.err.println("nie udalo sie stworzyc tabeli employees_by_branch");
            e.printStackTrace();
        }
        return null;
    }


    @Override
    public Long InsertEmployee(List<String> values) {
        System.out.println("start wstawiania do tabeli employees_by_branch");
        Long startTime = System.nanoTime();
        try {
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            System.err.println("Nie udało się wyłączyć autocommit.");
            e.printStackTrace();
            return null;
        }
        try (Statement statement = connection.createStatement()) {
            for (String value : values) {
                statement.addBatch(value);
            }
            int[] updateCounts = statement.executeBatch();
            connection.commit();

            Long endTime = System.nanoTime();
            System.out.println("koniec wstawiania do tabeli employees_by_branch, czas dzialanie programu wyniosl: " + (endTime - startTime) + " nanosekund.");
            return endTime - startTime;
        } catch (SQLException e) {
            System.err.println("Nie udało się wstawić do tabeli employees_by_branch.");
            e.printStackTrace();
            try {
                if (connection != null) {
                    connection.rollback();
                }
            } catch (SQLException ex) {
                System.err.println("Nie udało się wycofać transakcji.");
                ex.printStackTrace();
            }
        } finally {
            try {
                if (connection != null) {
                    connection.setAutoCommit(true);
                }
            } catch (SQLException e) {
                System.err.println("Nie udało się przywrócić autocommit.");
                e.printStackTrace();
            }
        }
        return null;
    }
}
