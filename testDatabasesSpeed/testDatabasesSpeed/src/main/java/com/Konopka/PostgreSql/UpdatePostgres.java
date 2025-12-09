package com.Konopka.PostgreSql;

import com.Konopka.Interfaces.UpdateInterface;
import com.datastax.oss.driver.api.core.cql.Row;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;

public class UpdatePostgres implements UpdateInterface {

    public static Connection connection;

    public UpdatePostgres(Connection connection){
        UpdatePostgres.connection = connection;
    }


    @Override
    public Long updateEmployeeTable(List<Row> rows) {
        try(Statement statement = connection.createStatement()){
            System.out.println("start update employee table");
            Long startTime = System.nanoTime();
            statement.execute("""
            UPDATE employees_by_branch SET last_name='zbyszek';
            """);
            Long endTime = System.nanoTime();
            System.out.println("koniec update employee table czas dzialanie: " + (endTime - startTime) );
            return endTime - startTime;
        }catch (Exception e){
            System.err.println("Nie udalo sie wykonac update na tabeli employees_by_branch");
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Long updateRecordInEmployeeTable(Row row) {
        String updateSql = """
        UPDATE employees_by_branch
        SET last_name = 'krzysiu'
        WHERE branch_id = ? AND employeeid = ?;
        """;
        try (PreparedStatement preparedUpdate = connection.prepareStatement(updateSql)) {
            System.out.println("start zmiany danych w tabeli employees_by_branch (PostgreSQL)");
            Long startTime = System.nanoTime();
            String branchId = row.getString("branch_id");
            UUID employeeId = row.getUuid("employeeid");
            preparedUpdate.setString(1, branchId);
            preparedUpdate.setObject(2, employeeId, java.sql.Types.OTHER);
            int rowsAffected = preparedUpdate.executeUpdate();
            Long endTime = System.nanoTime();
            if (rowsAffected > 0) {
                System.out.println("Pomyślnie zmieniono dane dla branch_id: " + branchId + " i employeeid: " + employeeId);
            } else {
                System.out.println("Brak rekordów do modyfikacji dla branch_id: " + branchId + " i employeeid: " + employeeId);
            }
            System.out.println("koniec modyfikacji danych w tabeli employees_by_branch czas dzialania programu wyniosl: " + (endTime - startTime));
            return endTime - startTime;
        } catch (SQLException e) {
            System.err.println("Nie udało się zmodyfikować danych w tabeli employees_by_branch w PostgreSQL");
            e.printStackTrace();
        }
        return null;
    }
}
