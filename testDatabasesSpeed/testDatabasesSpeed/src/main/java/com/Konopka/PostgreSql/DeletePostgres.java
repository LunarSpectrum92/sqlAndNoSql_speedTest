package com.Konopka.PostgreSql;

import com.Konopka.Interfaces.DeleteInterface;
import com.datastax.oss.driver.api.core.cql.Row;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Objects;
import java.util.UUID;

public class DeletePostgres implements DeleteInterface {

    private static Connection connection;

    public DeletePostgres(Connection connection) {
        DeletePostgres.connection = connection;
    }


    @Override
    public Long deleteTableRow(Row row) {
        String sql = """
                DELETE FROM employees_by_branch
                WHERE  branch_id = ? AND employeeid = ?;
                """;
        try(PreparedStatement statement = connection.prepareStatement(sql)){
            System.out.println("start usuwania danych z tabeli employees_by_branch");
            Long startTime = System.nanoTime();
            String branchId = row.getString("branch_id");
            UUID employeeId = row.getUuid("employeeid");
            statement.setString(1, branchId);
            statement.setObject(2, employeeId, java.sql.Types.OTHER);
            statement.execute();
            Long endTime = System.nanoTime();
            System.out.println("koniec usuwania danych z tabeli employees_by_branch czas dzialania programu wyniosl: " + (endTime - startTime));
            return endTime - startTime;
        }catch (Exception e){
            System.out.println("usuwanie danych z tabeli employees_by_branch niepowiodlo sie");
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Long deleteAllTableContent() {
        try(Statement statement = connection.createStatement()) {
            System.out.println("start usuwania danych z tabeli employees_by_branch");
            Long startTime = System.nanoTime();
            statement.execute("""
                    DELETE FROM employees_by_branch;
                    """);
            Long endTime = System.nanoTime();
            System.out.println("koniec usuwania danych z tabeli employees_by_branch czas dzialania programu wyniosl: " + (endTime - startTime));
            return endTime - startTime;
        }catch(Exception e){
            System.err.println("nie udalo sie usunac danych z tabeli employees_by_branch");
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Long deleteTable() {
        try(Statement statement = connection.createStatement()) {
            System.out.println("start usuwania tabeli employees_by_branch");
            Long startTime = System.nanoTime();
            statement.execute("""
                    DROP TABLE employees_by_branch;
                    """);
            Long endTime = System.nanoTime();
            System.out.println("koniec usuwania tabeli employees_by_branch czas dzialania programu wyniosl: " + (endTime - startTime));
            return endTime - startTime;
        }catch(Exception e){
            System.err.println("nie udalo sie usunac danych z tabeli employees_by_branch");
            e.printStackTrace();
        }
        return null;
    }
}
