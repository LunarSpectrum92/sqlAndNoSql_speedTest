package com.Konopka.CassnadraOld;

import com.Konopka.Interfaces.DeleteInterface;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;

import java.util.Objects;

public class DeleteCassandraOld implements DeleteInterface {


    public static CqlSession session;

    public DeleteCassandraOld(CqlSession session) {
        DeleteCassandraOld.session = session;
    }


    @Override
    public Long deleteTableRow(Row row) {
        PreparedStatement preparedDelete = session.prepare("""
        DELETE FROM cassandraSpeedTestDb.employees_by_branch
        WHERE branch_id = ? AND employeeid = ?;
    """);
        try{
            System.out.println("start usuwania danych z tabeli employees_by_branch");
            Long startTime = System.nanoTime();
            BoundStatement bound = preparedDelete.bind(row.getString("branch_id"), row.getUuid("employeeid"));
            session.execute(bound);
            Long endTime = System.nanoTime();
            System.out.println("Pomyślnie usunięto dane dla branch_id: " + row.getString("branch_id") + " i employeeid: " + row.getUuid("employeeid"));
            System.out.println("koniec usuwania danych z tabeli employees_by_branch czas dzialania programu wyniosl: " + (endTime - startTime));
            return endTime - startTime;
        }catch(Exception e){
            System.err.println("nie udalo sie usunac danych z tabeli employees_by_branch");
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Long deleteAllTableContent() {
        try{
            System.out.println("start usuwania danych z tabeli employees_by_branch");
            Long startTime = System.nanoTime();
            var resultSet = session.execute("""
                    TRUNCATE cassandraSpeedTestDb.employees_by_branch;
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
        try{
            System.out.println("start usuwania tabeli employees_by_branch");
            Long startTime = System.nanoTime();
            session.execute("""
                    DROP TABLE cassandraSpeedTestDb.employees_by_branch;   
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
