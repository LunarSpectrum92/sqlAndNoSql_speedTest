package com.Konopka.CassandraNew;

import com.Konopka.Interfaces.CreateInterface;
import com.datastax.oss.driver.api.core.CqlSession;

import java.util.List;


public class CreateCassandra implements CreateInterface {

    public static CqlSession session;

    public CreateCassandra(CqlSession session) {
        CreateCassandra.session = session;
    }

    @Override
    public Long CreateEmployee() {
        try{
            System.out.println("start tworzenia tabeli employees_by_branch");
            Long startTime = System.nanoTime();
            var resultSet = session.execute("""
                    CREATE TABLE IF NOT EXISTS cassandraSpeedTestDb.employees_by_branch (
                        branch_id text,
                        employeeid UUID,
                        first_name text,
                        last_name text,
                        position text,
                        salary decimal,
                        email text,
                        phone_number text,
                        PRIMARY KEY (branch_id, employeeid)
                    );
                    """);
            Long endTime = System.nanoTime();
            System.out.println("koniec tworzenia tabeli employees_by_branch, czas dzialanie programu wyniosl: " + (endTime - startTime));
            return endTime - startTime;
        }catch(Exception e){
            System.err.println("nie udalo sie stworzyc tabeli employees_by_branch");
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Long InsertEmployee(List<String> values) {
        try{
            System.out.println("start wstawiania do tabeli employees_by_branch");
            Long startTime = System.nanoTime();
            for(String value : values){
                session.execute("BEGIN BATCH\n" +
                        value
                        + "APPLY BATCH");
            }
            Long endTime = System.nanoTime();
            System.out.println("koniec wstawiania do tabeli employees_by_branch, czas dzialanie programu wyniosl: " + (endTime - startTime));
            return endTime - startTime;
        }catch(Exception e){
            System.err.println("nie udalo sie wstawic do tabeli employees_by_branch");
            e.printStackTrace();
        }
        return null;
    }



}