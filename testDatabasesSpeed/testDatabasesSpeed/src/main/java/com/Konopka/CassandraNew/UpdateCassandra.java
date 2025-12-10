package com.Konopka.CassandraNew;

import com.Konopka.Interfaces.UpdateInterface;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import java.util.List;

public class UpdateCassandra implements UpdateInterface {


    public static CqlSession session;

    public UpdateCassandra(CqlSession session) {
        UpdateCassandra.session = session;
    }


    @Override
    public Long updateEmployeeTable(List<Row> rows) {
        try{
            System.out.println("start modyfikowania tabeli employees_by_branch za pomocą Batch");
            PreparedStatement prepared = session.prepare("""
    UPDATE cassandraSpeedTestDb.employees_by_branch
    SET last_name = 'zbyszek'
    WHERE branch_id = ? AND employeeid = ?;
    """);
            BatchStatement batch = BatchStatement.builder(DefaultBatchType.UNLOGGED)
                    .build();

            int counter = 0;

            Long startTime = System.nanoTime();
            for(Row row : rows){
                BoundStatement bound = prepared.bind(row.getString("branch_id"), row.getUuid("employeeid"));
                batch = batch.add(bound);
                counter++;
                if (counter % 100 == 0) {
                    session.execute(batch);
                    batch = BatchStatement.builder(DefaultBatchType.UNLOGGED).build(); // Nowy batch
                }
            }
            if (!(batch.size() != 0)) {
                session.execute(batch);
            }
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
        public Long updateRecordInEmployeeTable(Row row) {
            PreparedStatement preparedDelete = session.prepare("""
        UPDATE cassandraSpeedTestDb.employees_by_branch
        SET last_name = 'krzysiu'
        WHERE branch_id = ? AND employeeid = ?;
        """);
            try{
                System.out.println("start zmiany danych w tabeli employees_by_branch");
                Long startTime = System.nanoTime();
                BoundStatement bound = preparedDelete.bind(row.getString("branch_id"), row.getUuid("employeeid"));
                session.execute(bound);
                Long endTime = System.nanoTime();
                System.out.println("Pomyślnie zmieniono dane dla branch_id: " + row.getString("branch_id") + " i employeeid: " + row.getUuid("employeeid"));
                System.out.println("koniec modyfikacji danych w tabeli employees_by_branch czas dzialania programu wyniosl: " + (endTime - startTime));
                return endTime - startTime;
            }catch(Exception e){
                System.err.println("nie udalo sie zmodyfikowac danych w tabeli employees_by_branch");
                e.printStackTrace();
            }
            return null;
        }


}
