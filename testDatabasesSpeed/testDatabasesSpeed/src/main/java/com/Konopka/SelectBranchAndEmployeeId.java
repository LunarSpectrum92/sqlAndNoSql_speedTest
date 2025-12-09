package com.Konopka;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;

import java.util.List;

public class SelectBranchAndEmployeeId {


    public List<Row> selectIds(CqlSession session) {

        try{
            var resultSet = session.execute("SELECT branch_id, employeeid FROM cassandraSpeedTestDb.employees_by_branch");
            List<Row> rows = resultSet.all();
            return rows;

        }catch(Exception e){
            e.printStackTrace();
        }
        return null;

    }


    public List<Row> selectBranchIds(CqlSession session) {

        try{
            var resultSet = session.execute("SELECT branch_id FROM cassandraSpeedTestDb.branches");
            List<Row> rows = resultSet.all();
            return rows;

        }catch(Exception e){
            e.printStackTrace();
        }
        return null;

    }





}
