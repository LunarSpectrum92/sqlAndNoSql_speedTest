package com.Konopka.Interfaces;

import com.datastax.oss.driver.api.core.cql.Row;

import java.util.List;

public interface UpdateInterface {

    public Long updateEmployeeTable(List<Row> rows);
    public Long updateRecordInEmployeeTable(Row row);
}
