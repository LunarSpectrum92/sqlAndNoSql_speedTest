package com.Konopka.Interfaces;

import com.datastax.oss.driver.api.core.cql.Row;

import java.util.List;

public interface DeleteInterface {

    public Long deleteTableRow(Row row);
    public Long deleteAllTableContent();
    public Long deleteTable();

}
