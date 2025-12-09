package com.Konopka;

import com.Konopka.CassandraNew.*;
import com.Konopka.CassnadraOld.*;
import com.Konopka.MySql.*;
import com.Konopka.PostgreSql.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class Main {
    public static void main(String[] args) throws IOException {

        List<String> timeResults = new ArrayList<>(52);
        List<Long> nanoseconds = new ArrayList<>();
        List<String> firstCsvLine = List.of(
                "Cassandra_New_SelectOrders",
                "Cassandra_New_SelectOrderDetails",
                "Cassandra_New_SelectCustomers",
                "Cassandra_New_SelectCustomerOrderDetails",
                "Cassandra_New_CountOrdersForCustomer",
                "Cassandra_New_SelectAll",
                "Cassandra_New_CreateEmployee",
                "Cassandra_New_InsertEmployees",
                "Cassandra_New_UpdateEmployeeTable",
                "Cassandra_New_UpdateRecord",
                "Cassandra_New_DeleteRow",
                "Cassandra_New_DeleteAll",
                "Cassandra_New_DeleteTable",
                "Cassandra_Old_SelectOrders",
                "Cassandra_Old_SelectOrderDetails",
                "Cassandra_Old_SelectCustomers",
                "Cassandra_Old_SelectCustomerOrderDetails",
                "Cassandra_Old_CountOrdersForCustomer",
                "Cassandra_Old_SelectAll",
                "Cassandra_Old_CreateEmployee",
                "Cassandra_Old_InsertEmployees",
                "Cassandra_Old_UpdateEmployeeTable",
                "Cassandra_Old_UpdateRecord",
                "Cassandra_Old_DeleteRow",
                "Cassandra_Old_DeleteAll",
                "Cassandra_Old_DeleteTable",
                "PostgreSQL_SelectOrders",
                "PostgreSQL_SelectOrderDetails",
                "PostgreSQL_SelectCustomers",
                "PostgreSQL_SelectCustomerOrderDetails",
                "PostgreSQL_CountOrdersForCustomer",
                "PostgreSQL_SelectAll",
                "PostgreSQL_CreateEmployee",
                "PostgreSQL_InsertEmployees",
                "PostgreSQL_UpdateEmployeeTable",
                "PostgreSQL_UpdateRecord",
                "PostgreSQL_DeleteRow",
                "PostgreSQL_DeleteAll",
                "PostgreSQL_DeleteTable",
                "MySQL_SelectOrders",
                "MySQL_SelectOrderDetails",
                "MySQL_SelectCustomers",
                "MySQL_SelectCustomerOrderDetails",
                "MySQL_CountOrdersForCustomer",
                "MySQL_SelectAll",
                "MySQL_CreateEmployee",
                "MySQL_InsertEmployees",
                "MySQL_UpdateEmployeeTable",
                "MySQL_UpdateRecord",
                "MySQL_DeleteRow",
                "MySQL_DeleteAll",
                "MySQL_DeleteTable"
        );

        System.out.println("=======================================================================================");
        System.out.println("----------------------------- OTWÓRZ POŁĄCZENIA DO BAZ DANYCH ---------------------------");
        System.out.println("=======================================================================================\n");

        ConnectCassandra connectCassandra = new ConnectCassandra();
        var session = connectCassandra.getSession();
        System.out.println("-> Połączenie z Cassandrą nawiązane.");

        ConnectCassandraOld connectCassandraOld = new ConnectCassandraOld();
        var sessionOld = connectCassandraOld.getSession();
        System.out.println("-> Połączenie z CassandrąOld nawiązane.");

        ConnectPostgre connectPostgre = new ConnectPostgre();
        var connPostgre = connectPostgre.getConnection();
        System.out.println("-> Połączenie z PostgreSQL nawiązane.");

        ConnectMySql connectMySql = new ConnectMySql();
        var connMySql = connectMySql.getConnection();
        System.out.println("-> Połączenie z MySQL nawiązane.");

        var selectBranchAndEmployeeId = new SelectBranchAndEmployeeId();
        var result = selectBranchAndEmployeeId.selectBranchIds(session);

        GenerateDataSet generateDataSet = new GenerateDataSet(result);
        generateDataSet.generateDataSetForCassAndSql();


        System.out.println("\n=======================================================================================");
        System.out.println("---------------------------------------- CASSANDRA OPERATIONS ---------------------------");
        System.out.println("=======================================================================================\n");

        System.out.println("--- 1. READ: Odczytywanie danych z tabel Cassandra ---");
        ReadCassandra readCassandra = new ReadCassandra(session);

        nanoseconds.add(readCassandra.SelectOrders());
        nanoseconds.add(readCassandra.SelectOrderDetails());
        nanoseconds.add(readCassandra.SelectCustomers());
        nanoseconds.add(readCassandra.SelectCustomerOrderDetails());
        nanoseconds.add(readCassandra.CountOrdersForCustomer());
        nanoseconds.add(readCassandra.SelectAll());

        System.out.println("--- 2. CREATE: Tworzenie danych Cassandra ---");
        CreateCassandra createCassandra = new CreateCassandra(session);
        nanoseconds.add(createCassandra.CreateEmployee());
        nanoseconds.add(createCassandra.InsertEmployee(generateDataSet.getValues()));

        System.out.println("--- 3. SELECT: Pobieranie ID nowych rekordów z Cassandry ---");

        long selectIdsTime = 0;
        var ids = selectBranchAndEmployeeId.selectIds(session);


        System.out.println("--- 4. UPDATE: Modyfikowanie danych Cassandra ---");
        UpdateCassandra updateCassandra = new UpdateCassandra(session);
        nanoseconds.add(updateCassandra.updateEmployeeTable(ids));
        nanoseconds.add(updateCassandra.updateRecordInEmployeeTable(ids.getFirst()));

        System.out.println("--- 5. DELETE: Usuwanie danych z Cassandra ---");
        DeleteCassandra deleteCassandra = new DeleteCassandra(session);
        nanoseconds.add(deleteCassandra.deleteTableRow(ids.getFirst()));
        nanoseconds.add(deleteCassandra.deleteAllTableContent());
        nanoseconds.add(deleteCassandra.deleteTable());

        connectCassandra.CloseConnection();
        System.out.println("-> Połączenie z Cassandrą zamknięte.");


        System.out.println("\n=======================================================================================");
        System.out.println("------------------------------------ CASSANDRA OLD OPERATIONS ---------------------------");
        System.out.println("=======================================================================================\n");

        System.out.println("--- 1. READ: Odczytywanie danych z tabel Cassandra Old ---");
        ReadCassandraOld readCassandraOld = new ReadCassandraOld(sessionOld);

        nanoseconds.add(readCassandraOld.SelectOrders());
        nanoseconds.add(readCassandraOld.SelectOrderDetails());
        nanoseconds.add(readCassandraOld.SelectCustomers());
        nanoseconds.add(readCassandraOld.SelectCustomerOrderDetails());
        nanoseconds.add(readCassandraOld.CountOrdersForCustomer());
        nanoseconds.add(readCassandraOld.SelectAll());

        System.out.println("--- 2. CREATE: Tworzenie danych Cassandra Old ---");
        CreateCassandraOld createCassandraOld = new CreateCassandraOld(sessionOld);
        nanoseconds.add(createCassandraOld.CreateEmployee());
        nanoseconds.add(createCassandraOld.InsertEmployee(generateDataSet.getValues()));

        System.out.println("--- 3. UPDATE: Modyfikowanie danych Cassandra Old ---");
        UpdateCassandraOld updateCassandraOld = new UpdateCassandraOld(sessionOld);
        nanoseconds.add(updateCassandraOld.updateEmployeeTable(ids));
        nanoseconds.add(updateCassandraOld.updateRecordInEmployeeTable(ids.getFirst()));

        System.out.println("--- 4. DELETE: Usuwanie danych z Cassandra Old ---");
        DeleteCassandraOld deleteCassandraOld = new DeleteCassandraOld(sessionOld);
        nanoseconds.add(deleteCassandraOld.deleteTableRow(ids.getFirst()));
        nanoseconds.add(deleteCassandraOld.deleteAllTableContent());
        nanoseconds.add(deleteCassandraOld.deleteTable());

        connectCassandraOld.CloseConnection();
        System.out.println("-> Połączenie z Cassandrą Old zamknięte.");


        System.out.println("\n=======================================================================================");
        System.out.println("-------------------------------------- POSTGRESQL OPERATIONS ----------------------------");
        System.out.println("=======================================================================================\n");

        System.out.println("--- 1. READ: odczytywanie danych z tabel postgre ---");
        ReadPostgres readPostgres = new ReadPostgres(connPostgre);

        nanoseconds.add(readPostgres.SelectOrders());
        nanoseconds.add(readPostgres.SelectOrderDetails());
        nanoseconds.add(readPostgres.SelectCustomers());
        nanoseconds.add(readPostgres.SelectCustomerOrderDetails());
        nanoseconds.add(readPostgres.CountOrdersForCustomer());
        nanoseconds.add(readPostgres.SelectAll());

        System.out.println("--- 2. CREATE: tworzenie danych postgre ---");
        CreatePostgres createPostgres = new CreatePostgres(connPostgre);
        nanoseconds.add(createPostgres.CreateEmployee());
        nanoseconds.add(createPostgres.InsertEmployee(generateDataSet.getValuesSql()));

        System.out.println("--- 3. UPDATE: modyfikowanie danych postgre ---");
        UpdatePostgres updatePostgres = new UpdatePostgres(connPostgre);
        nanoseconds.add(updatePostgres.updateEmployeeTable(ids));
        nanoseconds.add(updatePostgres.updateRecordInEmployeeTable(ids.getFirst()));

        System.out.println("--- 4. DELETE: usuwanie danych z postgre ---");
        DeletePostgres deletePostgres = new DeletePostgres(connPostgre);
        nanoseconds.add(deletePostgres.deleteTableRow(ids.getFirst()));
        nanoseconds.add(deletePostgres.deleteAllTableContent());
        nanoseconds.add(deletePostgres.deleteTable());

        connectPostgre.CloseConnection();
        System.out.println("-> Połączenie z PostgreSQL zamknięte.");


        System.out.println("\n=======================================================================================");
        System.out.println("---------------------------------------- MYSQL OPERATIONS -------------------------------");
        System.out.println("=======================================================================================\n");

        System.out.println("--- 1. READ: odczytywanie danych z tabel MySql ---");
        ReadMySql readMySql = new ReadMySql(connMySql);

        nanoseconds.add(readMySql.SelectOrders());
        nanoseconds.add(readMySql.SelectOrderDetails());
        nanoseconds.add(readMySql.SelectCustomers());
        nanoseconds.add(readMySql.SelectCustomerOrderDetails());
        nanoseconds.add(readMySql.CountOrdersForCustomer());
        nanoseconds.add(readMySql.SelectAll());

        System.out.println("--- 2. CREATE: tworzenie danych MySql ---");
        CreateMySql createMySql = new CreateMySql(connMySql);
        nanoseconds.add(createMySql.CreateEmployee());
        nanoseconds.add(createMySql.InsertEmployee(generateDataSet.getValuesSql()));

        System.out.println("--- 3. UPDATE: modyfikowanie danych MySql ---");
        UpdateMySql updateMySql = new UpdateMySql(connMySql);
        nanoseconds.add(updateMySql.updateEmployeeTable(ids));
        nanoseconds.add(updateMySql.updateRecordInEmployeeTable(ids.getFirst()));

        System.out.println("--- 4. DELETE: usuwanie danych z MySql ---");
        DeleteMySql deleteMySql = new DeleteMySql(connMySql);
        nanoseconds.add(deleteMySql.deleteTableRow(ids.getFirst()));
        nanoseconds.add(deleteMySql.deleteAllTableContent());
        nanoseconds.add(deleteMySql.deleteTable());

        connectMySql.CloseConnection();
        System.out.println("-> Połączenie z MySQL zamknięte.");




        ConvertToCsv convertToCsv = new ConvertToCsv();
        timeResults = convertToCsv.listMillisecondsToTime(nanoseconds);
        convertToCsv.addStringListToCsv(firstCsvLine);
        convertToCsv.addStringListToCsv(timeResults);
    }





}






