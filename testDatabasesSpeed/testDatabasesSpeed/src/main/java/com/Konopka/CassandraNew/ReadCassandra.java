package com.Konopka.CassandraNew;

import com.Konopka.Interfaces.ReadInterface;
import com.datastax.oss.driver.api.core.CqlSession;

import java.util.Objects;

public class ReadCassandra implements ReadInterface {

    public static CqlSession session;

    public ReadCassandra(CqlSession session) {
        ReadCassandra.session = session;
    }

    @Override
    public Long SelectOrders() {
        try{
            System.out.println("start select orders_by_branch");
            Long startTime = System.nanoTime();
            var resultSet = session.execute("""
                    SELECT *
                    FROM cassandraSpeedTestDb.orders_by_branch;
                    """);
            Long endTime = System.nanoTime();
             System.out.println("koniec select orders_by_branch czas dzialanie programu wyniosl: " + (endTime - startTime));
            return endTime - startTime;
        }catch(Exception e){
            System.err.println("nie udalo sie pobrac tabeli orders_by_branch");
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Long SelectOrderDetails() {
        try{
            System.out.println("start select order_details_by_order");
            Long startTime = System.nanoTime();
            var resultSet = session.execute("""
                     SELECT *
                     FROM cassandraspeedtestdb.order_details_by_order;
                    """);
            Long endTime = System.nanoTime();
            System.out.println("koniec select order_details_by_order czas dzialanie programu wyniosl: " + (endTime - startTime));
            return endTime - startTime;
        }catch(Exception e){
            System.err.println("nie udalo sie pobrac tabeli order_details_by_order");
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Long SelectCustomers() {
        try{
            System.out.println("start select customers");
            Long startTime = System.nanoTime();
            var resultSet = session.execute("""
                     SELECT *
                     FROM cassandraspeedtestdb.customers;
                    """);
            Long endTime = System.nanoTime();
            System.out.println("koniec select customers czas dzialanie programu wyniosl: " + (endTime - startTime));
            return endTime - startTime;

        }catch(Exception e){
            System.err.println("nie udalo sie pobrac tabeli customers");
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Long SelectCustomerOrderDetails() {
        try{
            System.out.println("start select orders_by_user");
            Long startTime = System.nanoTime();
            var resultSet = session.execute("""
                        SELECT userid, date_, orderid, namesurname , totalbasket
                        FROM cassandraspeedtestdb.orders_by_user;
                    """);
            Long endTime = System.nanoTime();
            System.out.println("koniec select orders_by_user czas dzialanie programu wyniosl: " + (endTime - startTime));
            return endTime - startTime;
        }catch(Exception e){
            System.err.println("nie udalo sie pobrac tabeli orders_by_user");
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Long CountOrdersForCustomer() {
        try{
            System.out.println("start count orders_by_user");
            Long startTime = System.nanoTime();
            var resultSet = session.execute("""
                       SELECT userid, count(orderid)
                       FROM cassandraspeedtestdb.orders_by_user
                       GROUP BY userid;
                    """);
            Long endTime = System.nanoTime();
            System.out.println("koniec count orders_by_user czas dzialanie programu wyniosl: " + (endTime - startTime));
            return endTime - startTime;
        }catch(Exception e){
            System.err.println("nie udalo sie pobrac tabeli orders_by_user");
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Long SelectAll() {
        try{
            System.out.println("start select all");
            Long startTime = System.nanoTime();
            var resultSet = session.execute("""
                       SELECT *
                       FROM cassandraspeedtestdb.orders_by_user;
                    """);
            Long endTime = System.nanoTime();
            System.out.println("koniec select all czas dzialanie programu wyniosl: " + (endTime - startTime));
            return endTime - startTime;
        }catch(Exception e){
            System.err.println("nie udalo sie pobrac tabeli all");
            e.printStackTrace();
        }
        return null;
    }
}
