package com.Konopka;

import com.datastax.oss.driver.api.core.cql.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class GenerateDataSet {

    private List<String> values;
    private List<String> valuesSql;
    private List<Row> BranchIds;

    Random rand = new Random();
    private int LastBranchIdNumber = 162;

    List<String> firstNames = List.of(
            "Anna", "Piotr", "Katarzyna", "Michał", "Ewa",
            "Adam", "Alicja", "Marcin", "Magda", "Krzysztof"
    );
    List<String> lastNames = List.of(
            "Kowalska", "Nowak", "Zielińska", "Wójcik", "Kowalczyk",
            "Lewandowski", "Dąbrowska", "Mazur", "Gajda", "Szymański"
    );
    List<String> positions = List.of(
            "Analityk", "Programista", "Księgowa", "Manager", "Specjalista HR",
            "Architekt", "Tester", "Inżynier", "Grafik", "Help Desk"
    );
    List<String> salaries = List.of(
            "6500.00",
            "12000.50",
            "7800.00",
            "15000.00",
            "9200.00",
            "18500.00",
            "5900.00",
            "10500.00",
            "8100.00",
            "7200.00"
    );
    List<String> emails = List.of(
            "anna@firma.pl", "piotr@firma.pl", "kasia@firma.pl", "michal@firma.pl", "ewa@firma.pl",
            "adam@firma.pl", "alicja@firma.pl", "marcin@firma.pl", "magda@firma.pl", "krzysiek@firma.pl"
    );
    List<String> phoneNumbers = List.of(
            "111-222-333", "222-333-444", "333-444-555", "444-555-666", "555-666-777",
            "666-777-888", "777-888-999", "888-999-000", "999-000-111", "000-111-222"
    );
    List<String> branchesList = new ArrayList<>();



    private int dataListSize = 10;


    GenerateDataSet(List<Row> BranchIds) {
        this.values = new ArrayList<>();
        this.valuesSql = new ArrayList<>();
        this.BranchIds = BranchIds;
        for(Row row : this.BranchIds){
            branchesList.add(row.getString(0));
        }
    }

    public void generateDataSetForCassAndSql(){
        for(int j = 0; j < 10000; j++) {
            StringBuilder value = new StringBuilder();
            for (int i = 0; i < 100; i++) {
                int branchNumber = rand.nextInt(1, LastBranchIdNumber - 1);
                String branchId = branchesList.get(branchNumber);
                UUID employeeId = UUID.randomUUID();
                String firstName = firstNames.get(rand.nextInt(dataListSize));
                String lastName = lastNames.get(rand.nextInt(dataListSize));
                String position = positions.get(rand.nextInt(dataListSize));
                String salary = salaries.get(rand.nextInt(dataListSize));
                String email = emails.get(rand.nextInt(dataListSize));
                String phoneNumber = phoneNumbers.get(rand.nextInt(dataListSize));
                value.append(generateCassandra(
                        branchId,
                        employeeId,
                        firstName,
                        lastName,
                        position,
                        salary,
                        email,
                        phoneNumber
                ));

                this.valuesSql.add(
                        generateSql(
                                branchId,
                                employeeId,
                                firstName,
                                lastName,
                                position,
                                salary,
                                email,
                                phoneNumber
                        ));
            }
            this.values.add(value.toString());
        }
    }


    public void generateDataSetSql(){
        for(int j = 0; j < 1000000; j++) {
            StringBuilder value = new StringBuilder();
                value.append("INSERT INTO employees_by_branch \n" +
                        "(branch_id, employeeid, first_name, last_name, position, salary, email, phone_number)\n");
                value.append("VALUES (");
                value.append(rand.nextInt(1, LastBranchIdNumber)).append(", ");
                value.append("'").append(UUID.randomUUID()).append("', ");
                value.append("'").append(firstNames.get(rand.nextInt(dataListSize))).append("', ");
                value.append("'").append(lastNames.get(rand.nextInt(dataListSize))).append("', ");
                value.append("'").append(positions.get(rand.nextInt(dataListSize))).append("', ");
                value.append(salaries.get(rand.nextInt(dataListSize))).append(", ");
                value.append("'").append(emails.get(rand.nextInt(dataListSize))).append("', ");
                value.append("'").append(phoneNumbers.get(rand.nextInt(dataListSize))).append("'");
                value.append(");\n");
            this.values.add(value.toString());
        }
    }


    public String generateSql(
            String branchId,
            UUID employeeId,
            String firstName,
            String lastName,
            String position,
            String salary,
            String email,
            String phoneNumber)
    {
        StringBuilder valueSql = new StringBuilder();
        valueSql.append("INSERT INTO employees_by_branch \n" +
                "(branch_id, employeeid, first_name, last_name, position, salary, email, phone_number)\n");
        valueSql.append("VALUES (");
        valueSql.append("'").append(branchId).append("', ");
        valueSql.append("'").append(employeeId).append("', ");
        valueSql.append("'").append(firstName).append("', ");
        valueSql.append("'").append(lastName).append("', ");
        valueSql.append("'").append(position).append("', ");
        valueSql.append(salary).append(", ");
        valueSql.append("'").append(email).append("', ");
        valueSql.append("'").append(phoneNumber).append("'");
        valueSql.append(");\n");
        return valueSql.toString();
    }

    public StringBuilder generateCassandra(
            String branchId,
            UUID employeeId,
            String firstName,
            String lastName,
            String position,
            String salary,
            String email,
            String phoneNumber)
    {
        StringBuilder value = new StringBuilder();
        value.append("INSERT INTO cassandraspeedtestdb.employees_by_branch \n" +
                "(branch_id, employeeid, first_name, last_name, position, salary, email, phone_number)\n");
        value.append("VALUES (");
        value.append("'").append(branchId).append("', ");
        value.append(employeeId).append(", ");
        value.append("'").append(firstName).append("', ");
        value.append("'").append(lastName).append("', ");
        value.append("'").append(position).append("', ");
        value.append(salary).append(", ");
        value.append("'").append(email).append("', ");
        value.append("'").append(phoneNumber).append("'");
        value.append(");\n");
        return value;
    }


    public List<String> getValues() {
        return values;
    }

    public List<String> getValuesSql() {
        return valuesSql;
    }
}
