package com.apache.spark;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import java.text.SimpleDateFormat;

public class HouseMapper implements MapFunction<Row, House> {

    private static final long serialVersionUID=1L;
    @Override
    public House call(Row row) throws Exception {
        House house= new House();
        //whatever the header name is defined in the csv file, we need to provide here.
        house.setId(row.getAs("id"));
        house.setAddress(row.getAs("address"));
        house.setSqft(row.getAs("sqft"));
        house.setPrice(row.getAs("price"));

        //because the datatype of this column is date so need to do some extra code for it.
        Object vacancyDateValue = row.getAs("vacantBy");
        if (vacancyDateValue != null) {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            java.util.Date parsedDate = dateFormat.parse(vacancyDateValue.toString());
            house.setVacantBy(new java.sql.Date(parsedDate.getTime()));
        }

        return house;
    }
}
