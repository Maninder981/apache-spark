package com.apache.spark;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Flatmap we use here one line can have multiple words. it returns multiple results per input row and put them
 * into one dataset. this override function split the line with space.
 *
 */
public class LineMapper implements FlatMapFunction<Row, String> {

    private static final long serialVersionID=1L;

    @Override
    public Iterator<String> call(Row row) throws Exception {
        return Arrays.asList(row.toString().split(" ")).iterator();
    }
}
