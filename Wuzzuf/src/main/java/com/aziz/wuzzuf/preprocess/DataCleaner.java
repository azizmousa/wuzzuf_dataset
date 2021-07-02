package com.aziz.wuzzuf.preprocess;
import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.List;

public class DataCleaner {

    public static String getNullCount(SparkSession session, Dataset<Row> df){
        StringBuilder builder = new StringBuilder();
        List<Long> cols = new ArrayList<>();
        df.createOrReplaceTempView("wuzzuf");
        for(String col : df.columns()) {
            long c =  session.sql("select "+col+" as "+col+" from wuzzuf where "+col+" is null").count();
            cols.add(c);
        }
        int count = 0;
        for (String col : df.columns()) {
            builder.append("<br>").append("<b>").append(col).append(": </b>").append(cols.get(count++)).append("</br>");
        }
        builder.insert(0, "<h1>Number Of nulls in wuzzuf dataset:</h1>");
        return builder.toString();
    }


    public static Dataset<Row> dropNulls(SparkSession session, Dataset<Row> df){
        return df.filter(row -> !row.anyNull());
    }

    public static Dataset<Row> removeDuplicates(SparkSession session, Dataset<Row> df){
        return df.distinct();
    }


}
