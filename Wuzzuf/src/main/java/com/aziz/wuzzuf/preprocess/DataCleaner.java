package com.aziz.wuzzuf.preprocess;
import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DataCleaner {

    public static String getNullCount(SparkSession session, Dataset<Row> df){
        StringBuilder builder = new StringBuilder();
        List<Column> cols = new ArrayList<>();
        for(String col : df.columns()) {
            Column c = df.col(col);
            Column nulls = functions.count(functions.when(functions.isnan(c), c)).alias(col);
            cols.add(nulls);
        }
        df.select(cols.get(0)).show();
        for(int i =0; i <cols.size();i++) {
            for (Iterator<String> it = df.select(cols.get(i)).toJSON().toLocalIterator(); it.hasNext(); ) {
                String js = it.next();
                builder.append("<br>").append(js).append("</br>");
            }
        }
        builder.insert(0, "<h1>Number Of nulls in wuzzuf dataset:</h1>");
        return builder.toString();
    }


}
