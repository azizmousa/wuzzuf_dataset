package com.aziz.wuzzuf.info;

import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;

public class DataInfo {

    public static Dataset<Row> getMostDemandCompany(SparkSession session, Dataset<Row> df) {
        df.createOrReplaceTempView("wuzzuf");
        String sql = "select Company, count(Title) as jobs from wuzzuf  group by Company order by jobs desc";
        return session.sql(sql);
    }

    public static Dataset<Row> getMostPopularJobTitle(SparkSession session, Dataset<Row> df) {
        df.createOrReplaceTempView("wuzzuf");
        String sql = "select Title, count(Title) as titleCount from wuzzuf  group by Title order by titleCount desc";
        return session.sql(sql);
    }


    public static Dataset<Row> getMostPopularAreas(SparkSession session, Dataset<Row> df) {
        df.createOrReplaceTempView("wuzzuf");
        String sql = "select Location, count(Location) as locCount from wuzzuf  " +
                "group by Location order by locCount desc";
        return session.sql(sql);
    }

    public static Dataset<Row> getMostPopularSills(SparkSession session, Dataset<Row> df) {
        df.createOrReplaceTempView("wuzzuf");
        String sql = "select Skills from wuzzuf";
        Dataset<Row> dataset = session.sql(sql);
        List<Row> rows = dataset.select("Skills").collectAsList();
        List<String> allSkills = new ArrayList<>();
        for (Row row : rows) {
            String str = row.toString().replace("[", "").replace("]", "");
            String[] skills = str.split(",");
            allSkills.addAll(Arrays.asList(skills));
        }
        Dataset<Row> skillsDf = session.createDataset(allSkills, Encoders.STRING()).toDF("Skills");
        skillsDf.createOrReplaceTempView("skillsDf");
        String skillsSQL = "select Skills, count(Skills) as skCount from skillsDf  " +
                "group by Skills order by skCount desc";
        skillsDf = session.sql(skillsSQL);
        return skillsDf;
    }

    public static Table convertExp(Table table){
        StringColumn yearsExp = table.stringColumn("YearsExp");
        List<String> minYearList = new ArrayList<>();
        List<String> maxYearList = new ArrayList<>();
        for(String row : yearsExp){
            String rowCleaned = row.replace("Yrs of Exp", "")
                    .replace(" ", "");
            String [] splits = new String[]{"0", "0"};
            if(rowCleaned.contains("-"))
                splits = rowCleaned.split("-");
            else if (rowCleaned.contains("+")) {
                splits[0] = rowCleaned.replace("+", "");
                splits[1] = "inf";
            } else{
                splits[1] = "inf";
            }
            minYearList.add(splits[0]);
            maxYearList.add(splits[1]);
        }
        StringColumn minCol = StringColumn.create("minExp", minYearList);
        StringColumn maxCol = StringColumn.create("maxExp", maxYearList);

        table.addColumns(minCol);
        table.addColumns(maxCol);
        return table;
    }
}
