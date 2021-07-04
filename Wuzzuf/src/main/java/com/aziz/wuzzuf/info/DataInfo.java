package com.aziz.wuzzuf.info;

import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.StringType;
import org.spark_project.dmg.pmml.DataType;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.api.TypeTags;

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

}
