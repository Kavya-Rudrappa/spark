package test.org.apache.spark.sql;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.SparkSession;


import static test.org.apache.spark.sql.PopRecordEntity.ENCOUNTER;

public class PopRecordDatasets {
    private final SparkSession spark;
    private final Map<PopRecordEntity, Map.Entry<Dataset<? extends SpecificRecord>, String>> componentDatasets =
            new HashMap<>();

    private PopRecordDatasets(SparkSession spark) {

        this.spark = spark;
    }



    @SuppressWarnings("unchecked")
    public <T extends SpecificRecord> Dataset<T> get(PopRecordEntity model) {

        if (componentDatasets.containsKey(model)) {
            return (Dataset<T>) componentDatasets.get(model).getKey();
        } else {
            return (Dataset<T>) spark.emptyDataset(model.getModelEncoder());
        }
    }

    public PopRecordDatasets put(PopRecordEntity model, Map.Entry<Dataset<? extends SpecificRecord>, String> datasetEntry) {
        componentDatasets.put(model, datasetEntry);
        return this;
    }


    public static PopRecordDatasets fromAvroDirectory(SparkSession spark){


        return new PopRecordDatasets(spark).initFromAvro();
    }

    private PopRecordDatasets initFromAvro() {

        Encoder<? extends SpecificRecord> encoder_new = ENCOUNTER.getModelEncoder();

        Dataset<? extends SpecificRecord> dataset;

        dataset = spark.read()
                .option("avroSchema", ENCOUNTER.getModelSchema().toString())
                .format("avro")
                .json("src/test/resources/employees.json")
                .as(encoder_new);

        put(ENCOUNTER, new AbstractMap.SimpleEntry<>(dataset, "src/test/resources/employees.json"));

        return this;

    }
}

