package test.org.apache.spark.sql;

import com.databricks.spark.avro.AvroEncoder;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;


public enum PopRecordEntity {
    ENCOUNTER(
            Encounter.class,
            Encounter.getClassSchema(),
            "encounter"),

    COMPANY(
            Company.class,
            Company.getClassSchema(),
            "encounter");

    private final Class<? extends SpecificRecord> modelClass;
    private final Encoder<? extends SpecificRecord> modelEncoder;
    private final Schema modelSchema;
    private final String directoryName;
    private final String uidField;
    private final Map<PopRecordEntity, Map.Entry<Dataset<? extends SpecificRecord>, String>> componentDatasets =
            new HashMap<>();

    PopRecordEntity(
            Class<? extends SpecificRecord> modelClass,
            Schema modelSchema,
            String directoryName) {
        this.directoryName = directoryName;
        this.modelClass = modelClass;
        this.modelEncoder = getEncoderFromClass(modelClass);
        this.modelSchema = modelSchema;
        this.uidField = "uid";
    }


    /**
     * Get directoryName.
     *
     * @return the model's Hive table name
     */
    public String getDirectoryName() {
        return this.directoryName;
    }

    /**
     * Get modelClass.
     *
     * @return the model's {@link Encoder}
     */
    public Class<? extends SpecificRecord> getModelClass() {
        return this.modelClass;
    }

    /**
     * Get modelEncoder.
     *
     * @return the model's {@link Encoder}
     */
    public Encoder<? extends SpecificRecord> getModelEncoder() {
        return this.modelEncoder;
    }

    /**
     * Get modelSchema.
     *
     * @return the model's Avro {@link Schema}
     */
    public Schema getModelSchema() {
        return this.modelSchema;
    }


    /**
     * Get modelSchema.
     *
     * @return the model's Avro {@link Schema}
     */
    public String getUidField() {
        return this.uidField;
    }

    public String getHiveTableName() {
        return getDirectoryName();
    }

    /**
     * Gets the model enum from the given directory name.
     *
     * @param directoryName the directory name.
     * @return the {@link PopRecordEntity} model enum
     */
    public static PopRecordEntity fromDirectoryName(String directoryName) {
        String name = directoryName.toLowerCase(Locale.ENGLISH);
        return fromHiveTableName(name);

    }

    /**
     * Gets the model enum from the given Hive table name.
     *
     * @param tableName the Hive table name.
     * @return the {@link PopRecordEntity} model enum
     */
    public static PopRecordEntity fromHiveTableName(String tableName) {

        String name = tableName.toLowerCase(Locale.ENGLISH).replaceAll("_", "");

        switch (name) {
            case "encounter":
                return PopRecordEntity.ENCOUNTER;
            case "company":
                return PopRecordEntity.COMPANY;
            default:
                return null;
        }
    }

    private static Encoder<? extends SpecificRecord> getEncoderFromClass(
            Class<? extends SpecificRecord> avroClass) {
        return AvroEncoder.of(avroClass);
    }

}
