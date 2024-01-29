package test.org.apache.spark.sql;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;

public abstract class  Encounter  extends SpecificRecordBase implements SpecificRecord {
    private static final byte[] schemaBytes = "{\"type\":\"record\",\"name\":\"Encounter\",\"namespace\":\"test.org.apache.spark.sql\",\"doc\":\"* A model representing Encounter.\",\"fields\":[{\"name\":\"name\",\"type\":[\"null\",{\"type\":\"string\"}],\"doc\":\"* Name of the employee\",\"default\":null},{\"name\":\"salary\",\"type\":[\"null\",{\"type\":\"int\"}],\"doc\":\"* Salary of the employee\",\"default\":null}]}".getBytes(Charset.forName("UTF-8"));
    public static final Schema SCHEMA$;
    private String name;
    private long salary;

    static {
        try {
            SCHEMA$ = (new Schema.Parser()).parse(new ByteArrayInputStream(schemaBytes));
        } catch (IOException var1) {
            throw new SchemaParseException(var1);
        }
    }

    public Encounter(String name, long salary) {
        this.name = name;
        this.salary = salary;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getSalary() {
        return salary;
    }

    public void setSalary(long salary) {
        this.salary = salary;
    }

    public static Schema getClassSchema() {
        return SCHEMA$;
    }



}
