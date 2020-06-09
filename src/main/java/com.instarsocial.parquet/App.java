package com.instarsocial.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;


public class App {

    public static void main(String[] args) {

        try {
            Schema schema = parseSchema();

            List<GenericData.Record> recordList = generateRecords(schema);

            Path path = new Path("data.parquet");

            try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(path)
                    .withSchema(schema)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                    .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                    .withConf(new Configuration())
                    .withValidation(false)
                    .withDictionaryEncoding(false)
                    .build()) {

                for (GenericData.Record record : recordList) {
                    writer.write(record);
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace(System.out);
        }
    }

    private static Schema parseSchema() {
        String schemaJson = "{\"namespace\": \"org.myorganization.mynamespace\"," //Not used in Parquet, can put anything
                + "\"type\": \"record\"," //Must be set as record
                + "\"name\": \"myrecordname\"," //Not used in Parquet, can put anything
                + "\"fields\": ["
                + " {\"name\": \"myString\",  \"type\": [\"string\", \"null\"]}"
                + ", {\"name\": \"myInteger\", \"type\": \"int\"}" //Required field
                + ", {\"name\": \"myDateTime\", \"type\": [{\"type\": \"long\", \"logicalType\" : \"timestamp-millis\"}, \"null\"]}"
                + " ]}";

        Schema.Parser parser = new Schema.Parser().setValidate(true);
        Schema schema = parser.parse(schemaJson);

        return schema;
    }

    private static List<GenericData.Record> generateRecords(Schema schema) {

        List<GenericData.Record> recordList = new ArrayList<GenericData.Record>();

        for(int i = 1; i <= 100_000; i++) {
            GenericData.Record record = new GenericData.Record(schema);
            record.put("myInteger", i);
            record.put("myString", i + "hi world of parquet!");
            record.put("myDateTime", new DateTime().plusHours(i).getMillis());

            recordList.add(record);
        }

        return recordList;
    }
}