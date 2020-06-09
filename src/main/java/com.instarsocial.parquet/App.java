package com.instarsocial.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;


public class App {

    public static void main(String[] args) {

        System.out.println("Start");

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
                + ", {\"name\": \"myDecimal\", \"type\": [{\"type\": \"fixed\", \"size\":8, \"logicalType\": \"decimal\", \"name\": \"mydecimaltype1\", \"precision\": 10, \"scale\": 2}, \"null\"]}"
                + ", {\"name\": \"myDateTime\", \"type\": [{\"type\": \"long\", \"logicalType\" : \"timestamp-millis\"}, \"null\"]}"
                + " ]}";

        Schema.Parser parser = new Schema.Parser().setValidate(true);
        Schema schema = parser.parse(schemaJson);

        /*Schema.Parser parser = new    Schema.Parser();
        Schema schema = null;
        try {
            // pass path to schema
            schema = parser.parse(ClassLoader.getSystemResourceAsStream(
                    "resources/EmpSchema.avsc"));

        } catch (IOException e) {
            e.printStackTrace();
        }*/

        return schema;
    }

    private static List<GenericData.Record> generateRecords(Schema schema) {
        MutableDateTime epoch = new MutableDateTime(0l, DateTimeZone.UTC);

        List<GenericData.Record> recordList = new ArrayList<GenericData.Record>();
        for(int i = 0; i < 100_000; i++) {
            GenericData.Record record = new GenericData.Record(schema);
            record.put("myInteger", 404+i);
            record.put("myString", "hi world of parquet!" + i);
            //record.put("myDecimal", toFixed(new BigDecimal("65.12").setScale(2, RoundingMode.HALF_UP)));
            record.put("myDateTime", new DateTime().plusHours(i).getMillis());

            recordList.add(record);
        }

        return recordList;
    }

    private static GenericData.Fixed toFixed(BigDecimal myDecimalValue) {
        //Next we get the decimal value as one BigInteger (like there was no decimal point)
        BigInteger myUnscaledDecimalValue = myDecimalValue.unscaledValue();

        //Finally we serialize the integer
        byte[] decimalBytes = myUnscaledDecimalValue.toByteArray();

        //We need to create an Avro 'Fixed' type and pass the decimal schema once more here:
        String schema = "{\"type\":\"fixed\", \"size\":8, \"precision\":10, \"scale\":2, \"name\":\"mydecimaltype1\"}";
        GenericData.Fixed fixed = new GenericData.Fixed(new Schema.Parser().parse(schema));

        byte[] myDecimalBuffer = new byte[8];
        if (myDecimalBuffer.length >= decimalBytes.length) {
            //Because we set our fixed byte array size as 8 bytes, we need to
            //pad-left our original value's bytes with zeros
            int myDecimalBufferIndex = myDecimalBuffer.length - 1;
            for(int i = decimalBytes.length - 1; i >= 0; i--){
                myDecimalBuffer[myDecimalBufferIndex] = decimalBytes[i];
                myDecimalBufferIndex--;
            }

            //Save result
            fixed.bytes(myDecimalBuffer);
        } else {
            throw new IllegalArgumentException(String.format("Decimal size: %d was greater than the allowed max: %d",
                    decimalBytes.length, myDecimalBuffer.length));
        }

        return fixed;
    }
}
