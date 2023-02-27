package com.demo;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;

import java.util.Date;
import java.util.List;

@UdfDescription(name = "extractMinuteIndex", author = "sa-webb", version = "1.0.0", description = "A function that extracts the minute from a timestamp and uses it to index an array of values.")
public class ExtractMinuteIndexUdf {

    @Udf(description = "Extracts the minute from the timestamp field and uses it to index the values array")
    public int formula(@UdfParameter long t, @UdfParameter List<Integer> v) {
        String date = new Date(t).toString();
        int min = Integer.parseInt(date.substring(14, 16));
        return v.get(min);
    }

}
