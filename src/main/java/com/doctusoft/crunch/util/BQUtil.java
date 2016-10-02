package com.doctusoft.crunch.util;

import com.doctusoft.crunch.model.BQWriteable;
import com.doctusoft.crunch.model.StackOverflowComment;
import com.doctusoft.crunch.model.StackOverflowPost;
import com.doctusoft.crunch.model.StackOverflowThread;
import com.google.api.services.bigquery.model.TableSchema;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public interface BQUtil {

    static String getNullSafeBQTimeStamp(LocalDateTime dateTime) {
        return  dateTime == null ? null : dateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    }


    String NULLABLE = "nullable";
    String REQUIRED = "required";
    String REPEATED = "repeated";
    String RECORD = "record";


    static <T extends BQWriteable> TableSchema getSchema(Class<T> entityClass) {

       if(StackOverflowComment.class.equals(entityClass)) {
           return StackOverflowComment.BQ_TABLE_SCHEMA;
       } else if(StackOverflowPost.class.equals(entityClass)) {
           return StackOverflowPost.BQ_TABLE_SCHEMA;
       } else if(StackOverflowThread.class.equals(entityClass)) {
           return StackOverflowThread.BQ_TABLE_SCHEMA;
       } else {
           throw new IllegalArgumentException("Not supported entityclass: "  + entityClass.getCanonicalName());
       }

    }

}
