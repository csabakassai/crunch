package com.doctusoft.crunch.util;

import javax.xml.bind.annotation.adapters.XmlAdapter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class JaxbDateTimeAdapter extends XmlAdapter<String, LocalDateTime> {

    public LocalDateTime unmarshal(String value) {
        return parseLocalDateTime(value);
    }

    public String marshal(LocalDateTime value) {
        return printLocalDateTime(value);
    }


    private static DateTimeFormatter detectFormat(String xmlDateTime) {
        if (xmlDateTime.endsWith("Z")) {
            return DateTimeFormatter.ISO_OFFSET_DATE_TIME;
        }
        return DateTimeFormatter.ISO_LOCAL_DATE_TIME;
    }

    public static LocalDateTime parseLocalDateTime(String xmlDateTime) {
        return LocalDateTime.parse(xmlDateTime, detectFormat(xmlDateTime));
    }

    public static String printLocalDateTime(LocalDateTime localDateTime) {
        return localDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);

    }


}
