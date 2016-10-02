package com.doctusoft.crunch.jaxb;

import com.doctusoft.crunch.model.StackOverflowPost;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.junit.Assert;
import org.junit.Test;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.StringReader;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by cskassai on 28/09/16.
 */
public class TestXmlParsing {

    @Test
    public void testRowParsing() throws Exception {
        List<String> lines = Resources.readLines(getClass().getResource("/testRows.xml"), Charsets.UTF_8);

        JAXBContext jaxbContext = JAXBContext.newInstance(StackOverflowPost.class);
        Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
        Stream<StackOverflowPost> rows = lines.stream().map((String line) -> {
            try {
                return (StackOverflowPost) unmarshaller.unmarshal(new StringReader(line));
            } catch (JAXBException e) {
                throw new RuntimeException(e);
            }
        });


        List<StackOverflowPost> rowList = rows.collect(Collectors.toList());
        Assert.assertTrue(!rowList.isEmpty());
    }
}
