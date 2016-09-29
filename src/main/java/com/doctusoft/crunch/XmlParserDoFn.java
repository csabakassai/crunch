package com.doctusoft.crunch;

import com.doctusoft.crunch.jaxb.StackOverFlowXmlRow;
import com.google.cloud.dataflow.sdk.transforms.DoFn;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import java.io.StringReader;

/**
 * Created by cskassai on 28/09/16.
 */
public class XmlParserDoFn extends DoFn<String, StackOverFlowXmlRow>{

    private final static JAXBContext jaxbContext;

    static {
        try {
            jaxbContext = JAXBContext.newInstance(StackOverFlowXmlRow.class);
        } catch (JAXBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void processElement(ProcessContext processContext) throws Exception {

        String line = processContext.element();
        StackOverFlowXmlRow unmarshal = (StackOverFlowXmlRow) jaxbContext.createUnmarshaller().unmarshal(new StringReader(line));

        processContext.output(unmarshal);
    }
}
