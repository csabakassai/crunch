package com.doctusoft.crunch.cheat;

import com.doctusoft.crunch.XmlParserDoFn;
import com.doctusoft.crunch.jaxb.StackOverFlowXmlRow;
import com.doctusoft.crunch.util.DomainClass;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;

import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.io.XmlSource;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Filter;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.sun.java.browser.plugin2.DOM;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import java.io.Serializable;
import java.io.StringReader;

/**
 * Created by cskassai on 28/09/16.
 */
@Log4j2
public class StackOverFlowPipelineStep1 implements Serializable {

    @SneakyThrows
    public static void main(String[] args) {

        DataflowPipelineOptions dataflowPipelineOptions = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
        dataflowPipelineOptions.setRunner(DataflowPipelineRunner.class);
        dataflowPipelineOptions.setProject("ds-bq-demo");
        dataflowPipelineOptions.setZone("europe-west1-b");

        dataflowPipelineOptions.setStagingLocation("gs://ds-bq-demo-eu/staging/");
        dataflowPipelineOptions.setTempLocation("gs://ds-bq-demo-eu/temp/");

        Pipeline pipeline = Pipeline.create(dataflowPipelineOptions);


        XmlSource<StackOverFlowXmlRow> postXmlSource = XmlSource
                .<StackOverFlowXmlRow>from("gs://ds-bq-demo-eu/stackoverflow/Posts-samples.xml")
                .withRootElement("posts")
                .withRecordElement("row")
                .withRecordClass(StackOverFlowXmlRow.class);

        PCollection<StackOverFlowXmlRow> posts = pipeline.apply(Read.from(postXmlSource)).setCoder(SerializableCoder.of(StackOverFlowXmlRow.class));;

        final DomainClass<StackOverFlowXmlRow> dom = DomainClass.of(StackOverFlowXmlRow.class);

        PCollection<TableRow> bqRows = posts.apply(ParDo.of(new DoFn<StackOverFlowXmlRow, TableRow>() {
            @Override
            public void processElement(ProcessContext processContext) throws Exception {
                StackOverFlowXmlRow s = processContext.element();

                TableRow tableRow = dom.writeBq(s);

                processContext.output(tableRow);
            }
        }));

        bqRows.apply(BigQueryIO.Write
                .withSchema(dom.getBqSchema())
                .to(dom.createTable(pipeline.getOptions().as(DataflowPipelineOptions.class).getProject(), "stackoverflow").getTableReference())
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));



        pipeline.run();
    }
}
