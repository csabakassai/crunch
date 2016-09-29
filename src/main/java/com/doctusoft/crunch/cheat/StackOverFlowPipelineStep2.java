package com.doctusoft.crunch.cheat;

import com.doctusoft.crunch.jaxb.StackOverFlowXmlRow;
import com.doctusoft.crunch.jaxb.StackoverflowComment;
import com.doctusoft.crunch.util.DomainClass;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.XmlSource;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

import java.io.Serializable;

/**
 * Created by cskassai on 28/09/16.
 */
@Log4j2
public class StackOverFlowPipelineStep2 implements Serializable {

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

        PCollection<StackOverFlowXmlRow> posts = pipeline.apply(Read.from(postXmlSource)).setCoder(SerializableCoder.of(StackOverFlowXmlRow.class));

        final DomainClass<StackOverFlowXmlRow> postDomainClass = DomainClass.of(StackOverFlowXmlRow.class);

        PCollection<TableRow> bqRows = posts.apply(ParDo.of(new DoFn<StackOverFlowXmlRow, TableRow>() {
            @Override
            public void processElement(ProcessContext processContext) throws Exception {
                StackOverFlowXmlRow s = processContext.element();

                TableRow tableRow = postDomainClass.writeBq(s);

                processContext.output(tableRow);
            }
        }));

        bqRows.apply(BigQueryIO.Write
                .withSchema(postDomainClass.getBqSchema())
                .to(postDomainClass.createTable(pipeline.getOptions().as(DataflowPipelineOptions.class).getProject(), "stackoverflow").getTableReference())
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

        XmlSource<StackoverflowComment> commentXmlSource = XmlSource
                .<StackoverflowComment>from("gs://ds-bq-demo-eu/stackoverflow/Comments.xml")
                .withRootElement("comments")
                .withRecordElement("row")
                .withRecordClass(StackoverflowComment.class);

        PCollection<StackoverflowComment> comments = pipeline.apply(Read.from(commentXmlSource)).setCoder(SerializableCoder.of(StackoverflowComment.class));

        final DomainClass<StackoverflowComment> commentDomainClass = DomainClass.of(StackoverflowComment.class);

        PCollection<TableRow> commentRows = comments.apply(ParDo.of(new DoFn<StackoverflowComment, TableRow>() {
            @Override
            public void processElement(ProcessContext processContext) throws Exception {
                StackoverflowComment s = processContext.element();
                TableRow tableRow = commentDomainClass.writeBq(s);
                processContext.output(tableRow);
            }
        }));

        bqRows.apply(BigQueryIO.Write
                .withSchema(commentDomainClass.getBqSchema())
                .to(commentDomainClass.createTable(pipeline.getOptions().as(DataflowPipelineOptions.class).getProject(), "stackoverflow").getTableReference())
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));





        pipeline.run();
    }
}
