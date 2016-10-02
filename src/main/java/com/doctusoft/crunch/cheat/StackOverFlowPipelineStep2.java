package com.doctusoft.crunch.cheat;

import com.doctusoft.crunch.model.StackOverflowComment;
import com.doctusoft.crunch.model.StackOverflowPost;
import com.google.api.services.bigquery.model.TableReference;
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

@Log4j2
public class StackOverflowPipelineStep2 implements Serializable {

    @SneakyThrows
    public static void main(String[] args) {

        DataflowPipelineOptions dataflowPipelineOptions = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
        dataflowPipelineOptions.setRunner(DataflowPipelineRunner.class);
        dataflowPipelineOptions.setProject(StackOverflowPipelineConstants.PROJECT_ID);
        dataflowPipelineOptions.setZone(StackOverflowPipelineConstants.ZONE);
        dataflowPipelineOptions.setStagingLocation(StackOverflowPipelineConstants.STAGING_LOCATION);

        Pipeline pipeline = Pipeline.create(dataflowPipelineOptions);


        XmlSource<StackOverflowPost> postXmlSource = XmlSource
                .<StackOverflowPost>from(StackOverflowPipelineConstants.POSTS_SAMPLE_LOCATION)
                .withRootElement("posts")
                .withRecordElement("row")
                .withRecordClass(StackOverflowPost.class);

        PCollection<StackOverflowPost> posts = pipeline
                .apply("Reading posts", Read.from(postXmlSource))
                .setCoder(SerializableCoder.of(StackOverflowPost.class));

        PCollection<TableRow> bqRows = posts.apply("Converting posts to BQ rows", ParDo.of(new DoFn<StackOverflowPost, TableRow>() {
            @Override
            public void processElement(ProcessContext processContext) throws Exception {
                StackOverflowPost s = processContext.element();
                TableRow tableRow = s.toBQTableRow();
                processContext.output(tableRow);
            }
        }));

        bqRows.apply("Writing posts to BQ", BigQueryIO.Write
                .withSchema(StackOverflowPost.BQ_TABLE_SCHEMA)
                .to(new TableReference()
                        .setProjectId(dataflowPipelineOptions.getProject())
                        .setDatasetId("stackoverflow")
                        .setTableId("post"))
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

        XmlSource<StackOverflowComment> commentXmlSource = XmlSource
                .<StackOverflowComment>from(StackOverflowPipelineConstants.COMMENTS_SAMPLE_LOCATION)
                .withRootElement("comments")
                .withRecordElement("row")
                .withRecordClass(StackOverflowComment.class);

        PCollection<StackOverflowComment> comments = pipeline
                .apply("Reading comments", Read.from(commentXmlSource))
                .setCoder(SerializableCoder.of(StackOverflowComment.class));

        PCollection<TableRow> commentBQRows = comments.apply("Converting comments to BQ rows", ParDo.of(new DoFn<StackOverflowComment, TableRow>() {

            @Override
            public void processElement(ProcessContext processContext) throws Exception {
                StackOverflowComment s = processContext.element();
                TableRow tableRow = s.toBQTableRow();
                processContext.output(tableRow);
            }
        }));

        commentBQRows.apply("Writing comments to BQ", BigQueryIO.Write
                .withSchema(StackOverflowComment.BQ_TABLE_SCHEMA)
                .to(new TableReference()
                        .setProjectId(dataflowPipelineOptions.getProject())
                        .setDatasetId("stackoverflow")
                        .setTableId("comment"))
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));



        pipeline.run();
    }
}
