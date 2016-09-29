package com.doctusoft.crunch.cheat;

import com.doctusoft.crunch.jaxb.StackOverFlowXmlRow;
import com.doctusoft.crunch.jaxb.StackoverflowComment;
import com.doctusoft.crunch.model.StackOverflowQuestion;
import com.doctusoft.crunch.util.DomainClass;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.CannotProvideCoderException;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderProvider;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.XmlSource;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.WithKeys;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

/**
 * Created by cskassai on 28/09/16.
 */
@Log4j2
public class StackOverFlowPipelineStep4 implements Serializable {

    @SneakyThrows
    public static void main(String[] args) {

        DataflowPipelineOptions dataflowPipelineOptions = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
        dataflowPipelineOptions.setRunner(DataflowPipelineRunner.class);
        dataflowPipelineOptions.setProject("ds-bq-demo");
        dataflowPipelineOptions.setZone("europe-west1-b");
        dataflowPipelineOptions.setWorkerMachineType("n1-highmem-8");
        dataflowPipelineOptions.setNumWorkers(32);

        dataflowPipelineOptions.setStagingLocation("gs://ds-bq-demo-eu/staging/");
        dataflowPipelineOptions.setTempLocation("gs://ds-bq-demo-eu/temp/");

        Pipeline pipeline = Pipeline.create(dataflowPipelineOptions);

        XmlSource<StackOverFlowXmlRow> postXmlSource = XmlSource
                .<StackOverFlowXmlRow>from("gs://ds-bq-demo-eu/stackoverflow/Posts.xml")
                .withRootElement("posts")
                .withRecordElement("row")
                .withRecordClass(StackOverFlowXmlRow.class);

        PCollection<StackOverFlowXmlRow> posts = pipeline.apply(Read.from(postXmlSource)).setCoder(SerializableCoder.of(StackOverFlowXmlRow.class));;

        XmlSource<StackoverflowComment> commentXmlSource = XmlSource
                .<StackoverflowComment>from("gs://ds-bq-demo-eu/stackoverflow/Comments.xml")
                .withRootElement("comments")
                .withRecordElement("row")
                .withRecordClass(StackoverflowComment.class);

        PCollection<StackoverflowComment> comments = pipeline.apply(Read.from(commentXmlSource)).setCoder(SerializableCoder.of(StackoverflowComment.class));
//        PCollection<String> postlines = pipeline.apply(TextIO.Read.from("gs://ds-bq-demo-eu/stackoverflow/Posts.xml"));

//        PCollection<StackOverFlowXmlRow> posts = postlines.apply(ParDo.of(new XmlParserDoFn())).setCoder(SerializableCoder.of(StackOverFlowXmlRow.class));

//        PCollection<String> commentlines = pipeline.apply(TextIO.Read.from("gs://ds-bq-demo-eu/stackoverflow/Comment-samples.xml"));
//
//        PCollection<StackoverflowComment> comments = commentlines.apply(ParDo.of(new XmlParserDoFn())).setCoder(SerializableCoder.of(StackoverflowComment.class));


        PCollection<KV<String, StackoverflowComment>> commentByPostId =
                comments.apply(WithKeys.of(StackoverflowComment::getPostId))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(StackoverflowComment.class)));

        PCollection<KV<String, StackOverFlowXmlRow>> postsByPostId =
                posts.apply(WithKeys.of(StackOverFlowXmlRow::getId))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(StackOverFlowXmlRow.class)));


        TupleTag<StackoverflowComment> commentTag = new TupleTag<StackoverflowComment>() {};
        TupleTag<StackOverFlowXmlRow> postTag = new TupleTag<StackOverFlowXmlRow>() {};

        PCollection<KV<String, CoGbkResult>> commentsAndPostsByPostId = KeyedPCollectionTuple
                .of(postTag, postsByPostId)
                .and(commentTag, commentByPostId)
                .apply(CoGroupByKey.create());

        posts = commentsAndPostsByPostId.apply("Joining comments", ParDo.of(new DoFn<KV<String, CoGbkResult>, StackOverFlowXmlRow>() {

            @Override
            public void processElement(ProcessContext processContext) throws Exception {
                KV<String, CoGbkResult> element = processContext.element();
                CoGbkResult value = element.getValue();
                Iterable<StackOverFlowXmlRow> posts = value.getAll(postTag);
                Iterable<StackoverflowComment> comments = value.getAll(commentTag);

                Iterator<StackOverFlowXmlRow> postIterator = posts.iterator();
                if(!postIterator.hasNext()) {
                    log.error("No post found to postId: {}", element.getKey());
                } else {
                    StackOverFlowXmlRow post = postIterator.next();
                    post.setComments(Lists.newArrayList(comments));
                    processContext.output(post);
                }

            }
        }));


        PCollection<KV<String, StackOverFlowXmlRow>> questionsAndAnswers = posts
//                .apply(Filter.byPredicate((StackOverFlowXmlRow row) -> row.getPostTypeId()"1")
                .apply(ParDo.of(new DoFn<StackOverFlowXmlRow, KV<String, StackOverFlowXmlRow>>() {
                    @Override
                    public void processElement(ProcessContext processContext) throws Exception {
                        StackOverFlowXmlRow element = processContext.element();
                        switch (element.getPostTypeId()) {
                            case "1":
                                processContext.output(KV.of(element.getId(), element));
                                break;
                            case "2":
                                processContext.output(KV.of(element.getParentID(), element));
                                break;
                            default:
                                log.warn("Unsupported posTypeId: {}", element.getPostTypeId());
                        }
                    }
                }));

        PCollection<KV<String, Iterable<StackOverFlowXmlRow>>> questionsWithItsAnswers = questionsAndAnswers.apply(GroupByKey.create());

        PCollection<StackOverflowQuestion> questions = questionsWithItsAnswers.apply(ParDo.of(new DoFn<KV<String, Iterable<StackOverFlowXmlRow>>, StackOverflowQuestion>() {
            @Override
            public void processElement(ProcessContext processContext) throws Exception {
                KV<String, Iterable<StackOverFlowXmlRow>> element = processContext.element();

                Iterable<StackOverFlowXmlRow> value = element.getValue();

                StackOverflowQuestion question = null;
                List<StackOverFlowXmlRow> answers = Lists.newArrayList();

                Iterator<StackOverFlowXmlRow> iterator = value.iterator();
                while (iterator.hasNext()) {
                    StackOverFlowXmlRow row = iterator.next();
                    switch (row.getPostTypeId()) {
                        case "1":
                            Preconditions.checkArgument(question == null);
                            question = new StackOverflowQuestion();
                            question.setQuestion(row);
                            break;
                        case "2":
                            answers.add(row);
                            break;
                        default:
                            log.warn("Unsupported posTypeId: {}", row.getPostTypeId());
                    }
                }

                if (question == null) {
                    log.error("No question for id: {}", element.getKey());
                } else {
                    question.setAnswers(answers);
                    processContext.output(question);
                }

            }
        }));


        final DomainClass<StackOverflowQuestion> dom = DomainClass.of(StackOverflowQuestion.class);



        PCollection<TableRow> bqRows = questions.apply(ParDo.of(new DoFn<StackOverflowQuestion, TableRow>() {
            @Override
            public void processElement(ProcessContext processContext) throws Exception {
                StackOverflowQuestion s = processContext.element();

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
