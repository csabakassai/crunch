package com.doctusoft.crunch.cheat;

import com.doctusoft.crunch.jaxb.StackOverFlowXmlRow;
import com.doctusoft.crunch.model.StackOverflowQuestion;
import com.doctusoft.crunch.util.DomainClass;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.XmlSource;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
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
public class StackOverFlowPipelineStep3 implements Serializable {

    @SneakyThrows
    public static void main(String[] args) {

        DataflowPipelineOptions dataflowPipelineOptions = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
        dataflowPipelineOptions.setRunner(DataflowPipelineRunner.class);
        dataflowPipelineOptions.setProject("ds-bq-demo");
        dataflowPipelineOptions.setZone("europe-west1-b");
        dataflowPipelineOptions.setWorkerMachineType("n1-highmem-8");
        dataflowPipelineOptions.setNumWorkers(16);

        dataflowPipelineOptions.setStagingLocation("gs://ds-bq-demo-eu/staging/");
        dataflowPipelineOptions.setTempLocation("gs://ds-bq-demo-eu/temp/");

        Pipeline pipeline = Pipeline.create(dataflowPipelineOptions);

        XmlSource<StackOverFlowXmlRow> xmlSource = XmlSource
                .<StackOverFlowXmlRow>from("gs://ds-bq-demo-eu/stackoverflow/Posts-sample.xml")
                .withRootElement("posts")
                .withRecordElement("row")
                .withRecordClass(StackOverFlowXmlRow.class);

        PCollection<StackOverFlowXmlRow> rows = pipeline.apply(Read.from(xmlSource));



        PCollection<KV<String, StackOverFlowXmlRow>> questionsAndAnswers = rows
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
