package com.doctusoft.crunch.cheat;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import lombok.extern.log4j.Log4j2;

import java.io.Serializable;

/**
 * Created by cskassai on 28/09/16.
 */
@Log4j2
public class StackOverFlowPipelineStep0 implements Serializable {

    public static void main(String[] args) {

        PipelineOptions dataflowPipelineOptions = PipelineOptionsFactory.create();


        Pipeline pipeline = Pipeline.create(dataflowPipelineOptions);

        PCollection<String> lines = pipeline.apply(TextIO.Read.from("gs://ds-bq-demo/stackoverflow/Posts-small-sample.xml"));

        lines = lines.apply(ParDo.of(new DoFn<String, String>() {

            @Override
            public void processElement(ProcessContext processContext) throws Exception {
                String line = processContext.element();
                log.info(line);
            }
        }));

        PCollection<Long> lineCount = lines.apply(Count.globally());


        lineCount.apply(ParDo.of(new DoFn<Long, Long>() {

            @Override
            public void processElement(ProcessContext processContext) throws Exception {
                log.info("Number of lines in file {}", processContext.element());
            }
        }));

        pipeline.run();
    }
}
