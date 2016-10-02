package com.doctusoft.crunch;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class StackOverflowPipeline {

    public static void main(String[] args) {

        DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
        options.setProject("ds-bq-demo");
        options.setRunner(DataflowPipelineRunner.class);
        options.setStagingLocation("gs://ds-bq-demo-eu/stackoverflow/stage/");


        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> postLines = pipeline.apply(TextIO.Read.from("gs://ds-bq-demo/stackoverflow/Posts-small-sample.xml"));

        postLines.apply(ParDo.of(new DoFn<String, String>() {
            @Override
            public void processElement(ProcessContext c) throws Exception {

            }
        }));

        pipeline.run();
    }
}
