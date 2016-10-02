package com.doctusoft.crunch.cheat;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import lombok.extern.log4j.Log4j2;

import java.io.Serializable;

@Log4j2
public class StackOverflowPipelineStep0Dataflow implements Serializable {

    public static void main(String[] args) {

        DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
        options.setRunner(DataflowPipelineRunner.class);
        options.setStagingLocation(StackOverflowPipelineConstants.STAGING_LOCATION);
        options.setProject(StackOverflowPipelineConstants.PROJECT_ID);
        Pipeline pipeline = Pipeline.create(options);


        PCollection<String> lines = pipeline.apply(TextIO.Read.from(StackOverflowPipelineConstants.POSTS_SAMPLE_LOCATION));
        lines.apply(ParDo.of(new DoFn<String, String>() {
            @Override
            public void processElement(ProcessContext c) throws Exception {
                String line = c.element();
                if(line.hashCode()%100 == 0) {
                    log.info(line);
                }
                c.output(line);
            }
        }));

        pipeline.run();
    }
}
