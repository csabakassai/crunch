package com.doctusoft.crunch.cheat;

import com.doctusoft.crunch.model.StackOverflowPost;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import lombok.extern.log4j.Log4j2;

import java.io.Serializable;

@Log4j2
public class StackOverflowPipelineStep0Local implements Serializable {

    public static void main(String[] args) {

        Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.create());
        PCollection<String> lines = pipeline.apply(TextIO.Read.from("gs://ds-bq-demo-eu/stackoverflow/Posts-small-sample.xml"));
        lines.apply(ParDo.of(new DoFn<String, StackOverflowPost>() {
            @Override
            public void processElement(ProcessContext c) throws Exception {
                String element = c.element();
                if(element.hashCode() % 100 == 0) {
                    log.info(element);
                }

            }
        }));
        pipeline.run();
    }
}
