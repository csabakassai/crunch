package com.doctusoft.crunch.cheat;

import com.doctusoft.crunch.BQWritePTransform;
import com.doctusoft.crunch.model.StackOverflowComment;
import com.doctusoft.crunch.model.StackOverflowPost;
import com.doctusoft.crunch.model.StackOverflowThread;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
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
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

import java.io.Serializable;
import java.util.Iterator;

@Log4j2
public class StackOverflowPipelineStep3 implements Serializable {

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

        posts.apply("Writing posts into BQ", new BQWritePTransform(StackOverflowPost.class, "post"));


        XmlSource<StackOverflowComment> commentXmlSource = XmlSource
                .<StackOverflowComment>from(StackOverflowPipelineConstants.COMMENTS_SAMPLE_LOCATION)
                .withRootElement("comments")
                .withRecordElement("row")
                .withRecordClass(StackOverflowComment.class);

        PCollection<StackOverflowComment> comments = pipeline
                .apply("Reading comments", Read.from(commentXmlSource))
                .setCoder(SerializableCoder.of(StackOverflowComment.class));

        comments.apply("Writing comments into BQ", new BQWritePTransform(StackOverflowComment.class, "comment"));


        PCollection<KV<String, StackOverflowPost>> questionsAndAnswers = posts
                .apply("Mapping by question id", ParDo.of(new DoFn<StackOverflowPost, KV<String, StackOverflowPost>>() {
                    @Override
                    public void processElement(ProcessContext processContext) throws Exception {
                        StackOverflowPost element = processContext.element();
                        String postTypeId = element.getPostTypeId();
                        switch (postTypeId) {
                            case "1":
                                processContext.output(KV.of(element.getId(), element));
                                break;
                            case "2":
                                processContext.output(KV.of(element.getParentID(), element));
                                break;
                            default:
                                log.warn("Unsupported posTypeId: {}", postTypeId);
                        }
                    }
                }));

        PCollection<KV<String, Iterable<StackOverflowPost>>> questionsWithItsAnswers = questionsAndAnswers.apply(GroupByKey.create());

        PCollection<StackOverflowThread> threads = questionsWithItsAnswers.apply("Joining answers to questions",
                ParDo.of(new DoFn<KV<String, Iterable<StackOverflowPost>>, StackOverflowThread>() {
                    @Override
                    public void processElement(ProcessContext processContext) throws Exception {

                        KV<String, Iterable<StackOverflowPost>> element = processContext.element();
                        Iterable<StackOverflowPost> postsByKey = element.getValue();

                        StackOverflowThread thread = new StackOverflowThread();

                        Iterator<StackOverflowPost> iterator = postsByKey.iterator();
                        while (iterator.hasNext()) {
                            StackOverflowPost post = iterator.next();
                            String postTypeId = post.getPostTypeId();
                            switch (postTypeId) {
                                case "1":
                                    if(thread.getQuestion() != null) {
                                        log.error("More question with id: {}", post.getId());
                                    }
                                    thread.setQuestion(post);
                                    break;
                                case "2":
                                    thread.getAnswers().add(post);
                                    break;
                                default:
                                    log.warn("Unsupported posTypeId: {}", postTypeId);
                            }
                        }

                        if (thread.getQuestion() == null) {
                            log.error("No question for id: {}", element.getKey());
                        } else {
                            processContext.output(thread);
                        }
                    }
                }));


        threads.apply("Writing threads into BQ", new BQWritePTransform(StackOverflowThread.class, "thread"));


        pipeline.run();
    }
}
