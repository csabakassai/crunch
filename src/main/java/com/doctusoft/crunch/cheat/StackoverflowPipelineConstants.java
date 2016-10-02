package com.doctusoft.crunch.cheat;

public interface StackOverflowPipelineConstants {

    String STAGING_LOCATION = "gs://";
    String PROJECT_ID = "";
    String ZONE = "europe-west1-b";

    String POSTS_SAMPLE_LOCATION = "gs://ds-bq-demo-eu/stackoverflow/Posts-sample.xml";
    String POSTS_LOCATION = "gs://ds-bq-demo-eu/stackoverflow/Posts.xml";

    String COMMENTS_SAMPLE_LOCATION = "gs://ds-bq-demo-eu/stackoverflow/Comments-sample.xml";
    String COMMENTS_LOCATION = "gs://ds-bq-demo-eu/stackoverflow/Comments.xml";
}
