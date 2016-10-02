package com.doctusoft.crunch;

import com.doctusoft.crunch.model.BQWriteable;
import com.doctusoft.crunch.model.StackOverflowPost;
import com.doctusoft.crunch.util.BQUtil;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BQWritePTransform<T extends BQWriteable> extends PTransform<PCollection<T>, PDone> {

    private final Class<T> entityClass;
    private final String tableName;

    @Override
    public PDone apply(PCollection<T> input) {

        PCollection<TableRow> bqRows = input.apply("Converting objects to BQ rows", ParDo.of(new DoFn<T, TableRow>() {
            @Override
            public void processElement(ProcessContext processContext) throws Exception {
                T s = processContext.element();
                TableRow tableRow = s.toBQTableRow();
                processContext.output(tableRow);
            }
        }));

        DataflowPipelineOptions dataflowPipelineOptions = input.getPipeline().getOptions().as(DataflowPipelineOptions.class);

        return bqRows.apply("Writing objects to BQ", BigQueryIO.Write
                .withSchema(BQUtil.getSchema(entityClass))
                .to(new TableReference()
                        .setProjectId(dataflowPipelineOptions.getProject())
                        .setDatasetId("stackoverflow")
                        .setTableId(tableName))
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
    }
}
