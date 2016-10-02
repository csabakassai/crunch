package com.doctusoft.crunch.model;

import com.doctusoft.crunch.util.BQUtil;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class StackOverflowThread implements Serializable, BQWriteable {

    private StackOverflowPost question;

    private List<StackOverflowPost> answers = Lists.newArrayList();

    public static final TableSchema BQ_TABLE_SCHEMA = new TableSchema();

    static {
        TableFieldSchema questionField = new TableFieldSchema()
                                                .setName("question")
                                                .setMode(BQUtil.REQUIRED)
                                                .setType(BQUtil.RECORD)
                                                .setFields(StackOverflowPost.BQ_FIELDS);

        TableFieldSchema answersField = new TableFieldSchema()
                                                .setName("answers")
                                                .setMode(BQUtil.REPEATED)
                                                .setType(BQUtil.RECORD)
                                                .setFields(StackOverflowPost.BQ_FIELDS);

        BQ_TABLE_SCHEMA.setFields(Lists.newArrayList(
                                    questionField,
                                    answersField));


    }

    @Override
    public TableRow toBQTableRow() {

        TableRow tableRow = new TableRow();

        TableRow questionRow = question.toBQTableRow();
        tableRow.set("question", questionRow);

        List<TableRow> answerRows = answers.stream().map(StackOverflowPost::toBQTableRow).collect(Collectors.toList());
        tableRow.set("answers", answerRows);

        return tableRow;
    }
}
