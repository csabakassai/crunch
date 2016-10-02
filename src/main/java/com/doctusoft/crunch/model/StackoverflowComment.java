package com.doctusoft.crunch.model;

import com.doctusoft.crunch.util.BQUtil;
import com.doctusoft.crunch.util.JaxbDateTimeAdapter;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.annotation.Nullable;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.List;

@XmlRootElement(name = "row")
@XmlAccessorType(XmlAccessType.FIELD)
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StackOverflowComment implements Serializable, BQWriteable {

    @XmlAttribute(name = "Id", required = true)
    private String id;

    @XmlAttribute(name = "PostId", required = true)
    private String postId;

    @XmlAttribute(name = "Score")
    @Nullable
    private String score;

    @XmlAttribute(name = "Text")
    @Nullable
    private String text;

    @XmlAttribute(name = "CreationDate")
    @XmlJavaTypeAdapter(JaxbDateTimeAdapter.class)
    @Nullable
    private LocalDateTime creationDate;

    @XmlAttribute(name = "UserDisplayName")
    @Nullable
    private String userDisplayName;

    @XmlAttribute(name = "UserId")
    @Nullable
    private String userId;

    public static final TableSchema BQ_TABLE_SCHEMA = new TableSchema();
    public static final List<TableFieldSchema> BQ_FIELDS;

    static {
        TableFieldSchema idField = new TableFieldSchema();
        idField.setName("id");
        idField.setType("STRING");

        TableFieldSchema postIdField = new TableFieldSchema();
        postIdField.setName("post_id");
        postIdField.setType("STRING");

        TableFieldSchema scoreField = new TableFieldSchema();
        scoreField.setName("score");
        scoreField.setType("INTEGER");

        TableFieldSchema textField = new TableFieldSchema();
        textField.setName("text");
        textField.setType("STRING");

        TableFieldSchema creationDateField = new TableFieldSchema();
        creationDateField.setName("creation_date");
        creationDateField.setType("TIMESTAMP");

        TableFieldSchema userDisplayNameField = new TableFieldSchema();
        userDisplayNameField.setName("user_display_name");
        userDisplayNameField.setType("STRING");

        TableFieldSchema userIdField = new TableFieldSchema();
        userIdField.setName("user_id");
        userIdField.setType("STRING");


        BQ_FIELDS =  Lists.newArrayList(
                                idField,
                                postIdField,
                                scoreField,
                                textField,
                                creationDateField,
                                userDisplayNameField,
                                userIdField);

        BQ_TABLE_SCHEMA.setFields(BQ_FIELDS);


    }

    public TableRow toBQTableRow() {
        TableRow tableRow = new TableRow();
        tableRow.set("id", this.getId())
                .set("post_id", this.getPostId())
                .set("score", this.getScore())
                .set("text", this.getText())
                .set("creation_date", BQUtil.getNullSafeBQTimeStamp(this.getCreationDate()))
                .set("user_display_name", this.getUserDisplayName())
                .set("user_id", this.getUserId());

        return tableRow;
    }


}
