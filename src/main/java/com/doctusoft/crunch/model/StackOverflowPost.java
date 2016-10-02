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
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;


@XmlRootElement(name = "row")
@XmlAccessorType(XmlAccessType.FIELD)
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StackOverflowPost implements Serializable, BQWriteable {

    @XmlAttribute(name = "Id", required = true)
    @Nullable
    private String id;

    @XmlAttribute(name = "PostTypeId", required = true)
    @Nullable
    private String postTypeId;

    @XmlAttribute(name = "AcceptedAnswerId")
    @Nullable
    private String acceptedAnswerId;

    @XmlAttribute(name = "ParentId")
    @Nullable
    private String parentID;

    @XmlAttribute(name = "CreationDate")
    @XmlJavaTypeAdapter(JaxbDateTimeAdapter.class)
    @Nullable
    private LocalDateTime creationDate;

    @XmlAttribute(name = "DeletionDate")
    @XmlJavaTypeAdapter(JaxbDateTimeAdapter.class)
    @Nullable
    private LocalDateTime deletionDate;

    @XmlAttribute(name = "Score")
    @Nullable
    private Integer score;

    @XmlAttribute(name = "ViewCount")
    @Nullable
    private Integer viewCount;

    @XmlAttribute(name = "Body")
    @Nullable
    private String body;

    //(only present if user has not been deleted; always -1 for tag wiki entries, i.e. the community user owns them)
    @XmlAttribute(name = "OwnerUserId")
    @Nullable
    private String ownerUserId;

    @XmlAttribute(name = "LastEditorUserId")
    @Nullable
    private String ownerDisplayName;
    //nullable

    @XmlAttribute(name = "LastEditorUserId")
    @Nullable
    private String lastEditorUserId;
    //nullable

    @XmlAttribute(name = "LastEditorDisplayName")
    @Nullable
    private String lastEditorDisplayName;

    //"2009-03-05T22:28:34.823" - the date and time of the most recent edit to the post (nullable)
    @XmlAttribute(name = "LastEditDate")
    @XmlJavaTypeAdapter(JaxbDateTimeAdapter.class)
    @Nullable
    private LocalDateTime lastEditDate;

    //="2009-03-11T12:51:01.480" - the date and time of the most recent activity on the post. For a question, this could be the post being edited, a new answer was posted, a bounty was started, etc.
    @XmlAttribute(name = "LastActivityDate")
    @XmlJavaTypeAdapter(JaxbDateTimeAdapter.class)
    @Nullable
    private LocalDateTime lastActivityDate;

    //(nullable)
    @XmlAttribute(name = "Title")
    @Nullable
    private String title;

    //(nullable)
    @XmlAttribute(name = "Tags")
    @Nullable
    private String tags;

    @XmlAttribute(name = "AnswerCount")
    @Nullable
    private Integer answerCount;

    @XmlAttribute(name = "CommentCount")
    @Nullable
    private Integer commentCount;

    @XmlAttribute(name = "FavoriteCount")
    @Nullable
    private Integer favoriteCount;

    //(present only if the post is closed)
    @XmlAttribute(name = "ClosedDate")
    @XmlJavaTypeAdapter(JaxbDateTimeAdapter.class)
    @Nullable
    private LocalDateTime closedDate;

    //(present only if post is community wikied)
    @XmlAttribute(name = "CommunityOwnedDate")
    @XmlJavaTypeAdapter(JaxbDateTimeAdapter.class)
    @Nullable
    private LocalDateTime communityOwnedDate;

    @XmlTransient
    @Nullable
    private List<StackOverflowComment> comments = Lists.newArrayList();


    public static final TableSchema BQ_TABLE_SCHEMA = new TableSchema();

    public static final List<TableFieldSchema> BQ_FIELDS;

    static {
        TableFieldSchema idField = new TableFieldSchema();
        idField.setName("id");
        idField.setType("STRING");

        TableFieldSchema posTypeIdField = new TableFieldSchema();
        posTypeIdField.setName("pos_type_id");
        posTypeIdField.setType("STRING");

        TableFieldSchema acceptedAnswerIdField = new TableFieldSchema();
        acceptedAnswerIdField.setName("accepted_answer_id");
        acceptedAnswerIdField.setType("STRING");

        TableFieldSchema parentIdField = new TableFieldSchema();
        parentIdField.setName("parent_id");
        parentIdField.setType("STRING");

        TableFieldSchema creationDateField = new TableFieldSchema();
        creationDateField.setName("creation_date");
        creationDateField.setType("TIMESTAMP");

        TableFieldSchema deletionDateField = new TableFieldSchema();
        deletionDateField.setName("deletion_date");
        deletionDateField.setType("TIMESTAMP");

        TableFieldSchema scoreField = new TableFieldSchema();
        scoreField.setName("score");
        scoreField.setType("INTEGER");

        TableFieldSchema viewCountField = new TableFieldSchema();
        viewCountField.setName("view_count");
        viewCountField.setType("INTEGER");

        TableFieldSchema bodyField = new TableFieldSchema();
        bodyField.setName("body");
        bodyField.setType("STRING");

        TableFieldSchema ownerUserIdField = new TableFieldSchema();
        ownerUserIdField.setName("owner_user_id");
        ownerUserIdField.setType("STRING");

        TableFieldSchema ownerDisplayNameField = new TableFieldSchema();
        ownerDisplayNameField.setName("owner_display_name");
        ownerDisplayNameField.setType("STRING");

        TableFieldSchema lastEditorUserIdField = new TableFieldSchema();
        lastEditorUserIdField.setName("last_editor_user_id");
        lastEditorUserIdField.setType("STRING");

        TableFieldSchema lastEditorDisplayNameField = new TableFieldSchema();
        lastEditorDisplayNameField.setName("last_editor_display_name");
        lastEditorDisplayNameField.setType("STRING");

        TableFieldSchema lastEditDate = new TableFieldSchema();
        lastEditDate.setName("last_edit_date");
        lastEditDate.setType("TIMESTAMP");

        TableFieldSchema lastActivityDate = new TableFieldSchema();
        lastActivityDate.setName("last_activity_date");
        lastActivityDate.setType("TIMESTAMP");

        TableFieldSchema titleField = new TableFieldSchema();
        titleField.setName("title");
        titleField.setType("STRING");

        TableFieldSchema tagsField = new TableFieldSchema();
        tagsField.setName("tags");
        tagsField.setType("STRING");

        TableFieldSchema answerCountField = new TableFieldSchema();
        answerCountField.setName("answer_count");
        answerCountField.setType("INTEGER");

        TableFieldSchema commentCountField = new TableFieldSchema();
        commentCountField.setName("comment_count");
        commentCountField.setType("INTEGER");

        TableFieldSchema favoriteCountField = new TableFieldSchema();
        favoriteCountField.setName("favorite_count");
        favoriteCountField.setType("INTEGER");

        TableFieldSchema closedDateField = new TableFieldSchema();
        closedDateField.setName("closed_date");
        closedDateField.setType("TIMESTAMP");

        TableFieldSchema communityOwnedDateField = new TableFieldSchema();
        communityOwnedDateField.setName("community_owned_date");
        communityOwnedDateField.setType("TIMESTAMP");

        TableFieldSchema commentsField = new TableFieldSchema()
                .setName("comments")
                .setMode(BQUtil.REPEATED)
                .setType(BQUtil.RECORD)
                .setFields(StackOverflowComment.BQ_FIELDS);

        BQ_FIELDS = Lists.newArrayList(
                                idField,
                                posTypeIdField,
                                acceptedAnswerIdField,
                                parentIdField,
                                creationDateField,
                                deletionDateField,
                                scoreField,
                                viewCountField,
                                bodyField,
                                ownerUserIdField,
                                ownerDisplayNameField,
                                lastEditorUserIdField,
                                lastEditorDisplayNameField,
                                lastEditDate,
                                lastActivityDate,
                                titleField,
                                tagsField,
                                answerCountField,
                                commentCountField,
                                favoriteCountField,
                                closedDateField,
                                communityOwnedDateField,
                                commentsField);

        BQ_TABLE_SCHEMA.setFields(BQ_FIELDS);


    }

    public TableRow toBQTableRow() {
        TableRow tableRow = new TableRow();
        tableRow.set("id", this.getId())
                .set("pos_type_id", this.getPostTypeId())
                .set("accepted_answer_id", this.getAcceptedAnswerId())
                .set("parent_id", this.getParentID())
                .set("creation_date", BQUtil.getNullSafeBQTimeStamp(this.getCreationDate()))
                .set("deletion_date", BQUtil.getNullSafeBQTimeStamp(this.getDeletionDate()))
                .set("score", this.getScore())
                .set("view_count", this.getViewCount())
                .set("body", this.getBody())
                .set("owner_user_id", this.getOwnerUserId())
                .set("owner_display_name", this.getOwnerDisplayName())
                .set("last_editor_user_id", this.getLastEditorUserId())
                .set("last_editor_display_name", this.getLastEditorDisplayName())
                .set("last_edit_date", BQUtil.getNullSafeBQTimeStamp(this.getLastEditDate()))
                .set("last_activity_date", BQUtil.getNullSafeBQTimeStamp(this.getLastActivityDate()))
                .set("title", this.getTitle())
                .set("tags", this.getTags())
                .set("answer_count", this.getAnswerCount())
                .set("comment_count", this.getCommentCount())
                .set("favorite_count", this.getFavoriteCount())
                .set("closed_date", BQUtil.getNullSafeBQTimeStamp(this.getClosedDate()))
                .set("community_owned_date", BQUtil.getNullSafeBQTimeStamp(this.getCommunityOwnedDate()));

        List<TableRow> commentRows = comments.stream().map(StackOverflowComment::toBQTableRow).collect(Collectors.toList());

        tableRow.set("comments", commentRows);

        return tableRow;
    }

}
