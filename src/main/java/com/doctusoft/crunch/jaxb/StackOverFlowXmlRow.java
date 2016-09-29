package com.doctusoft.crunch.jaxb;

import com.doctusoft.crunch.util.JaxbDateTimeAdapter;
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


/**
 * Created by cskassai on 28/09/16.
 */
@XmlRootElement(name = "row")
@XmlAccessorType(XmlAccessType.FIELD)
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StackOverFlowXmlRow implements Serializable {

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
    private List<StackoverflowComment> comments;

}
