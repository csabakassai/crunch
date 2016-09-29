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
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.io.Serializable;
import java.time.LocalDateTime;

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
public class StackoverflowComment implements Serializable {

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

}
