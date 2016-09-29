package com.doctusoft.crunch.model;

import com.doctusoft.crunch.jaxb.StackOverFlowXmlRow;
import com.doctusoft.crunch.util.Embed;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

/**
 * Created by cskassai on 28/09/16.
 */
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class StackOverflowQuestion implements Serializable {

    private StackOverFlowXmlRow question;

    private List<StackOverFlowXmlRow> answers;
}
