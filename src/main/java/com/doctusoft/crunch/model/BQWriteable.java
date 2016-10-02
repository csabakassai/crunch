package com.doctusoft.crunch.model;

import com.google.api.services.bigquery.model.TableRow;

public interface BQWriteable {

    TableRow toBQTableRow();
}
