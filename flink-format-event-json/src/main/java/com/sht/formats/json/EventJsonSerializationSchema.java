package com.sht.formats.json;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import java.util.List;

public class EventJsonSerializationSchema implements SerializationSchema<RowData> {

    private List<RowType.RowField> rowTypeFields;
    private String others = "others";

    public EventJsonSerializationSchema(RowType rowType, TimestampFormat timestampFormat) {
        this.rowTypeFields = rowType.getFields();
    }

    @Override
    public byte[] serialize(RowData rowData) {
        JSONObject jsonObject = new JSONObject();
        for (int i=0; i< rowTypeFields.size(); i++) {
            final RowType.RowField rowField = rowTypeFields.get(i);
            final String rowLine = rowData.getString(i).toString();
            if (others.equals(rowField.getName())) {
                jsonObject.putAll(JSONObject.parseObject(rowLine));
            } else {
                jsonObject.put(rowField.getName(), rowLine);
            }
        }
        return jsonObject.toJSONString().getBytes();
    }
}
