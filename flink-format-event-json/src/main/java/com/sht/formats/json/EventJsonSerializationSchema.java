package com.sht.formats.json;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

public class EventJsonSerializationSchema implements SerializationSchema<RowData> {

    public EventJsonSerializationSchema(RowType rowType, TimestampFormat timestampFormat) {

    }

    @Override
    public byte[] serialize(RowData rowData) {
        return new byte[0];
    }
}
