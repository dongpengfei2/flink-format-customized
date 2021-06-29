package com.sht.formats.json;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;

public class EventJsonDeserializationSchema implements DeserializationSchema<RowData> {

    public EventJsonDeserializationSchema(RowType rowType, TypeInformation<RowData> resultTypeInfo, boolean ignoreParseErrors, TimestampFormat timestampFormatOption) {

    }

    @Override
    public RowData deserialize(byte[] bytes) throws IOException {
        return null;
    }

    @Override
    public boolean isEndOfStream(RowData rowData) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return null;
    }
}
