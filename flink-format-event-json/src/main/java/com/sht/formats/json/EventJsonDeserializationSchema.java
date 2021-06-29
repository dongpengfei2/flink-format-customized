package com.sht.formats.json;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class EventJsonDeserializationSchema implements DeserializationSchema<RowData> {

    private TypeInformation<RowData> resultTypeInfo;

    private DataType dataType;

    public EventJsonDeserializationSchema(DataType dataType, TypeInformation<RowData> resultTypeInfo, boolean ignoreParseErrors, TimestampFormat timestampFormatOption) {
        this.resultTypeInfo = resultTypeInfo;
        this.dataType = dataType;
    }

    @Override
    public RowData deserialize(byte[] bytes) throws IOException {
        throw new RuntimeException(
            "Please invoke DeserializationSchema#deserialize(byte[], Collector<RowData>) instead.");
    }

    @Override
    public void deserialize(byte[] message, Collector<RowData> out) throws IOException {
        if (message == null || message.length == 0) {
            return;
        }

    }

    @Override
    public boolean isEndOfStream(RowData rowData) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return resultTypeInfo;
    }
}
