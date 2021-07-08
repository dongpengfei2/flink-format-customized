package com.sht.formats.json;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import java.io.IOException;
import java.util.List;

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
        String line = new String(message);
        final RowType rowType = (RowType) dataType.getLogicalType();
        final List<RowType.RowField> fields = rowType.getFields();
        final JSONObject jsonObject = JSONObject.parseObject(line);
        GenericRowData rowData = new GenericRowData(fields.size());
        for (int i=0; i<fields.size(); i++) {
            final RowType.RowField rowField = fields.get(i);
            rowData.setField(i, new BinaryStringData(jsonObject.getString(rowField.getName())));
        }
        out.collect(rowData);
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
