# Event Json Format

该table format主要用于解析行为打点的json格式数据。众所周知，事件数据格式只有少部分字段是固定的，其他字段都是用户自定义的，用在特定的场景。这样对业务SQL化带来一定的空难，event json用来解析此类数据，公共字段解析到用户自定义的schema，其他字段存入format 源数据中，以json 字符串的形式存入default字段。