package org.gradoop.flink.io.filter;

import org.apache.hadoop.hbase.filter.Filter;

public interface HBaseFilter extends Expression{

  Filter getHBaseFilter();

}
