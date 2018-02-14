package com.google.refine.beam.operations;

import com.google.api.services.bigquery.model.TableRow;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.beam.sdk.transforms.SimpleFunction;

public class AddColumnFn extends SimpleFunction<TableRow, TableRow> {

  private static final long serialVersionUID = 1L;

  private String srcCol;
  private String targetCol;
  private String exp;

  public AddColumnFn(String srcCol, String targetCol, String exp) {
    this.srcCol = srcCol;
    this.targetCol = targetCol;
    this.exp = exp;
  }

  @Override
  public TableRow apply(TableRow input) {
    Object value = evaluateExp(input, srcCol, exp);
    TableRow ret = new TableRow();
    ret.set("col1", input.get("col1"));
    ret.set("col2", input.get("col2"));
    // TODO add in the right position/index
    ret.set(targetCol, value);
    return ret;
  }

  private Object evaluateExp(TableRow row, String srcColumn, String expression) {
    if (expression.startsWith("grel:")) {
      expression = expression.substring("grel:".length());
    }
    // TODO improve expression handling
    final StringBuilder ret = new StringBuilder();
    final String[] splits = expression.split(Pattern.quote("+"));
    for (String split : splits) {
      split = split.trim();
      if (split.equals("value")) {
        final Object fieldVal = row.get(srcColumn);
        split = fieldVal == null ? "" : fieldVal.toString();
      }
      // } else if (split.startsWith("cells")) {
      // Matcher matcher = cellValuePattern.matcher(split);
      // if (matcher.find()) {
      // final Object fieldVal = row.getField(cachedColumnPos.get(matcher.group(1)));
      // split = fieldVal == null ? "" : fieldVal.toString();
      // } else {
      // throw new IllegalArgumentException("Expression not supported yet: " + expression);
      // }
      // } else {
      split = split.replace("\"", "");
      // }
      ret.append(split);
    }
    return ret.toString();
  }
}
