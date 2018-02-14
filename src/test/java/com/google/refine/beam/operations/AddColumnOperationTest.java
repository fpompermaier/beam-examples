/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.refine.beam.operations;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableRow;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AddColumnOperationTest {

  private static final String COL3_NAME = "col3";

  private static final String COL2_NAME = "col2";

  private static final String COL1_NAME = "col1";

  private static final String ADD_COLUMN_OP = "{\n" //
      + "    \"op\": \"core/column-addition\",\n"
      + "    \"description\": \"Create column test at index 2 based on column col2 using expression grel:value + \\\" test\\\"\",\n"
      + "    \"engineConfig\": {\n" + "      \"mode\": \"row-based\",\n" + "      \"facets\": []\n"
      + "    },\n" + "    \"newColumnName\": \"col3\",\n" + "    \"columnInsertIndex\": 2,\n"
      + "    \"baseColumnName\": \"col2\",\n"
      + "    \"expression\": \"grel:value + \\\" test\\\"\",\n"
      + "    \"onError\": \"set-to-blank\"\n" //
      + "  }\n";

  private static final String[] IN_CSV_LINES = new String[] {"Matteo,Salvini", "Matteo,Renzi"};
  private static final String[] OUT_CSV_LINES =
      new String[] {"Matteo,Salvini,Salvini test", "Matteo,Renzi,Renzi test"};
  private static final ObjectMapper OBJ_MAPPER = new ObjectMapper();

  @Rule
  public TestPipeline p = TestPipeline.create();

  /** Example test that tests a PTransform by using an in-memory input and inspecting the output. */
  @Test
  @Category(ValidatesRunner.class)
  public void testAddColumnOp() throws Exception {
    
    final List<String> strRows = Arrays.asList(IN_CSV_LINES);
    final JsonNode operationJson = OBJ_MAPPER.readTree(ADD_COLUMN_OP);
    final String srcColumn = operationJson.get("baseColumnName").asText();
    final String targetColumn = operationJson.get("newColumnName").asText();
    // TODO String[] facets = operationJson.get("engineConfig").get("facets");
    // TODO int colPos = operationJson.getInt("columnInsertIndex");
    final String exp = operationJson.get("expression").asText();

    PCollection<TableRow> input = p.apply(Create.of(strRows)).setCoder(StringUtf8Coder.of())
        .apply(ParDo.of(new CsvLineToTableRowFn()));

    PCollection<TableRow> transformedRows =
        input.apply("addColumn", MapElements.via(new AddColumnFn(srcColumn, targetColumn, exp)));

    PCollection<String> output = transformedRows//
        .apply("toCsv", MapElements.into(TypeDescriptors.strings())//
        .via((TableRow row) -> row.get(COL1_NAME) + "," + row.get(COL2_NAME) + "," + row.get(COL3_NAME)));

    PAssert.that(output).containsInAnyOrder(OUT_CSV_LINES);

    p.run().waitUntilFinish();
  }

  /**
   * Prepares the data for writing to BigQuery by building a TableRow object containing an integer
   * representation of month and the number of tornadoes that occurred in each month.
   */
  private static class CsvLineToTableRowFn extends DoFn<String, TableRow> {

    private static final long serialVersionUID = 1L;

    @ProcessElement
    public void processElement(ProcessContext c) {
      final String[] split = c.element().split(",");
      TableRow row = new TableRow()//
          .set(COL1_NAME, split[0])//
          .set(COL2_NAME, split[1]);
      c.output(row);
    }
  }

}
