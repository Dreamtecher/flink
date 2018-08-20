/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.stream.sql;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.utils.JavaStreamTestData;
import org.apache.flink.table.runtime.utils.StreamITCase;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Integration tests for streaming SQL.
 */
public class JavaSqlITCase extends AbstractTestBase {

	@Test
	public void testRowRegisterRowWithNames() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		StreamITCase.clear();

		List<Row> data = new ArrayList<>();
		data.add(Row.of(1, 1L, "Hi"));
		data.add(Row.of(2, 2L, "Hello"));
		data.add(Row.of(3, 2L, "Hello world"));

		TypeInformation<?>[] types = {
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.LONG_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO};
		String[] names = {"a", "b", "c"};

		RowTypeInfo typeInfo = new RowTypeInfo(types, names);

		DataStream<Row> ds = env.fromCollection(data).returns(typeInfo);

		Table in = tableEnv.fromDataStream(ds, "a,b,c");
		tableEnv.registerTable("MyTableRow", in);

		String sqlQuery = "SELECT a,c FROM MyTableRow";
		Table result = tableEnv.sqlQuery(sqlQuery);

		DataStream<Row> resultSet = tableEnv.toAppendStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink<Row>());
		env.execute();

		List<String> expected = new ArrayList<>();
		expected.add("1,Hi");
		expected.add("2,Hello");
		expected.add("3,Hello world");

		StreamITCase.compareWithList(expected);
	}

	@Test
	public void testSelect() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		StreamITCase.clear();

		DataStream<Tuple3<Integer, Long, String>> ds = JavaStreamTestData.getSmall3TupleDataSet(env);
		Table in = tableEnv.fromDataStream(ds, "a,b,c");
		tableEnv.registerTable("MyTable", in);

		String sqlQuery = "SELECT * FROM MyTable";
		Table result = tableEnv.sqlQuery(sqlQuery);

		DataStream<Row> resultSet = tableEnv.toAppendStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink<Row>());
		env.execute();

		List<String> expected = new ArrayList<>();
		expected.add("1,1,Hi");
		expected.add("2,2,Hello");
		expected.add("3,2,Hello world");

		StreamITCase.compareWithList(expected);
	}

	@Test
	public void testFilter() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		StreamITCase.clear();

		DataStream<Tuple5<Integer, Long, Integer, String, Long>> ds = JavaStreamTestData.get5TupleDataStream(env);
		tableEnv.registerDataStream("MyTable", ds, "a, b, c, d, e");

		String sqlQuery = "SELECT a, b, e FROM MyTable WHERE c < 4";
		Table result = tableEnv.sqlQuery(sqlQuery);

		DataStream<Row> resultSet = tableEnv.toAppendStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink<Row>());
		env.execute();

		List<String> expected = new ArrayList<>();
		expected.add("1,1,1");
		expected.add("2,2,2");
		expected.add("2,3,1");
		expected.add("3,4,2");

		StreamITCase.compareWithList(expected);
	}

	@Test
	public void testUnion() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		StreamITCase.clear();

		DataStream<Tuple3<Integer, Long, String>> ds1 = JavaStreamTestData.getSmall3TupleDataSet(env);
		Table t1 = tableEnv.fromDataStream(ds1, "a,b,c");
		tableEnv.registerTable("T1", t1);

		DataStream<Tuple5<Integer, Long, Integer, String, Long>> ds2 = JavaStreamTestData.get5TupleDataStream(env);
		tableEnv.registerDataStream("T2", ds2, "a, b, d, c, e");

		String sqlQuery = "SELECT * FROM T1 " +
							"UNION ALL " +
							"(SELECT a, b, c FROM T2 WHERE a	< 3)";
		Table result = tableEnv.sqlQuery(sqlQuery);

		DataStream<Row> resultSet = tableEnv.toAppendStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink<Row>());
		env.execute();

		List<String> expected = new ArrayList<>();
		expected.add("1,1,Hi");
		expected.add("2,2,Hello");
		expected.add("3,2,Hello world");
		expected.add("1,1,Hallo");
		expected.add("2,2,Hallo Welt");
		expected.add("2,3,Hallo Welt wie");

		StreamITCase.compareWithList(expected);
	}

	@Test
	public void test() throws Exception {
		MatchRecognizeCondition$45 condition = new MatchRecognizeCondition$45();
		condition.filter(Row.of("ACME", 4L, 13, 1), new IterativeCondition.Context() {
			@Override
			public Iterable getEventsForPattern(String name) throws Exception {
				return Collections.singletonList(Row.of("ACME", 5L, 10, 1));
			}
		});
	}


	public class MatchRecognizeCondition$45 extends org.apache.flink.cep.pattern.conditions.IterativeCondition {

		@Override
		public boolean filter(
			Object _in1, org.apache.flink.cep.pattern.conditions.IterativeCondition.Context ctx)
			throws Exception {

			org.apache.flink.types.Row in1 = (org.apache.flink.types.Row) _in1;



			boolean isNull$26 = (java.lang.Integer) in1.getField(2) == null;
			int result$25;
			if (isNull$26) {
				result$25 = -1;
			}
			else {
				result$25 = (java.lang.Integer) in1.getField(2);
			}

			int visitedEventNumber$31 = 0;
			int eventIndex$30;
			java.lang.Integer result$28 = -1;
			boolean isNull$29 = true;
			do {

				java.util.List patternEvents$27 = new java.util.ArrayList();

				for (Object event$32 : ctx
					.getEventsForPattern("DOWN")) {
					patternEvents$27.add(event$32);
				}

				eventIndex$30 = patternEvents$27.size() - (1 - visitedEventNumber$31);
				if (eventIndex$30 >= 0) {
					result$28 = (java.lang.Integer) ((org.apache.flink.types.Row) patternEvents$27.get(eventIndex$30))
						.getField(2);
					isNull$29 = false;
					break;
				}

				visitedEventNumber$31 += patternEvents$27.size();
				patternEvents$27.clear();

			} while (false);

			boolean isNull$34 = isNull$26 || isNull$29;
			boolean result$33;
			if (isNull$34) {
				result$33 = false;
			}
			else {
				result$33 = result$25 < result$28;
			}


			boolean result$43 = true;
			boolean isNull$44 = false;
			if (!isNull$34 && result$33) {
				// left expr is true, skip right expr
			} else {

				int visitedEventNumber$39 = 0;
				int eventIndex$38;
				java.lang.Integer result$36 = -1;
				boolean isNull$37 = true;
				do {

					java.util.List patternEvents$35 = new java.util.ArrayList();

					for (Object event$40 : ctx
						.getEventsForPattern("DOWN")) {
						patternEvents$35.add(event$40);
					}

					eventIndex$38 = patternEvents$35.size() - (1 - visitedEventNumber$39);
					if (eventIndex$38 >= 0) {
						result$36 = (java.lang.Integer) ((org.apache.flink.types.Row) patternEvents$35.get(eventIndex$38))
							.getField(2);
						isNull$37 = false;
						break;
					}

					visitedEventNumber$39 += patternEvents$35.size();
					patternEvents$35.clear();

				} while (false);

				boolean result$41 = isNull$37;
				boolean isNull$42 = false;

				if (!isNull$34 && !isNull$42) {
					result$43 = result$33 || result$41;
					isNull$44 = false;
				}
				else if (!isNull$34 && result$33 && isNull$42) {
					result$43 = true;
					isNull$44 = false;
				}
				else if (!isNull$34 && !result$33 && isNull$42) {
					result$43 = false;
					isNull$44 = true;
				}
				else if (isNull$34 && !isNull$42 && result$41) {
					result$43 = true;
					isNull$44 = false;
				}
				else if (isNull$34 && !isNull$42 && !result$41) {
					result$43 = false;
					isNull$44 = true;
				}
				else {
					result$43 = false;
					isNull$44 = true;
				}
			}

			return result$43;

		}
	}

}
