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

package org.apache.flink.cep.operator;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.compiler.NFAFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * TODO Update docs.
 * @param <IN>
 * @param <KEY>
 */
public class CoCepPatternOperator<IN, KEY> extends AbstractCoCepPatternOperator<IN, KEY , Map<String, List<IN>>> {

	public CoCepPatternOperator(
		TypeSerializer<IN> inputSerializer,
		boolean isProcessingTime,
		NFAFactory<IN> nfaFactory) {
		super(inputSerializer, isProcessingTime, nfaFactory);
	}

	@Override
	protected void processEvent(NFA<IN> nfa, IN event, long timestamp) {
		Tuple2<Collection<Map<String, List<IN>>>, Collection<Tuple2<Map<String, List<IN>>, Long>>> patterns =
			nfa.process(event, timestamp);

		emitMatchedSequences(patterns.f0, timestamp);
	}

	@Override
	protected void advanceTime(NFA<IN> nfa, long timestamp) {
		Tuple2<Collection<Map<String, List<IN>>>, Collection<Tuple2<Map<String, List<IN>>, Long>>> patterns =
			nfa.process(null, timestamp);

		emitMatchedSequences(patterns.f0, timestamp);
	}

	private void emitMatchedSequences(Iterable<Map<String, List<IN>>> matchedSequences, long timestamp) {
		Iterator<Map<String, List<IN>>> iterator = matchedSequences.iterator();

		if (iterator.hasNext()) {
			StreamRecord<Map<String, List<IN>>> streamRecord = new StreamRecord<>(null, timestamp);

			do {
				streamRecord.replace(iterator.next());
				output.collect(streamRecord);
			} while (iterator.hasNext());
		}
	}
}
