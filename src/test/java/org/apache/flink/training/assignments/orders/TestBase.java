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

package org.apache.flink.training.assignments.orders;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.training.assignments.sinks.TestSink;
import org.apache.flink.training.assignments.utils.ExerciseBase;

import java.util.List;

public abstract class TestBase<IN, OUT> {

    protected List<OUT> runApp(SourceFunction<IN> source, TestSink<OUT> sink, Testable exercise) throws Exception {
        //ExerciseBase.rates = (SourceFunction<IN>) source;

        return execute(sink, exercise);
    }

//    protected List<OUT> runApp(TestRateSource rates, TestStringSource strings, TestSink<OUT> sink, Testable exercise) throws Exception {
//        ExerciseBase.rates = rates;
//        ExerciseBase.strings = strings;
//
//        return execute(sink, exercise);
//    }

    private List<OUT> execute(TestSink<OUT> sink, Testable exercise, Testable solution) throws Exception {
        TestSink.VALUES.clear();

        ExerciseBase.out = sink;
        ExerciseBase.parallelism = 1;

        try {
            exercise.main();
        } catch (Exception e) {
            TestSink.VALUES.clear();
            throw e;
        }

        return TestSink.VALUES;
    }

    private List<OUT> execute(TestSink<OUT> sink, Testable solution) throws Exception {
        TestSink.VALUES.clear();

        ExerciseBase.out = sink;
        ExerciseBase.parallelism = 1;

        solution.main();

        return TestSink.VALUES;
    }

}
