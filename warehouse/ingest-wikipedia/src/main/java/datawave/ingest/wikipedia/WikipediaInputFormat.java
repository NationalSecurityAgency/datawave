/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package datawave.ingest.wikipedia;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import datawave.ingest.data.RawRecordContainer;

public class WikipediaInputFormat extends SequenceFileInputFormat<LongWritable,RawRecordContainer> {

    @Override
    public RecordReader<LongWritable,RawRecordContainer> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new RecordReader<LongWritable,RawRecordContainer>() {

            private WikipediaRecordReader delegate = null;

            @Override
            public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
                delegate = new WikipediaRecordReader();
                delegate.initialize(split, context);
            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                return delegate.nextKeyValue();
            }

            @Override
            public LongWritable getCurrentKey() throws IOException, InterruptedException {
                return delegate.getCurrentKey();
            }

            @Override
            public RawRecordContainer getCurrentValue() throws IOException, InterruptedException {
                return delegate.getEvent();
            }

            @Override
            public float getProgress() throws IOException, InterruptedException {
                return delegate.getProgress();
            }

            @Override
            public void close() throws IOException {
                delegate.close();
                delegate = null;
            }
        };
    }
}
