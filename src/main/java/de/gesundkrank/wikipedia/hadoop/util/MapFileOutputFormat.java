/*
 * This file is part of wikipedia-hadoop.
 * Copyright (C) 2015 Jan Graßegger
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see [http://www.gnu.org/licenses/].
 */

package de.gesundkrank.wikipedia.hadoop.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.NumberFormat;

public class MapFileOutputFormat extends FileOutputFormat<WritableComparable, Writable> {

	private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
	  static {
	    NUMBER_FORMAT.setMinimumIntegerDigits(4);
	    NUMBER_FORMAT.setGroupingUsed(false);
	  }

	@Override
	public RecordWriter<WritableComparable, Writable> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();

		FileOutputCommitter committer =
		      (FileOutputCommitter) getOutputCommitter(context);

        SequenceFile.Writer.Option keyClass = SequenceFile.Writer.keyClass(WritableComparable.class);
        SequenceFile.Writer.Option valueClass = SequenceFile.Writer.valueClass(Writable.class);
        SequenceFile.Writer.Option compressionType =
                SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK);


        final MapFile.Writer out =
                new MapFile.Writer(conf, committer.getWorkPath(), keyClass, valueClass, compressionType);

		return new RecordWriter<WritableComparable, Writable>() {
			@Override
			public void close(TaskAttemptContext arg0) throws IOException,
					InterruptedException {
				out.close();
			}

			@Override
			public void write(WritableComparable key, Writable value) throws IOException,
					InterruptedException {
				out.append(key, value);
			}
		};
	}
}
