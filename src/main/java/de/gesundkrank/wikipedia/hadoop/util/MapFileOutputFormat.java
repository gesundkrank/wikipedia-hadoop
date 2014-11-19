/*
 * This file is part of wikipedia-hadoop. The new internet cookbook
 * Copyright (C) 2014 Jan Graßegger
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.NumberFormat;

public class MapFileOutputFormat extends FileOutputFormat<WritableComparable,Writable> {

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
		Path file =
			new Path(committer.getWorkPath(), getUniqueFile(context, "", ""));
	    FileSystem fs = file.getFileSystem(conf);

		final MapFile.Writer out = new MapFile.Writer(
				conf,
				fs,
				file.toString(),
				context.getOutputKeyClass().asSubclass(WritableComparable.class),
				context.getOutputValueClass().asSubclass(Writable.class),
				SequenceFile.CompressionType.BLOCK);

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

	// Overrides a method in the class FileOutputFormat
	public synchronized static String getUniqueFile(TaskAttemptContext context,
			String name, String extension) {
		TaskID taskId = context.getTaskAttemptID().getTaskID();
		int partition = taskId.getId();
        return name + NUMBER_FORMAT.format(partition) + extension;
	}

}
