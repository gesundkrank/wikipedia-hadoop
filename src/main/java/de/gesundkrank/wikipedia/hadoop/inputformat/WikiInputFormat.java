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

package de.gesundkrank.wikipedia.hadoop.inputformat;

import de.gesundkrank.wikipedia.hadoop.WikiPageWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


/**
 * Inputformat for wikipedia xml dumps
 *
 * @author Jan Graßegger <jan.grassegger@uni-weimar.de>
 */
public class WikiInputFormat extends FileInputFormat<Text, WikiPageWritable> {

    @Override
    public RecordReader<Text, WikiPageWritable> createRecordReader(
            InputSplit split, TaskAttemptContext context) {

        return new WikiInputRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return !filename.getName().endsWith(".bz2");
    }


}
