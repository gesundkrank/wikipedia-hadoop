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

import de.gesundkrank.wikipedia.hadoop.WikiRevisionWritable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Jan Graßegger<jan.grassegger@uni-weimar.de>
 */
public class WikiInputRecordReader
        extends RecordReader<LongWritable, WikiRevisionWritable> {
    private static final Logger LOGGER = Logger.getLogger(WikiInputRecordReader.class);

    private LongWritable currentId = new LongWritable();
    private WikiPageParser parser;
    private WikiRevisionWritable currentRevision;
    private FSDataInputStream currentFile;
    private BufferedReader currentReader;
    private FileSystem fs;
    private FileSplit fileSplit;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        this.fs = FileSystem.get(context.getConfiguration());
        this.parser = new WikiPageParser();
        openSplit(split);

    }

    private void openSplit(InputSplit split) {
        try {
            fileSplit = ((FileSplit) split);
            Path splitPath = fileSplit.getPath();
            currentFile = this.fs.open(splitPath);
            currentFile.skip(fileSplit.getStart() - 1);

            InputStream inputStream = currentFile;

            /*if(fileSplit.getPath().getName().endsWith(".bz2")) {
                CompressionCodec compressionCodec = new BZip2Codec();
                inputStream = compressionCodec.createInputStream(currentFile);
            }*/
            currentReader = new BufferedReader(new InputStreamReader(inputStream));
        } catch (IOException e) {
            LOGGER.error(e);
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (currentFile == null) {
            return false;
        }

        if (currentFile.getPos() > fileSplit.getStart() + fileSplit.getLength()) {
            close();
            return false;
        }


        currentRevision = parser.readNextRevision(currentReader);
        if (currentRevision == null) {
            close();
            return false;
        }

        long id = currentRevision.getId();

        currentId.set(id);
        return true;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException,
            InterruptedException {
        return currentId;
    }

    @Override
    public WikiRevisionWritable getCurrentValue() throws IOException,
            InterruptedException {
        return currentRevision;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return (float) (currentFile.getPos() - fileSplit.getStart()) /
                (float) fileSplit.getLength();
    }

    @Override
    public void close() throws IOException {
        currentFile.close();
    }
}
