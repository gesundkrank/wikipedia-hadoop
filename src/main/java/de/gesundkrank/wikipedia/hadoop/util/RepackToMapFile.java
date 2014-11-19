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

import de.gesundkrank.wikipedia.hadoop.WikiPageWritable;
import de.gesundkrank.wikipedia.hadoop.inputformat.WikiInputFormat;
import de.gesundkrank.wikipedia.hadoop.io.WikidumpLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author Jan Graßegger<jan.grassegger@uni-weimar.de>
 */
public class RepackToMapFile extends Configured implements Tool {
    private final static Logger logger = Logger.getLogger(RepackToMapFile.class);

    public static class WikiMapper extends Mapper<Text, WikiPageWritable, Text, WikiPageWritable> {
        @Override
        protected void map(Text key, WikiPageWritable value, Context context) throws IOException, InterruptedException {
            if(value.isRedirect()) return;
            context.write(key, value);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = getConf();

        logger.info("Tool name: "+getClass().getSimpleName());

        Job job = new Job(configuration, getClass().getSimpleName());
        job.setJarByClass(getClass());

        job.setMapperClass(WikiMapper.class);
        job.setInputFormatClass(WikiInputFormat.class);
        job.setOutputFormatClass(MapFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(WikiPageWritable.class);


        WikidumpLoader wikidumpLoader = new WikidumpLoader(false);
        wikidumpLoader.addWikidump(job, "wikidumps");

        MapFileOutputFormat.setOutputPath(job, new Path("wikipedia-mapfile"));

        job.setNumReduceTasks(1);

        return job.waitForCompletion(true) ? 0 : 1;


    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new RepackToMapFile(), args);
    }
}
