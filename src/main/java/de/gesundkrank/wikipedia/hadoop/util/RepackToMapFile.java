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
import de.gesundkrank.wikipedia.hadoop.io.WikiDumpLoader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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
    private static final Logger LOGGER = Logger.getLogger(RepackToMapFile.class);

    @Override
    public int run(String[] args) throws Exception {

        CommandLineParser parser = new GnuParser();
        Options options = getOptions();

        try {
            CommandLine commandLine = parser.parse(options, args);

            if (commandLine.hasOption('h')) {
                printHelp(options);
                return 0;
            }

            String basePath = commandLine.getOptionValue('b');
            String outputPath = commandLine.getOptionValue('o');
            boolean checkNew = commandLine.hasOption('c');

            return run(basePath, outputPath, checkNew);

        } catch (ParseException e) {
            System.err.printf("Parsing failed.  Reason: %s%n", e.getMessage());
            printHelp(options);
            return 1;
        }
    }

    public int run(String basePath, String outputPath, boolean checkNew) throws Exception {
        Configuration configuration = getConf();

        LOGGER.info("Tool name: " + getClass().getSimpleName());

        Job job = new Job(configuration, getClass().getSimpleName());
        job.setJarByClass(getClass());

        job.setMapperClass(WikiMapper.class);
        job.setInputFormatClass(WikiInputFormat.class);
        job.setOutputFormatClass(MapFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(WikiPageWritable.class);


        WikiDumpLoader wikiDumpLoader = new WikiDumpLoader(checkNew);
        wikiDumpLoader.addWikiDump(job, basePath);

        MapFileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setNumReduceTasks(1);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private static Options getOptions() {


        Options options = new Options();

        options.addOption("h", "help", false, "Show this message.");

        Option basePath = new Option("b", "basePath", true, "The path where your Wikipedia dumps get stored. " +
                "Creates subPaths.");
        basePath.setRequired(true);
        options.addOption(basePath);

        Option outputPath = new Option("o", "outputPath", true, "Path where the MapFile is stored.");
        outputPath.setRequired(true);
        options.addOption(outputPath);

        options.addOption("c", "checkNew", false, "Checks for new Wikipedia online.");

        return options;
    }

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("hadoop jar <jar>", options);
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new RepackToMapFile(), args);
    }

    public static class WikiMapper extends Mapper<Text, WikiPageWritable, Text, WikiPageWritable> {
        @Override
        protected void map(Text key, WikiPageWritable value, Context context) throws IOException, InterruptedException {
            if (value.isRedirect()) {
                return;
            }

            context.write(key, value);
        }
    }
}
