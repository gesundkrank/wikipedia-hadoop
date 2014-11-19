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
import de.gesundkrank.wikipedia.hadoop.converter.Converter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author Jan Graßegger<jan.grassegger@uni-weimar.de>
 */
public enum MapFileReader {
    INSTANCE;

    public static final String namenode = "hdfs://webis70.medien.uni-weimar.de:8020";
    private Configuration conf;
    private FileSystem fs;
    private Reader reader;
    private final String path = "/user/moji8208/wikipedia-mapfile";
    private final Logger logger;

    //	private Random random = new Random();
    private class Reader {
        private MapFile.Reader reader = null;
        private Logger logger = Logger.getLogger(getClass());

        public Reader() {
            try {
                reader = new MapFile.Reader(fs, path, conf);
            } catch (IOException e) {
                logger.error("Can't initialize MapFile.Reader.", e);
            }
        }

        public WikiPageWritable read(Text title) throws IOException, ArticleNotFoundException {
            WikiPageWritable page = new WikiPageWritable();
            reader.get(title, page);
            if(page == null) throw new ArticleNotFoundException(title.toString());
            return page;
        }

        private void close() throws IOException {
            reader.close();
        }


    }

    private MapFileReader() {
        logger = Logger.getLogger(getClass());
        logger.debug("init MapFileRecordReader");

        conf = new Configuration();
        conf.set("fs.default.name", namenode);
        try {
            fs = FileSystem.get(conf);
            reader = new Reader();
        } catch (IOException e) {
            logger.error(e);
        }

    }

    public static WikiPageWritable read(Text title) throws IOException, ArticleNotFoundException {
        return INSTANCE.reader.read(title);
    }

    public static WikiPageWritable read(String title) throws IOException, ArticleNotFoundException {
        return read(new Text(title));
    }

    public static  void close() {
        INSTANCE.logger.debug("closing MapFileRecordReader");
        try {
            INSTANCE.reader.close();
        } catch (IOException e) {
            // nothing to do
        }
    }

    public static void main(String[] args) throws IOException, ArticleNotFoundException, Converter.ConverterException {
        System.out.println(read("1969 Alpine Skiing World Cup – Women's Slalom").getRevisions().getFirst().getHTML());
    }

    public static class ArticleNotFoundException extends Exception {
        public ArticleNotFoundException(String title) {
            super(String.format("Article with title %s does not exist", title));
        }
    }
}
