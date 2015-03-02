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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author Jan Graßegger<jan.grassegger@uni-weimar.de>
 */
public class MapFileReader implements AutoCloseable{
    private static MapFileReader INSTANCE;

    private final Logger logger;
    private Configuration conf;
    private MapFile.Reader reader;
    private String nameNode;
    private Path path = new Path("wikipedia-mapfile");

    public static MapFileReader getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new MapFileReader();
        }

        return INSTANCE;
    }

    private MapFileReader() {
        logger = Logger.getLogger(getClass());
        logger.debug("init MapFileRecordReader");

        conf = new Configuration();

        if (nameNode != null) {
            conf.set("fs.default.name", nameNode);
        }

        try {
            reader = new MapFile.Reader(path, conf);
        } catch (IOException e) {
            logger.error(e);
        }
    }

    private void initMapFileReader() throws IOException {
        reader = new MapFile.Reader(path, conf);
    }

    public String getNameNode() {
        return nameNode;
    }

    public void setNameNode(String nameNode) throws IOException {
        this.nameNode = nameNode;
        conf.set("fs.default.name", nameNode);
        reader = null;
    }

    public Path getPath() {
        return path;
    }

    public void setPath(String path) throws IOException {
        this.path = new Path(path);
        reader = null;
    }

    public WikiPageWritable read(Text title) throws IOException, ArticleNotFoundException {
        if (reader == null) {
            initMapFileReader();
        }

        WikiPageWritable page = new WikiPageWritable();
        page = (WikiPageWritable)reader.get(title, page);
        if(page == null) {
            throw new ArticleNotFoundException(title.toString());
        }
        return page;
    }

    public WikiPageWritable read(String title) throws IOException, ArticleNotFoundException {
        return read(new Text(title));
    }

    public void close() {
        logger.debug("closing MapFileRecordReader");
        try {
            if (reader != null) {
                reader.close();
            }
        } catch (IOException e) {
            // nothing to do
        }
    }

    public static class ArticleNotFoundException extends Exception {
        public ArticleNotFoundException(String title) {
            super(String.format("Article with title %s does not exist", title));
        }
    }


}
