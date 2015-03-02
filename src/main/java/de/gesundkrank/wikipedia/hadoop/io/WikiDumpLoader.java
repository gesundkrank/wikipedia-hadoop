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

package de.gesundkrank.wikipedia.hadoop.io;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

public class WikiDumpLoader {
    private final static String dumpUrl =
            "http://dumps.wikimedia.org/%swiki/latest/%swiki-latest-pages-articles.xml.bz2";

    private final boolean checkNew;
    private final Logger logger;

    /**
     * Default constructor uses {@link WikiDumpLoader#WikiDumpLoader(boolean)} with checkNew
     * set true
     */
    public WikiDumpLoader() {
        this(true);
    }

    /**
     * Constructor
     *
     * @param checkNew if false its not checked if a new version exists and
     *                 no new dump is downloaded
     */
    public WikiDumpLoader(boolean checkNew) {
        this.checkNew = checkNew;
        this.logger = Logger.getLogger(getClass());
    }

    /**
     * Adds input path of latest english wikipedia dump
     *
     * @param job
     * @param basePathStr
     * @throws java.io.IOException
     */
    public void addWikiDump(Job job, String basePathStr) throws IOException {
        addWikiDump(job, basePathStr, Locale.ENGLISH);
    }

    /**
     * Adds inputpath to a given hadoop job
     *
     * @param job         hadoop job
     * @param basePathStr
     * @param locale      Language of the wikidump
     * @throws java.io.IOException
     */
    public void addWikiDump(Job job, String basePathStr, Locale locale) throws IOException {
        Path basePath = new Path(basePathStr, locale.getLanguage());
        FileSystem fs = FileSystem.get(job.getConfiguration());
        FileStatus latestLocalDumpStatus = checkLocalDumps(fs, basePath);
        Path latestDump = null;
        if (latestLocalDumpStatus != null) {
            latestDump = latestLocalDumpStatus.getPath();
        } else {
            logger.info("Could not find a valid dump. Loading new version.");
        }


        if (checkNew || latestDump == null) {
            long latestDumpTime = checkNewDump(locale);
            if (latestLocalDumpStatus == null || latestDumpTime > latestLocalDumpStatus.getModificationTime()) {
                latestDump = loadNewDump(fs, basePath, latestDumpTime, locale);
            } else {
                throw new IOException("failed to get latest dump");
            }
        }

        FileInputFormat.addInputPath(job, latestDump);
    }

    /**
     * Return last change of the latest online dump of the given language
     *
     * @param locale Language of wikidump
     * @return
     */
    private long checkNewDump(Locale locale) throws IOException {
        try {
            String localeDumpUrl = String.format(dumpUrl, locale.getLanguage(), locale.getLanguage());
            URLConnection connection = new URL(localeDumpUrl).openConnection();
            String lastModified = connection.getHeaderField("Last-Modified");
            return new SimpleDateFormat("E, d MMM yyyy HH:mm:ss Z", Locale.ENGLISH)
                    .parse(lastModified).getTime();
        } catch (ParseException e) {
            throw new IOException(e);
        }
    }

    /**
     * Returns {@link FileStatus} of the latest dump in the HDFS
     *
     * @param fs       HDFS
     * @param basepath Base path of hdfs wikidumps
     * @return
     */
    private FileStatus checkLocalDumps(FileSystem fs, Path basepath) {

        long lastLocalChange = 0;
        FileStatus lastLocalDump = null;
        try {
            if (!fs.exists(basepath)) {
                fs.mkdirs(basepath);
                return null;
            }

            FileStatus[] stati = fs.listStatus(basepath);

            for (FileStatus status : stati) {
                long fileChange = status.getModificationTime();
                if (fileChange > lastLocalChange)
                    lastLocalDump = status;
            }
        } catch (IOException e) {
            logger.error(e);
        }
        return lastLocalDump;

    }

    /**
     * Loads new dump and unpack it into hdfs
     *
     * @param fs  HDFS
     * @param basePath
     * @param time
     * @return
     * @throws java.io.IOException
     */
    private Path loadNewDump(FileSystem fs, Path basePath, long time, Locale locale) throws IOException {
        System.out.println("loading new dump");
        String localeDumpUrl = String.format(dumpUrl, locale.getLanguage(), locale.getLanguage());
        URLConnection connection = new URL(localeDumpUrl).openConnection();

        try (BZip2CompressorInputStream bzIn = new BZip2CompressorInputStream(connection.getInputStream())) {
            String fileName = String.format("%swiki-latest-pages-articles.%d.xml", locale.getLanguage(), time);
            Path path = new Path(basePath, fileName);
            FSDataOutputStream outputStream = fs.create(path);

            final byte[] buffer = new byte[1024];
            int n;
            while (-1 != (n = bzIn.read(buffer))) {
                outputStream.write(buffer, 0, n);
            }

            return path;
        }
    }

}
