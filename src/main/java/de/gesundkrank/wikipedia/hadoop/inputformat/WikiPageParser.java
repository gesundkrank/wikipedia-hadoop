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
import org.apache.commons.lang3.StringEscapeUtils;

import javax.xml.bind.DatatypeConverter;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Jan Graßegger<jan.grassegger@uni-weimar.de>
 */
public class WikiPageParser {
    public static final String PAGESTART = "<page>";

    private static final char NEWLINE = '\n';

    private static final Pattern titlePattern =
            Pattern.compile(".*<title>(.+)</title>.*");
    private static final Pattern idPattern =
            Pattern.compile(".*<id>([0-9]+)</id>.*");
    private static final Pattern redirectPattern =
            Pattern.compile(".*<redirect.*/>.*");
    private static final Pattern revisionPattern =
            Pattern.compile(".*<revision>.*");
    private static final Pattern revisionEndPattern =
            Pattern.compile(".*</revision>.*");
    private static final Pattern timestampPattern =
            Pattern.compile(".*<timestamp>(.+)</timestamp>");
    private static final Pattern contributorPattern =
            Pattern.compile(".*<contributor>.*");
    private static final Pattern usernamePattern =
            Pattern.compile(".*<username>(.+)</username>.*");
    private static final Pattern commentPattern =
            Pattern.compile(".*<comment>(.+)</comment>.*");
    private static final Pattern textBeginPattern =
            Pattern.compile(".*<text xml:space=\"preserve\">(.+)");
    private static final Pattern textEndPattern =
            Pattern.compile("(.+)</text>.*");
    private static final Pattern isMinorPattern =
            Pattern.compile(".*<minor />.*");
    private static final Pattern pageEndPattern =
            Pattern.compile(".*</page>.*");
    private static final Pattern contributorEndPattern =
            Pattern.compile(".*</contributor>.*");


    private long numBytesRead = 0;

    public WikiPageParser() {
    }

    public WikiPageWritable readNextPage(BufferedReader in)
            throws IOException {

//		numBytesRead = 0;
        boolean foundTitle = false;
        boolean foundId = false;
        boolean foundRedirect = false;


        String line;

        while ((line = in.readLine()) != null) {
            if (line.trim().startsWith(PAGESTART))
                break;
        }

        if (line == null) return null;

        WikiPageWritable wikiPage = new WikiPageWritable();

        while ((line = in.readLine()) != null) {

            //title
            if (!foundTitle) {
                String title = matchTitle(line);
                if (title != null) {
                    wikiPage.setTitle(title);
                    foundTitle = true;
                    continue;
                }

            }

            //id
            if (!foundId) {
                long id = matchId(line);
                if (id != -1) {
                    wikiPage.setId(id);
                    foundId = true;
                    continue;
                }
            }

            //redirect
            if (!foundRedirect && matchRedirect(line)) {
                wikiPage.setRedirect(true);
                foundRedirect = true;
                continue;
            }

            //revision
            if (matchRevision(line)) {
                WikiPageWritable.WikiPageRevision revision = wikiPage.newRevision();
                readNextRevision(revision, in);
                continue;


            }

            Matcher pageEndMatcher = pageEndPattern.matcher(line);
            if (pageEndMatcher.matches())
                break;


        }

        return wikiPage;
    }

    public WikiPageWritable.WikiPageRevision readNextRevision(BufferedReader in) throws IOException {
        WikiPageWritable.WikiPageRevision revision = new WikiPageWritable.WikiPageRevision();
        readNextRevision(revision, in);
        return revision;
    }

    public void readNextRevision(WikiPageWritable.WikiPageRevision revision, BufferedReader in) throws IOException{

        String line;
        boolean foundRevisionId = false;
        boolean foundContributor = false;
        boolean foundComment = false;
        boolean foundText = false;
        boolean foundTimestamp = false;
        boolean foundMinor = false;

        while ((line = in.readLine()) != null) {

            //revisionid
            if (!foundRevisionId) {
                long id = matchId(line);
                if (id != -1) {
                    revision.setId(id);
                    foundRevisionId = true;
                    continue;
                }
            }


            //timestamp
            if (!foundTimestamp) {
                long timestamp = matchTimestamp(line);
                if (timestamp != -1) {
                    revision.setTimestamp(timestamp);
                    foundTimestamp = true;
                    continue;
                }
            }


            //contributor
            if (!foundContributor && matchContributor(line)) {
                foundContributor = true;
                boolean foundUsername = false;
                boolean foundContributorId = false;

                WikiPageWritable.WikiPageRevision.WikiPageContributor contributor = revision.getContributor();
                do {

                    //username
                    if (!foundUsername) {
                        String username = matchUsername(line);
                        if (username != null) {
                            contributor.setUsername(username);
                            foundUsername = true;
                            continue;
                        }
                    }


                    //contr_id
                    if (!foundContributorId) {
                        long id = matchId(line);
                        if (id != -1) {
                            contributor.setId(id);
                            foundContributorId = true;
                            continue;
                        }
                    }


                    if (matchContributorEnd(line))
                        break;

                } while ((line = in.readLine()) != null);

                revision.setContributor(contributor);
                continue;
            }

            //comment
            if (!foundComment) {
                String comment = matchComment(line);
                if (comment != null) {
                    revision.setComment(comment);
                    foundComment = true;
                    continue;
                }
            }

            //minor
            if (!foundMinor && matchMinor(line)) {
                revision.setMinor(true);
                continue;
            }

            //text
            if (!foundText) {
                String textLine = matchTextBegin(line);
                if (textLine != null) {
                    line = textLine;
                    foundText = true;
                    StringBuilder text = new StringBuilder();
                    do {
                        Matcher textEMatcher = textEndPattern.matcher(line);
                        if (textEMatcher.matches()) {
                            text.append(textEMatcher.group(1));
                            break;
                        }
                        line = StringEscapeUtils.unescapeXml(line);

                        text.append(line).append(NEWLINE);
                    } while ((line = in.readLine()) != null);
                    revision.setText(text.toString());
                    continue;
                }
            }

            Matcher revisionEndMatcher = revisionEndPattern.matcher(line);
            if (revisionEndMatcher.matches())
                break;


        }
    }

    public static String matchTitle(String line) {
        Matcher titleMatcher = titlePattern.matcher(line);
        if (titleMatcher.matches()) {
            return titleMatcher.group(1);
        }
        return null;
    }

    private static long matchId(String line) {
        Matcher idMatcher = idPattern.matcher(line);
        if (idMatcher.matches()) {
            return Long.parseLong(idMatcher.group(1));
        }
        return -1;
    }

    private static boolean matchRedirect(String line) {
        Matcher redirectMatcher = redirectPattern.matcher(line);
        return redirectMatcher.matches();
    }

    public static boolean matchRevision(String line) {
        Matcher revisionMatcher = revisionPattern.matcher(line);
        return revisionMatcher.matches();
    }

    private static long matchTimestamp(String line) {
        Matcher timestampMatcher = timestampPattern.matcher(line);
        if (timestampMatcher.matches()) {
            String timeString = timestampMatcher.group(1);
            return DatatypeConverter.parseDateTime(timeString).getTimeInMillis();
        }
        return -1;
    }

    private static boolean matchContributor(String line) {
        Matcher contributorMatcher = contributorPattern.matcher(line);
        return contributorMatcher.matches();
    }

    private static String matchUsername(String line) {
        Matcher userNameMatcher = usernamePattern.matcher(line);
        if (userNameMatcher.matches()) {
            return userNameMatcher.group(1);
        }
        return null;
    }

    private static boolean matchContributorEnd(String line) {
        Matcher contributorEndMatcher = contributorEndPattern.matcher(line);
        return contributorEndMatcher.matches();
    }

    private static String matchComment(String line) {
        Matcher commentMatcher = commentPattern.matcher(line);
        if (commentMatcher.matches()) {
            return commentMatcher.group(1);
        }
        return null;
    }

    private static String matchTextBegin(String line) {
        Matcher textBeginMatcher = textBeginPattern.matcher(line);
        if (textBeginMatcher.matches()) {
            return textBeginMatcher.group(1);
        }
        return null;
    }

    private static boolean matchMinor(String line) {
        Matcher isMinorMatcher = isMinorPattern.matcher(line);
        return isMinorMatcher.matches();
    }


    public long getCurrentNumBytesRead() {
        return numBytesRead;
    }

    public static void main(String[] args) throws IOException {
        FileInputStream wikipedia = new FileInputStream("/home/moji8208/wikipedia/de/dewiki-latest-pages-articles.1370244398000.xml");
        DataInputStream stream = new DataInputStream(wikipedia);

        WikiPageParser parser = new WikiPageParser();

        WikiPageWritable page;
        do{

            page = parser.readNextPage(new BufferedReader(new InputStreamReader(stream)));


            System.out.println(page.getTitle()+" "+page.getReversedURL("de"));


        }while (page!=null);
    }
}
