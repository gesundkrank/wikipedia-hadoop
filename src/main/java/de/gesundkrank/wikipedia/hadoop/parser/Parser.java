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

package de.gesundkrank.wikipedia.hadoop.parser;


import de.gesundkrank.wikipedia.hadoop.WikiPageWritable;
import de.gesundkrank.wikipedia.hadoop.WikiRevisionContributor;
import de.gesundkrank.wikipedia.hadoop.WikiRevisionWritable;
import org.apache.commons.lang3.StringEscapeUtils;

import javax.xml.bind.DatatypeConverter;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Jan Graßegger<jan.grassegger@uni-weimar.de>
 */
public class Parser {
    public static final String PAGE_START = "<page>";
    private static final char NEWLINE = '\n';

    private static final Pattern TITLE_PATTERN = Pattern.compile(".*<title>(.+)</title>.*"),
            ID_PATTERN = Pattern.compile(".*<id>([0-9]+)</id>.*"),
            REDIRECT_PATTERN = Pattern.compile(".*<redirect.*/>.*"),
            REVISION_PATTERN = Pattern.compile(".*<revision>.*"),
            REVISION_END_PATTERN = Pattern.compile(".*</revision>.*"),
            TIMESTAMP_PATTERN = Pattern.compile(".*<timestamp>(.+)</timestamp>"),
            CONTRIBUTOR_PATTERN = Pattern.compile(".*<contributor>.*"),
            USERNAME_PATTERN = Pattern.compile(".*<username>(.+)</username>.*"),
            COMMENT_PATTERN = Pattern.compile(".*<comment>(.+)</comment>.*"),
            TEXT_BEGIN_PATTERN = Pattern.compile(".*<text xml:space=\"preserve\">(.+)"),
            TEXT_END_PATTERN = Pattern.compile("(.+)</text>.*"),
            IS_MINOR_PATTERN = Pattern.compile(".*<minor />.*"),
            PAGE_END_PATTERN = Pattern.compile(".*</page>.*"),
            CONTRIBUTOR_END_PATTERN = Pattern.compile(".*</contributor>.*");


    private WikiRevisionWritable revision;
    private WikiPageWritable currentPage;
    private boolean foundRevisionId,
            foundContributor,
            foundComment,
            foundText,
            foundTimestamp,
            foundMinor,
            foundPageId,
            foundPageTitle,
            foundPageRedirect;

    public Parser() {
        currentPage = null;
    }

    private void resetMarkers() {
        foundRevisionId = false;
        foundContributor = false;
        foundComment = false;
        foundText = false;
        foundTimestamp = false;
        foundMinor = false;
    }

    private void resetPageMarkers() {
        foundPageId = false;
        foundPageTitle = false;
        foundPageRedirect = false;
    }

    public WikiRevisionWritable readNextRevision(BufferedReader in) throws IOException {
        resetMarkers();

        revision = new WikiRevisionWritable(currentPage);

        while (in.ready()) {
            String line = in.readLine();

            boolean isPageStart = line.trim().startsWith(PAGE_START);

            if (currentPage == null && !isPageStart) {
                continue;
            }

            if (isPageStart) {
                readNextPage(in);
                revision.setPage(currentPage);
            }

            if (readRevisionId(line) || readTimeStamp(line) || readContributor(line, in) ||
                    readComment(line) || readMinor(line) || readText(line, in)) {
                continue;
            }

            Matcher revisionEndMatcher = REVISION_END_PATTERN.matcher(line);
            if (revisionEndMatcher.matches()) {
                break;
            }
        }

        return revision;
    }

    private boolean readRevisionId(String line) {
        if (!foundRevisionId) {
            long id = matchId(line);
            if (id != -1) {
                revision.setId(id);
                foundRevisionId = true;
                return true;
            }
        }

        return false;
    }

    private boolean readTimeStamp(String line) {
        if (!foundTimestamp) {
            long timestamp = matchTimestamp(line);
            if (timestamp != -1) {
                revision.setTimestamp(timestamp);
                foundTimestamp = true;
                return true;
            }
        }
        return false;
    }

    private boolean readContributor(String line, BufferedReader in) throws IOException {
        if (!foundContributor && matchContributor(line)) {
            foundContributor = true;

            WikiRevisionContributor contributor = new WikiRevisionContributor();

            boolean foundUsername = false;
            boolean foundContributorId = false;

            while (in.ready()) {
                line = in.readLine();

                if (!foundUsername) {
                    String username = matchUsername(line);
                    if (username != null) {
                        contributor.setUsername(username);
                        foundUsername = true;
                        continue;
                    }
                }

                if (!foundContributorId) {
                    long id = matchId(line);
                    if (id != -1) {
                        contributor.setId(id);
                        foundContributorId = true;
                        continue;
                    }
                }

                if (matchContributorEnd(line)) {
                    break;
                }
            }
            revision.setContributor(contributor);
            return true;
        }
        return false;
    }

    private boolean readComment(String line) {
        if (!foundComment) {
            String comment = matchComment(line);
            if (comment != null) {
                revision.setComment(comment);
                foundComment = true;
                return true;
            }
        }
        return false;
    }

    private boolean readMinor(String line) {
        if (!foundMinor && matchMinor(line)) {
            revision.setMinor(true);
            return true;
        }
        return false;
    }

    private boolean readText(String line, BufferedReader in) throws IOException {
        if (!foundText) {
            line = matchTextBegin(line);
            if (line != null) {
                StringBuilder text = new StringBuilder();
                do {
                    Matcher textEMatcher = TEXT_END_PATTERN.matcher(line);
                    if (textEMatcher.matches()) {
                        text.append(textEMatcher.group(1));
                        break;
                    }
                    line = StringEscapeUtils.unescapeXml(line);

                    text.append(line).append(NEWLINE);
                    line = in.readLine();
                } while (line != null);

                foundText = true;
                revision.setText(text.toString());
                return true;
            }
        }

        return false;
    }

    private void readNextPage(BufferedReader in) throws IOException {
        resetPageMarkers();

        currentPage = new WikiPageWritable();

        while (in.ready()) {

            String line = in.readLine();

            if (readPageTitle(line) || readPageId(line) || readPageRedirect(line)) {
                continue;
            }

            //revision
            if (matchRevision(line)) {
                break;
            }

            Matcher pageEndMatcher = PAGE_END_PATTERN.matcher(line);
            if (pageEndMatcher.matches()) {
                break;
            }
        }
    }

    private boolean readPageTitle(String line) {
        if (!foundPageTitle) {
            String title = matchTitle(line);
            if (title != null) {
                currentPage.setTitle(title);
                foundPageTitle = true;
                return true;
            }
        }

        return false;
    }

    private boolean readPageId(String line) {
        if (!foundPageId) {
            long id = matchId(line);
            if (id != -1) {
                currentPage.setId(id);
                foundPageId = true;
                return true;
            }
        }
        return false;
    }

    private boolean readPageRedirect(String line) {
        if (!foundPageRedirect && matchRedirect(line)) {
            currentPage.setRedirect(true);
            foundPageRedirect = true;
            return true;
        }

        return false;
    }

    private static String matchTitle(String line) {
        Matcher titleMatcher = TITLE_PATTERN.matcher(line);
        if (titleMatcher.matches()) {
            return titleMatcher.group(1);
        }
        return null;
    }

    private static long matchId(String line) {
        Matcher idMatcher = ID_PATTERN.matcher(line);
        if (idMatcher.matches()) {
            return Long.parseLong(idMatcher.group(1));
        }
        return -1;
    }

    private static boolean matchRedirect(String line) {
        Matcher redirectMatcher = REDIRECT_PATTERN.matcher(line);
        return redirectMatcher.matches();
    }

    public static boolean matchRevision(String line) {
        Matcher revisionMatcher = REVISION_PATTERN.matcher(line);
        return revisionMatcher.matches();
    }

    private static long matchTimestamp(String line) {
        Matcher timestampMatcher = TIMESTAMP_PATTERN.matcher(line);
        if (timestampMatcher.matches()) {
            String timeString = timestampMatcher.group(1);
            return DatatypeConverter.parseDateTime(timeString).getTimeInMillis();
        }
        return -1;
    }

    private static boolean matchContributor(String line) {
        Matcher contributorMatcher = CONTRIBUTOR_PATTERN.matcher(line);
        return contributorMatcher.matches();
    }

    private static String matchUsername(String line) {
        Matcher userNameMatcher = USERNAME_PATTERN.matcher(line);
        if (userNameMatcher.matches()) {
            return userNameMatcher.group(1);
        }
        return null;
    }

    private static boolean matchContributorEnd(String line) {
        Matcher contributorEndMatcher = CONTRIBUTOR_END_PATTERN.matcher(line);
        return contributorEndMatcher.matches();
    }

    private static String matchComment(String line) {
        Matcher commentMatcher = COMMENT_PATTERN.matcher(line);
        if (commentMatcher.matches()) {
            return commentMatcher.group(1);
        }
        return null;
    }

    private static String matchTextBegin(String line) {
        Matcher textBeginMatcher = TEXT_BEGIN_PATTERN.matcher(line);
        if (textBeginMatcher.matches()) {
            return textBeginMatcher.group(1);
        }
        return null;
    }

    private static boolean matchMinor(String line) {
        Matcher isMinorMatcher = IS_MINOR_PATTERN.matcher(line);
        return isMinorMatcher.matches();
    }
}
