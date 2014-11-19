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

package de.gesundkrank.wikipedia.hadoop;


import de.gesundkrank.wikipedia.hadoop.converter.Converter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Locale;

/**
 * @author Jan Graßegger<jan.grassegger@uni-weimar.de>
 */
public class WikiPageWritable implements WritableComparable<WikiPageWritable> {

    private String title;
    private long id;
    private boolean isRedirect;
    private WikiPageRevisions revisions;


    public WikiPageWritable() {
        this(null, -1, false);
    }

    public WikiPageWritable(String title, long id, boolean isRedirect) {
        this.title = title;
        this.id = id;
        this.isRedirect = isRedirect;
        this.revisions = new WikiPageRevisions();
    }

    public String getTitle() {
        return title;
    }

    public String getNormalizedTitle() {
        return normalizeTitle(title);
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public boolean isRedirect() {
        return isRedirect;
    }

    public void setRedirect(boolean isRedirect) {
        this.isRedirect = isRedirect;
    }

    public String getURL(Locale locale) {
        return getURL(locale.getLanguage());
    }

    public String getReversedURL(Locale locale) {
        return getReversedURL(locale.getLanguage());
    }

    public String getURL(String language) {
        return String.format("http://%s.wikipedia.org/wiki/%s", language, getNormalizedTitle());
    }

    public String getReversedURL(String language) {
        return String.format("org.wikipedia.%s/wiki/%s", language, getNormalizedTitle());
    }

    public WikiPageRevisions getRevisions() {
        return revisions;
    }

    public WikiPageRevision newRevision() {
        WikiPageRevision revision = new WikiPageRevision();
        revisions.add(revision);
        return revision;
    }

    /**
     * normalizes title wikipedia url article part
     */
    public static String normalizeTitle(String title) {
        return title.replaceAll(" ", "_");
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(title);
        out.writeLong(id);
        out.writeBoolean(isRedirect);
        revisions.write(out);
    }


    @Override
    public void readFields(DataInput in) throws IOException {
        title = in.readUTF();
        id = in.readLong();
        isRedirect = in.readBoolean();
        revisions = new WikiPageRevisions();
        revisions.readFields(in);

    }

    @Override
    public int compareTo(WikiPageWritable o) {
        return Long.valueOf(id).compareTo(o.id);
    }

    @Override
    public String toString() {
        return String.format("title:%s\nid:%d\nisRedirect:%s\nrevisions:%s", title, id, isRedirect, revisions);
    }

    public class WikiPageRevisions extends LinkedList<WikiPageRevision> implements Writable {

		private static final long serialVersionUID = 1L;

		@Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(size());

            for(WikiPageRevision revision : this)
                revision.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            int size = dataInput.readInt();
            for(int i = 0; i < size; i++){
                WikiPageRevision revision = new WikiPageRevision();
                revision.readFields(dataInput);
                addLast(revision);
            }
        }
    }


    public static class WikiPageRevision
            implements WritableComparable<WikiPageRevision> {

        private long id;
        private long timestamp;
        private WikiPageContributor contributor;
        private String comment;
        private String text;
        private boolean isMinor;

        public WikiPageRevision() {
            this(-1, -1, false, "", "");
        }

        public WikiPageRevision(long id, long timestamp, boolean isMinor, String comment, String text) {
            this.id = id;
            this.timestamp = timestamp;
            this.contributor = new WikiPageContributor();
            this.comment = comment;
            this.text = text;
            this.isMinor = isMinor;
        }

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public WikiPageContributor getContributor() {
            return contributor;
        }

        public void setContributor(WikiPageContributor contributor) {
            this.contributor = contributor;
        }

        public String getComment() {
            return comment;
        }

        public void setComment(String comment) {
            this.comment = comment;
        }

        public String getText() {
            return text;
        }

        public String getPlainText() throws Converter.ConverterException {
            return Converter.INSTANCE.convertToPlainText(text, "title", id);
        }

        public String getHTML() throws Converter.ConverterException {
            return Converter.INSTANCE.convertToHTML(text, "title", id);
        }

        public void setText(String text) {
            this.text = text;
        }

        public boolean isMinor() {
            return isMinor;
        }

        public void setMinor(boolean isMinor) {
            this.isMinor = isMinor;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(id);
            out.writeLong(timestamp);
            contributor.write(out);
            out.writeUTF(comment != null ? comment : "");
            out.writeBoolean(isMinor);

            // workaround for "java.io.UTFDataFormatException: encoded string too long"
            byte[] b = text.getBytes("utf-8");
            out.writeInt(b.length);
            out.write(b);
        }


        @Override
        public void readFields(DataInput in) throws IOException {
            id = in.readLong();
            timestamp = in.readLong();
            contributor = new WikiPageContributor();
            contributor.readFields(in);
            comment = in.readUTF();
            isMinor = in.readBoolean();

            int textLength = in.readInt();
            byte[] b = new byte[textLength];
            in.readFully(b);
            text = new String(b, "utf-8");
        }

        @Override
        public String toString() {
            return String.format("id:%d\ntimestamp:%d\ncontributor:%s\ncomment:%s\nisMinor:%s\ntext:%s",
                    id, timestamp, contributor, comment, isMinor, text);
        }


        @Override
        public int compareTo(WikiPageRevision o) {
            return Long.valueOf(id).compareTo(o.id);
        }


        public class WikiPageContributor
                implements WritableComparable<WikiPageContributor> {

            private String username;
            private long id;

            public WikiPageContributor() {
                this("", -1);
            }

            public WikiPageContributor(String username, long id) {
                this.username = username;
                this.id = id;
            }

            public String getUsername() {
                return username;
            }

            public long getId() {
                return id;
            }

            public void setUsername(String username) {
                this.username = username;
            }

            public void setId(long id) {
                this.id = id;
            }


            @Override
            public void write(DataOutput out) throws IOException {
                out.writeUTF(username);
                out.writeLong(id);

            }

            @Override
            public void readFields(DataInput in) throws IOException {
                username = in.readUTF();
                id = in.readLong();
            }

            @Override
            public int compareTo(WikiPageContributor o) {
                return Long.valueOf(id).compareTo(o.id);
            }

            @Override
            public String toString() {
                return String.format("username:%s\nid:%d", username, id);
            }
        }
    }
}
