/*
 * This file is part of wikipedia-hadoop. The new internet cookbook
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

package de.gesundkrank.wikipedia.hadoop;

import de.gesundkrank.wikipedia.hadoop.converter.Converter;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Jan Graßegger<jan@anycook.de>
 */
public class WikiRevisionWritable implements WritableComparable<WikiRevisionWritable> {

    private long id;
    private long timestamp;
    private WikiPageWritable page;
    private WikiRevisionContributor contributor;
    private String comment;
    private String text;
    private boolean isMinor = false;

    public WikiRevisionWritable() {
        this(null);
    }

    public WikiRevisionWritable(WikiPageWritable page) {
        this.page = page;
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

    public WikiPageWritable getPage() {
        return page;
    }

    public void setPage(WikiPageWritable page) {
        this.page = page;
    }

    public WikiRevisionContributor getContributor() {
        return contributor;
    }

    public void setContributor(WikiRevisionContributor contributor) {
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
        page.write(out);

        if (contributor != null) {
            out.writeBoolean(true);
            contributor.write(out);
        } else {
            out.writeBoolean(false);
        }

        out.writeUTF(comment != null ? comment : "");
        out.writeBoolean(isMinor);

        // workaround for "java.io.UTFDataFormatException: encoded string too long"
        if (text != null) {
            out.writeBoolean(true);
            byte[] b = text.getBytes("utf-8");
            out.writeInt(b.length);
            out.write(b);
        } else {
            out.writeBoolean(false);
        }
    }


    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readLong();
        timestamp = in.readLong();
        page = new WikiPageWritable();
        page.readFields(in);


        if (in.readBoolean()) {
            contributor = new WikiRevisionContributor();
            contributor.readFields(in);
        } else {
            contributor = null;
        }
        comment = in.readUTF();
        isMinor = in.readBoolean();



        int textLength = in.readInt();

        if (in.readBoolean()) {
            byte[] b = new byte[textLength];
            in.readFully(b);
            text = new String(b, "utf-8");
        } else {
            text = null;
        }
    }

    @Override
    public String toString() {
        return String.format("id:%d%n timestamp:%d%n page: %s %n contributor:%s %n comment:%s %n isMinor:%s %n text:%s",
                id, timestamp, page, contributor, comment, isMinor, text);
    }


    @Override
    public int compareTo(WikiRevisionWritable o) {
        return Long.valueOf(id).compareTo(o.id);
    }



}
