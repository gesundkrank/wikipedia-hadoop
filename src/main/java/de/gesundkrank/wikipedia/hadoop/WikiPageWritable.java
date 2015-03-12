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

package de.gesundkrank.wikipedia.hadoop;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Locale;

/**
 * @author Jan Graßegger<jan.grassegger@uni-weimar.de>
 */
public class WikiPageWritable implements WritableComparable<WikiPageWritable> {

    private String title;
    private long id;
    private boolean isRedirect;

    public WikiPageWritable() {
        this(null, -1, false);
    }

    public WikiPageWritable(String title, long id, boolean isRedirect) {
        this.title = title;
        this.id = id;
        this.isRedirect = isRedirect;
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
    }


    @Override
    public void readFields(DataInput in) throws IOException {
        title = in.readUTF();
        id = in.readLong();
        isRedirect = in.readBoolean();
    }

    @Override
    public int compareTo(WikiPageWritable o) {
        return Long.valueOf(id).compareTo(o.id);
    }

    @Override
    public String toString() {
        return String.format("title:%s\nid:%d\nisRedirect:%s", title, id, isRedirect);
    }
}
