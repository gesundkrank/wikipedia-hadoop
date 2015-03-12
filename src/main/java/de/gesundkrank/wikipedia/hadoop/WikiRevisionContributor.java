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

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Jan Graßegger<jan@anycook.de>
 */
public class WikiRevisionContributor implements WritableComparable<WikiRevisionContributor> {

        private String username;
        private long id;

        public WikiRevisionContributor() {
            this("", -1);
        }

        public WikiRevisionContributor(String username, long id) {
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
        public int compareTo(WikiRevisionContributor o) {
            return Long.valueOf(id).compareTo(o.id);
        }

        @Override
        public String toString() {
            return String.format("username:%s\nid:%d", username, id);
        }
}
