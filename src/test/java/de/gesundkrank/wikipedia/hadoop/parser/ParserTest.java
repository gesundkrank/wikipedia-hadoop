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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.xml.bind.DatatypeConverter;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Locale;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

/**
 * @author Jan Graßegger<jan@anycook.de>
 */
public class ParserTest {

    private Parser parser;
    private BufferedReader in;
    private WikiRevisionWritable currentRevision;

    @BeforeClass
    public void setUp() {
        parser = new Parser();
        Reader reader = new InputStreamReader(getClass().getResourceAsStream("/wikidump_example.xml"));
        in = new BufferedReader(reader);
    }

    @Test
    public void readFirstRevision() throws IOException {
        currentRevision = parser.readNextRevision(in);
        assertNotNull(currentRevision);
    }

    @Test(dependsOnMethods = "readFirstRevision")
    public void readSecondRevision() throws IOException {
        readFirstRevision();
    }

    @Test(dependsOnMethods = "readFirstRevision")
    public void firstRevisionHasFields() {
        // check id
        assertEquals(631144794, currentRevision.getId());

        // check timestamp
        long expectedDate = DatatypeConverter.parseDateTime("2014-10-26T04:50:23Z").getTimeInMillis();
        assertEquals(expectedDate, currentRevision.getTimestamp());

        // check comment
        String expectedComment = "add [[WP:RCAT|rcat]]s";
        assertEquals(expectedComment, currentRevision.getComment());
    }

    @Test(dependsOnMethods = "readFirstRevision")
    public void firstRevisionTextTest() {
        assertNotNull(currentRevision.getText());
        String expectedText = "#REDIRECT [[Computer accessibility]]\n\n{{Redr|move|from CamelCase|up}}";
        assertEquals(expectedText, currentRevision.getText());
    }

    @Test(dependsOnMethods = "readFirstRevision")
    public void firstRevisionPlainTextTest() {
        String plainText = currentRevision.getPlainText(Locale.ENGLISH);
        assertNotNull(plainText);

        assertEquals("#REDIRECT Computer accessibility", plainText);
    }

    @Test(dependsOnMethods = "readFirstRevision")
    public void firstRevisionContributorTest() {
        WikiRevisionContributor contributor = currentRevision.getContributor();
        assertNotNull(contributor);

        assertEquals("Paine Ellsworth", contributor.getUsername());
        assertEquals(9092818, contributor.getId());
    }

    @Test(dependsOnMethods = "readFirstRevision")
    public void firstRevisionPageTest() {
        WikiPageWritable page = currentRevision.getPage();
        assertNotNull(page);

        assertEquals("AccessibleComputing", page.getTitle());
        assertEquals(10, page.getId());
        assertEquals(true, page.isRedirect());
    }

    @Test(dependsOnMethods = "readSecondRevision")
    public void secondRevisionHasFields() throws IOException {
        // check id
        assertEquals(645849603, currentRevision.getId());

        // check timestamp
        long expectedDate = DatatypeConverter.parseDateTime("2015-02-06T04:18:52Z").getTimeInMillis();
        assertEquals(expectedDate, currentRevision.getTimestamp());

        // check comment
        String expectedComment = "General fixes - Bracket fixes using [[Project:AWB|AWB]]";
        assertEquals(expectedComment, currentRevision.getComment());
    }

    @Test(dependsOnMethods = "readSecondRevision")
    public void secondRevisionTextTest() {
        assertNotNull(currentRevision.getText());

        String expectedStart = "{{Redirect2|Anarchist|Anarchists|the fictional character|Anarchist (comics)" +
                "|other uses|Anarchists (disambiguation)}}";
        assertTrue(currentRevision.getText().startsWith(expectedStart));

        String expectedEnd = "[[Category:Far-left politics]]";
        assertTrue(currentRevision.getText().endsWith(expectedEnd));

        String phraseToFind = "Modern anarchism sprang from the secular or religious thought of the " +
                "[[Age of Enlightenment|Enlightenment]]";
        assertTrue(currentRevision.getText().contains(phraseToFind));
    }

    @Test(dependsOnMethods = "readSecondRevision")
    public void secondRevisionContributorTest() {
        WikiRevisionContributor contributor = currentRevision.getContributor();
        assertNotNull(contributor);

        assertEquals("ChrisGualtieri", contributor.getUsername());
        assertEquals(16333418, contributor.getId());
    }

    @Test(dependsOnMethods = "readSecondRevision")
    public void secondRevisionPageTest() {
        WikiPageWritable page = currentRevision.getPage();
        assertNotNull(page);

        assertEquals("Anarchism", page.getTitle());
        assertEquals(12, page.getId());
        assertEquals(false, page.isRedirect());
    }

    @Test(dependsOnMethods = "readSecondRevision")
    public void secondRevisionPlainTextTest() {
        String plainText = currentRevision.getPlainText(Locale.ENGLISH);
        assertNotNull(plainText);

        assertTrue(plainText.startsWith("Anarchism is a collection of movements and ideologies"));
        assertTrue(plainText.endsWith("European Socialism: A History of Ideas and Movements (1959)"));
    }
}
