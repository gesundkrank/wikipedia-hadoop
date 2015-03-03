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

package de.gesundkrank.wikipedia.hadoop.converter;

import de.fau.cs.osr.ptk.common.VisitingException;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sweble.wikitext.engine.CompiledPage;
import org.sweble.wikitext.engine.Compiler;
import org.sweble.wikitext.engine.CompilerException;
import org.sweble.wikitext.engine.PageId;
import org.sweble.wikitext.engine.PageTitle;
import org.sweble.wikitext.engine.utils.HtmlPrinter;
import org.sweble.wikitext.engine.utils.SimpleWikiConfiguration;
import org.sweble.wikitext.lazy.LinkTargetException;

import javax.xml.bind.JAXBException;
import java.io.FileNotFoundException;
import java.io.StringWriter;

/**
 * @author Jan Grassegger<jan.grassegger@uni-weimar.de>
 */
public enum Converter {
    INSTANCE;

    private final Logger logger;
    private final SimpleWikiConfiguration config;
    private final Compiler compiler;
    private final Visitor visitor;

    private Converter() {
        this.logger = LogManager.getLogger(getClass());
        try {
            this.config = new SimpleWikiConfiguration(
                    "classpath:/org/sweble/wikitext/engine/SimpleWikiConfiguration.xml");
        } catch (FileNotFoundException | JAXBException e) {
            throw new Error("failed to initialise wiki text converter", e);
        }

        final int wrapCol = 80;

        this.compiler = new org.sweble.wikitext.engine.Compiler(config);
        this.visitor = new Visitor(config, wrapCol);
    }

    public String convertToPlainText(String wikiText) throws ConverterException {
        return convertToPlainText(wikiText, "TITLE_UNKNOWN", -1);
    }

    public String convertToPlainText(String wikiText, String title, long revisionId) throws ConverterException {
        return convert(wikiText, title, revisionId, false);
    }

    public String convertToHTML(String wikiText, String title, long revisionId) throws ConverterException {
        return convert(wikiText, title, revisionId, true);
    }

    public String convert(String wikiText, String title, long revisionId, boolean renderHtml)
            throws ConverterException {
        try {
            wikiText = StringEscapeUtils.unescapeHtml4(wikiText);
        } catch (IllegalArgumentException e) {
            // ignore result if unescaping fails
            logger.debug("unescaping wikiText failed");
        }

        try {
            PageTitle pageTitle = PageTitle.make(config, title);
            PageId pageId = new PageId(pageTitle, revisionId);
            CompiledPage cp = compiler.postprocess(pageId, wikiText, null);

            if (renderHtml) {
                StringWriter writer = new StringWriter();
                HtmlPrinter p = new HtmlPrinter(writer, pageTitle.getFullTitle());
                p.setCssResource("/org/sweble/wikitext/engine/utils/HtmlPrinter.css", "");
                p.setStandaloneHtml(true, "");
                p.go(cp.getPage());
                return writer.toString();
            } else {
                return (String) visitor.go(cp.getPage());
            }

        } catch (LinkTargetException | CompilerException | VisitingException e) {
            logger.error(e, e);
            throw new ConverterException(e);
        }
    }

    public static class ConverterException extends Exception {
        public ConverterException(Throwable cause) {
            super("failed to parse wiki text to plain text", cause);
        }
    }
}
