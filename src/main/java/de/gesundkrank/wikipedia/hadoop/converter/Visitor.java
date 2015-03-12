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

package de.gesundkrank.wikipedia.hadoop.converter;

import de.fau.cs.osr.ptk.common.AstVisitor;
import de.fau.cs.osr.ptk.common.ast.AstNode;
import de.fau.cs.osr.ptk.common.ast.NodeList;
import de.fau.cs.osr.ptk.common.ast.Text;
import de.fau.cs.osr.utils.StringUtils;
import org.apache.log4j.Logger;
import org.sweble.wikitext.engine.Page;
import org.sweble.wikitext.engine.PageTitle;
import org.sweble.wikitext.engine.utils.EntityReferences;
import org.sweble.wikitext.engine.utils.SimpleWikiConfiguration;
import org.sweble.wikitext.lazy.LinkTargetException;
import org.sweble.wikitext.lazy.encval.IllegalCodePoint;
import org.sweble.wikitext.lazy.parser.Bold;
import org.sweble.wikitext.lazy.parser.Enumeration;
import org.sweble.wikitext.lazy.parser.EnumerationItem;
import org.sweble.wikitext.lazy.parser.ExternalLink;
import org.sweble.wikitext.lazy.parser.HorizontalRule;
import org.sweble.wikitext.lazy.parser.ImageLink;
import org.sweble.wikitext.lazy.parser.InternalLink;
import org.sweble.wikitext.lazy.parser.Italics;
import org.sweble.wikitext.lazy.parser.Itemization;
import org.sweble.wikitext.lazy.parser.ItemizationItem;
import org.sweble.wikitext.lazy.parser.MagicWord;
import org.sweble.wikitext.lazy.parser.Paragraph;
import org.sweble.wikitext.lazy.parser.Section;
import org.sweble.wikitext.lazy.parser.Table;
import org.sweble.wikitext.lazy.parser.TableCell;
import org.sweble.wikitext.lazy.parser.TableHeader;
import org.sweble.wikitext.lazy.parser.TableRow;
import org.sweble.wikitext.lazy.parser.Url;
import org.sweble.wikitext.lazy.parser.Whitespace;
import org.sweble.wikitext.lazy.parser.XmlElement;
import org.sweble.wikitext.lazy.preprocessor.TagExtension;
import org.sweble.wikitext.lazy.preprocessor.Template;
import org.sweble.wikitext.lazy.preprocessor.TemplateArgument;
import org.sweble.wikitext.lazy.preprocessor.TemplateParameter;
import org.sweble.wikitext.lazy.preprocessor.XmlComment;
import org.sweble.wikitext.lazy.utils.XmlAttribute;
import org.sweble.wikitext.lazy.utils.XmlCharRef;
import org.sweble.wikitext.lazy.utils.XmlEntityRef;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * A visitor to convert an article AST into a pure text representation. To
 * better understand the visitor pattern as implemented by the Visitor class,
 * please take a look at the following resources:
 * <ul>
 * <li>{@see <a href="http://en.wikipedia.org/wiki/Visitor_pattern">http://en.wikipedia.org/wiki/Visitor_pattern</a>}
 * (classic pattern)</li>
 * <li>{@see <a href="http://www.javaworld.com/javaworld/javatips/jw-javatip98.html">
 *     http://www.javaworld.com/javaworld/javatips/jw-javatip98.html</a>}
 * (the version we use here)</li>
 * </ul>
 * <p/>
 * The methods needed to descend into an AST and visit the children of a given
 * node <code>n</code> are
 * <ul>
 * <li><code>dispatch(n)</code> - visit node <code>n</code>,</li>
 * <li><code>iterate(n)</code> - visit the <b>children</b> of node
 * <code>n</code>,</li>
 * <li><code>map(n)</code> - visit the <b>children</b> of node <code>n</code>
 * and gather the return values of the <code>visit()</code> calls in a list,</li>
 * <li><code>mapInPlace(n)</code> - visit the <b>children</b> of node
 * <code>n</code> and replace each child node <code>c</code> with the return
 * value of the call to <code>visit(c)</code>.</li>
 * </ul>
 */
public class Visitor extends AstVisitor {
    private static final Pattern WHITESPACE = Pattern.compile("\\s+");

    private final SimpleWikiConfiguration config;
    private final int wrapCol;
    private final Logger logger;
    private StringBuilder sb;
    private StringBuilder line;
    private boolean pastBod;
    private int needNewlines;
    private boolean needSpace;
    private boolean noWrap;
    private LinkedList<Integer> sections;
    private List<TagExtension> refs;

    // =========================================================================

    public Visitor(SimpleWikiConfiguration config, int wrapCol) {
        this.logger = Logger.getLogger(getClass());
        this.config = config;
        this.wrapCol = wrapCol;
    }

    @Override
    protected boolean before(AstNode node) {
        // This method is called by go() before visitation starts
        sb = new StringBuilder();
        line = new StringBuilder();
        pastBod = false;
        needNewlines = 0;
        needSpace = false;
        noWrap = false;
        sections = new LinkedList<>();
        refs = new ArrayList<>();
        return super.before(node);
    }

    @Override
    protected Object after(AstNode node, Object result) {
        finishLine();

        // This method is called by go() after visitation has finished
        // The return value will be passed to go() which passes it to the caller
        return sb.toString();
    }

    // =========================================================================


    public void visit(AstNode n) {
        // Fallback for all nodes that are not explicitly handled below
        write("<");
        write(n.getNodeName());
        write(" />");
    }

    public void visit(NodeList n) {
        iterate(n);
    }

    public void visit(Page p) {
        iterate(p.getContent());
    }

    public void visit(Text text) {
        write(text.getContent());
    }

    public void visit(Whitespace w) {
        write(" ");
    }

    public void visit(Bold b) {
        iterate(b.getContent());
    }

    public void visit(Italics i) {
        iterate(i.getContent());
    }

    public void visit(XmlCharRef cr) {
        write(Character.toChars(cr.getCodePoint()));
    }

    public void visit(XmlEntityRef er) {
        String ch = EntityReferences.resolve(er.getName());
        if (ch == null) {
            write('&');
            write(er.getName());
            write(';');
        } else {
            write(ch);
        }
    }

    public void visit(Table t) {
        iterate(t.getBody());
    }

    public void visit(TableRow tr) {
        iterate(tr.getBody());
        newline(3);
    }

    public void visit(TableHeader th) {
        iterate(th.getBody());
        newline(2);
    }

    public void visit(TableCell tc) {
        iterate(tc.getBody());
    }

    public void visit(Itemization i) {
        iterate(i.getContent());
    }

    public void visit(ItemizationItem ii) {
        iterate(ii.getContent());
        newline(2);
    }

    public void visit(Enumeration e) {
        iterate(e.getContent());
    }

    public void visit(EnumerationItem ei) {
        iterate(ei.getContent());
        newline(2);
    }

    public void visit(Url url) {
        write(url.getProtocol());
        write(':');
        write(url.getPath());
        write(' ');
    }

    public void visit(ExternalLink link) {
        iterate(link);
        newline(2);
    }

    public void visit(InternalLink link) {
        try {
            PageTitle page = PageTitle.make(config, link.getTarget());
            if (page.getNamespace().equals(config.getNamespace("Category"))) {
                return;
            }
        } catch (LinkTargetException e) {
            logger.debug(e, e);
        }

        write(link.getPrefix());
        if (link.getTitle().getContent() == null || link.getTitle().getContent().isEmpty()) {
            write(link.getTarget());
        } else {
            iterate(link.getTitle());
        }
        write(link.getPostfix());
    }

    public void visit(Section s) {
        finishLine();
        StringBuilder saveSb = sb;
        boolean saveNoWrap = noWrap;

        sb = new StringBuilder();
        noWrap = true;

        iterate(s.getTitle());
        finishLine();
        String title = sb.toString().trim();

        sb = saveSb;

        if (s.getLevel() >= 1) {
            while (sections.size() > s.getLevel()) {
                sections.removeLast();
            }
            while (sections.size() < s.getLevel()) {
                sections.add(1);
            }

            StringBuilder sb2 = new StringBuilder();
            for (int i = 0; i < sections.size(); ++i) {
                if (i < 1) {
                    continue;
                }

                sb2.append(sections.get(i));
                sb2.append('.');
            }

            if (sb2.length() > 0) {
                sb2.append(' ');
            }
            sb2.append(title);
            title = sb2.toString();
        }

        newline(2);
        write(title);
        newline(3);

        noWrap = saveNoWrap;

        iterate(s.getBody());

        while (sections.size() > s.getLevel()) {
            sections.removeLast();
        }

        sections.add(sections.removeLast() + 1);
    }

    public void visit(Paragraph p) {
        iterate(p.getContent());
        newline(2);
    }

    public void visit(HorizontalRule hr) {
        newline(2);
    }

    public void visit(XmlElement e) {
        if (e.getName().equalsIgnoreCase("br")) {
            newline(1);
        } else {
            iterate(e.getBody());
        }
    }

    public void visit(TagExtension n) {
        if (n.getName().toLowerCase().equals("ref")) {
            refs.add(n);
        }
    }

    public void visit(Template n) {
        if (n.isEmpty()) {
            return;
        }
        NodeList nl = n.getName();
        if (nl.isEmpty()) {
            return;
        }
        AstNode nameNode = nl.get(0);
        if (!(nameNode instanceof Text)) {
            return;
        }
        String name = ((Text) nameNode).getContent();
        if (!name.equals("Reflist")) {
            return;
        }
        for (TagExtension te : refs) {
            write(te.getBody());
            newline(2);
        }
    }

    // =========================================================================
    // Stuff we want to hide

    public void visit(ImageLink n) {
    }

    public void visit(IllegalCodePoint n) {
    }

    public void visit(XmlComment n) {
    }

    public void visit(TemplateArgument n) {
    }

    public void visit(TemplateParameter n) {
    }

    public void visit(MagicWord n) {
    }

    public void visit(XmlAttribute n) {
    }

    // =========================================================================

    private void newline(int num) {
        if (pastBod) {
            if (num > needNewlines) {
                needNewlines = num;
            }
        }
    }

    private void wantSpace() {
        if (pastBod) {
            needSpace = true;
        }
    }

    private void finishLine() {
        sb.append(line.toString());
        line.setLength(0);
    }

    private void writeNewlines(int num) {
        finishLine();
        sb.append(StringUtils.strrep('\n', num));
        needNewlines = 0;
        needSpace = false;
    }

    private void writeWord(String s) {
        int length = s.length();
        if (length == 0) {
            return;
        }

        if (!noWrap && needNewlines <= 0) {
            if (needSpace) {
                length += 1;
            }

            if (line.length() + length >= wrapCol && line.length() > 0) {
                writeNewlines(1);
            }
        }

        if (needSpace && needNewlines <= 0) {
            line.append(' ');
        }

        if (needNewlines > 0) {
            writeNewlines(needNewlines);
        }

        needSpace = false;
        pastBod = true;
        line.append(s);
    }

    private void write(String s) {
        if (s.isEmpty()) {
            return;
        }

        if (Character.isSpaceChar(s.charAt(0))) {
            wantSpace();
        }

        String[] words = WHITESPACE.split(s);
        for (int i = 0; i < words.length;) {
            writeWord(words[i]);
            if (++i < words.length) {
                wantSpace();
            }
        }

        if (Character.isSpaceChar(s.charAt(s.length() - 1))) {
            wantSpace();
        }
    }

    private void write(char[] cs) {
        write(String.valueOf(cs));
    }

    private void write(char ch) {
        writeWord(Character.toString(ch));
    }
}
