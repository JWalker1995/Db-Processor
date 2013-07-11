package db_processor.filter;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.ListIterator;

import db_processor.Filter;

public class FilterCleanHtml extends Filter
{
	private boolean opts_loaded = false;
	private String column;
	private int max_length;
	private boolean fix_invalid_nesting;
	
	@Override
	protected HashMap<String, String> get_params()
	{
		HashMap<String, String> res = new HashMap<String, String>();
		
		// The column to filter
		res.put("--column", null);
		
		// The column length limit
		res.put("--max-length", Integer.toString(Integer.MAX_VALUE));

		// Whether to fix invalid nesting: <i>Italic <b>Bold & Italic</i> Bold</b>
		res.put("--fix-invalid-nesting", "");
		
		return res;
	}
	
	@Override
	public void process(ResultSet row) throws SQLException
	{
		// Closing a non-opened tag will remove the closing tag.
		// Closing a incorrectly nested tag will end tags up to the opening tag.
		// Unclosed tags will be appended to the string
		
		// Don't confuse closing tags and tag endings:
		// <div>Closing Tags</div>
		//                  ^
		// <div>Tag Endings</div>
		//     ^                ^

		if (!opts_loaded)
		{
			column = opts.get("--column");
			if (column == null) {stop("Please specify a column with \"--column\""); return;}
			
			max_length = Integer.parseInt(opts.get("--max-length"));

			fix_invalid_nesting = !opts.get("--fix-invalid-nesting").isEmpty();

			opts_loaded = true;
		}
		
		String orig = row.getString(column);
		int use = Math.min(orig.length(), max_length);
		
		StringBuilder str;
		boolean changed;
		
		while (true)
		{
			str = new StringBuilder(orig.substring(0, use));
			changed = clean_html(str);
			
			if (str.length() <= max_length) {break;}
			use -= str.length() - max_length;
		}
		
		if (changed)
		{
			if (update)
			{
				row.updateString(column, str.toString());
				row.updateRow();
			}
			else
			{
				log(orig);
				log(str.toString());
				log();
			}
			count("Updated");
		}
		count("Processed");
	}
	
	private boolean clean_html(StringBuilder str)
	{
		boolean changed = false;
		LinkedList<String> tags = new LinkedList<String>();
		
		int i;
		int end = 0;
		tag: while ((i = str.indexOf("<", end)) != -1)
		{
			// Is open or close tag?
			int start = i;
			while (++i < str.length() && str.charAt(i) == ' ');
			boolean end_tag = i < str.length() && str.charAt(i) == '/';
			if (end_tag) {i++;}
			
			int tag_start = i;
			
			// Find end of tag
			while (true)
			{
				int quote1 = str.indexOf("'", i);
				int quote2 = str.indexOf("\"", i);
				int quote_i = quote1 == -1 ? quote2 : (quote2 == -1 ? quote1 : Math.min(quote1, quote2));
				
				end = str.indexOf(">", i) + 1;
				if (end == 0)
				{
					// Tag not ended
					end = str.length();
					str.append(">");
					changed = true;
				}
				if (quote_i == -1 || end <= quote_i) {break;}
				
				// If a quote comes before a ">", find the end quote:
				String quote = str.substring(quote_i, ++quote_i);
				i = str.indexOf(quote, quote_i) + 1;
				if (i == 0)
				{
					// Quote not ended
					str.append(quote);
					i = str.length();
					changed = true;
				}
			}
			
			// Continue on tags that are self-closed: <br />
			i = end - 1;
			while (str.charAt(--i) == ' ');
			if (str.charAt(i) == '/') {continue tag;}
			
			// Extract tag
			i = tag_start;
			while (i < str.length() && Character.isLetterOrDigit(str.charAt(i++)));
			String tag = str.substring(tag_start, i - 1).toLowerCase();
			
			// Continue on empty tags. This also includes comments: <!-- text -->
			if (tag.isEmpty()) {continue tag;}
			
			// Continue on void tags: area, base, br, col, embed, hr, img, input, keygen, link, menuitem, meta, param, source, track, wbr
			// http://www.w3.org/html/wg/drafts/html/master/syntax.html#void-elements
			if (tag.equals("area") || tag.equals("base") || tag.equals("br") || tag.equals("col") ||
				tag.equals("embed") || tag.equals("hr") || tag.equals("img") || tag.equals("input") ||
				tag.equals("keygen") || tag.equals("link") || tag.equals("menuitem") || tag.equals("meta") ||
				tag.equals("param") || tag.equals("source") || tag.equals("track") || tag.equals("wbr"))
				{continue tag;}
			
			if (end_tag)
			{
				if (fix_invalid_nesting)
				{
					if (tags.contains(tag))
					{
						// If this tag was opened previously, close all tags up to it
						StringBuilder insert = new StringBuilder();
						String poll;
						while (!(poll = tags.pollLast()).equals(tag))
						{
							insert.append("</" + poll + ">");
						}
						if (insert.length() > 0)
						{
							str.insert(start, insert);
							end += insert.length();
							changed = true;
						}
					}
					else
					{
						// If this tag was not opened, remove the end tag
						str.delete(start, end);
						end = start;
						changed = true;
					}
				}
				else
				{
					if (!tags.removeLastOccurrence(tag))
					{
						// If this tag was not opened, remove the end tag
						str.delete(start, end);
						end = start;
						changed = true;
					}
				}
			}
			else
			{
				tags.add(tag);
			}
		}
		
		// Add unclosed tags
		if (!tags.isEmpty())
		{
			ListIterator<String> it = tags.listIterator(tags.size());
			while (it.hasPrevious())
			{
				str.append("</" + it.previous() + ">");
			}
			changed = true;
		}
		
		return changed;
	}
}
