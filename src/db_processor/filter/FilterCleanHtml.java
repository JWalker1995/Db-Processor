package db_processor.filter;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.ListIterator;

import db_processor.Filter;

public class FilterCleanHtml extends Filter
{
	public FilterCleanHtml()
	{
		// Will be run less often than process()
	}
	
	@Override
	public void process(ResultSet row) throws SQLException
	{
		// Whether to fix invalid nesting: <i>Italic <b>Bold & Italic</i> Bold</b>
		boolean fix_invalid_nesting = false;
		
		// Closing a non-opened tag will remove the closing tag.
		// Closing a incorrectly nested tag will end tags up to the opening tag.
		// Unclosed tags will be appended to the string
		
		// Don't confuse closing tags and tag endings:
		// <div>Closing Tags</div>
		//                  ^
		// <div>Tag Endings</div>
		//     ^                ^
		
		// <d><a><b></a></b></c>
		// <d><a><b></b></a></b></c>
		// <d><a><b></b></a>
		// <d><a><b></b></a></d>
		
		boolean changed = false;
		StringBuilder str = new StringBuilder(row.getString("text"));
		LinkedList<String> tags = new LinkedList<String>();
		
		int start;
		int end = 0;
		tag: while ((start = str.indexOf("<", end)) != -1)
		{
			// Is open or close tag?
			while (str.charAt(++start) == ' ');
			boolean end_tag = str.charAt(start) == '/';
			if (end_tag) {start++;}
			int i = start;
			
			// Find end of tag
			int quote_i;
			while (true)
			{
				quote_i = Math.min(str.indexOf("\"", i), str.indexOf("'", i));
				end = str.indexOf(">", i);
				if (end == -1)
				{
					// Tag not ended
					count("Tag not ended");
					break tag;
				}
				if (quote_i == -1 || end < quote_i) {break;}
				
				// If a quote comes before a ">", find the end quote:
				String quote = str.substring(quote_i, ++quote_i);
				i = str.indexOf(quote, quote_i) + 1;
				if (i == 0)
				{
					// Quote not ended
					count("Quote not ended");
					i = quote_i;
				}
			}
			
			// Extract tag
			int space = str.indexOf(" ", start);
			if (space == -1) {space = end;}
			String tag = str.substring(start, Math.min(space, end));
			
			if (end_tag)
			{
				if (fix_invalid_nesting)
				{
					if (tags.contains(tag))
					{
						// If this tag was opened
						StringBuilder insert = new StringBuilder();
						String poll;
						while (!(poll = tags.pollLast()).equals(tag))
						{
							insert.append("</" + poll + ">");
						}
						if (insert.length() > 0)
						{
							str.insert(end + 1, insert);
							changed = true;
						}
					}
					else
					{
						// If this tag was not opened, remove the end tag
						str.replace(start, end, "");
						changed = true;
					}
				}
				else
				{
					if (!tags.removeLastOccurrence(tag))
					{
						// If this tag was not opened, remove the end tag
						str.replace(start, end, "");
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
			ListIterator<String> i = tags.listIterator();
			while (i.hasNext())
			{
				str.append("</" + i.next() + ">");
			}
			changed = true;
		}
		
		if (changed)
		{
			count(row.getString("text") + " -> " + str.toString());
			
			row.updateString("text", str.toString());
			row.updateRow();
			count("updated");
		}
		count("processed");
	}
}
