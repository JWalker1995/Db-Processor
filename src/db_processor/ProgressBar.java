package db_processor;

public class ProgressBar
{
	private long min;
	private long cur;
	private long max;
	private long range;
	private int width;
	private String unit;
	
	public ProgressBar(long min, long max, int width, String unit)
	{
		this.min = min;
		this.cur = min;
		this.max = max;
		this.range = max - min;
		if (this.range <= 0) {this.range = 0;}
		this.width = width;
		this.unit = unit;
		update();
	}
	
	public void set_min(long min)
	{
		if (cur < min) {cur = min;}
		else if (min > max) {min = max;}
		
		this.min = min;
		this.range = max - min;
		update();
	}
	public void set_cur(long cur)
	{
		if (cur < min) {cur = min;}
		else if (cur > max) {cur = max;}
		
		this.cur = cur;
		update();
	}
	public void set_max(long max)
	{
		if (cur > max) {cur = max;}
		else if (max < min) {max = min;}
		
		this.max = max;
		this.range= max - min;
		update();
	}
	public void end()
	{
		System.out.println();
	}
	
	protected void update()
	{
		long offset = cur - min;
		
		if (max == 0)
		{
			System.out.print(Long.toString(offset) + unit + "\r");
		}
		else
		{
			int perc = (int) Math.floor(offset * 100 / range);
			int bar = (int) Math.floor(offset * width / range);
			
			String.format("%010d", perc);
			
			System.out.print(String.format("%3s", perc) + "% [" + repeat('=', bar) + repeat(' ', width - bar) + "] " + Long.toString(offset) + unit + "\r");
		}
	}
	private StringBuilder repeat(char c, int n)
	{
		StringBuilder res = new StringBuilder();
		while (n-- > 0) {res.append(c);}
		return res;
	}
}
