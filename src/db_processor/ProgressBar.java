package db_processor;

public class ProgressBar
{
	private int cur;
	private int max;
	private int width;
	private String unit;
	
	public ProgressBar(int max, int width, String unit)
	{
		this.cur = 0;
		this.max = max;
		this.width = width;
		this.unit = unit;
		update();
	}
	
	public void set_cur(int cur)
	{
		if (cur < 0) {cur = 0;}
		else if (cur > max) {cur = max;}
		
		this.cur = cur;
		update();
	}
	public void set_max(int max)
	{
		if (max < 0) {max = 0;}
		else if (max < cur) {max = cur;}
		
		this.max = max;
		update();
	}
	public void end()
	{
		System.out.println();
	}
	
	protected void update()
	{
		if (max == 0)
		{
			System.out.print(Integer.toString(cur) + unit + "\r");
		}
		else
		{
			int perc = (int) Math.floor(cur * 100 / max);
			int bar = (int) Math.floor(cur * width / max);
	
			System.out.print(Integer.toString(perc) + "% [" + repeat('=', bar) + repeat(' ', width - bar) + "] " + Integer.toString(cur) + unit + "\r");
		}
	}
	private StringBuilder repeat(char c, int n)
	{
		StringBuilder res = new StringBuilder();
		while (n-- > 0) {res.append(c);}
		return res;
	}
}
