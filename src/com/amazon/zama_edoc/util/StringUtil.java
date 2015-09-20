package com.amazon.zama_edoc.util;

public class StringUtil
{
	public static boolean isEmptyString(String... strs)
	{
		for(String s : strs) {
			return s == null ? false : "".equals(s.trim());
		}
		return false;
	}
}
