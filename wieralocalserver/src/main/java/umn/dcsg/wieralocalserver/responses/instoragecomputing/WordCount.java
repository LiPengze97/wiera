package umn.dcsg.wieralocalserver.responses.instoragecomputing;

import umn.dcsg.wieralocalserver.LocalInstance;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Created by Kwangsung on 2/4/2016.
 */
public class WordCount extends InStorageComputing
{
	public WordCount(LocalInstance instance)
	{
		super(instance);
	}

	@Override
	public byte[] doComputing(Object... args)
	{
		//To String first to count word
		byte[] ret = null;
		byte[] value = (byte[])args[0];
		Map wordCount = new LinkedHashMap<>();
		int nCnt;

		if(value != null)
		{
			String strValue = new String(value);
			StringTokenizer tokenizer = new StringTokenizer(strValue);
			String strWord;

			while (tokenizer.hasMoreTokens())
			{
				strWord = tokenizer.nextToken();
				if(!wordCount.containsKey(strWord))
				{
					nCnt = 1;
				}
				else
				{
					nCnt = (int) wordCount.get(strWord) + 1;
				}

				wordCount.put(strWord, nCnt);
			}

			ret = wordCount.toString().getBytes();
		}

		return ret;
	}
}
