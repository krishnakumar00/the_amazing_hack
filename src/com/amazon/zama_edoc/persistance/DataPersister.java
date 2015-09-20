package com.amazon.zama_edoc.persistance;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.MessageFormat;

import com.amazon.zama_edoc.cassandra.CassandraAdapter;
import com.amazon.zama_edoc.util.StringUtil;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

/**
 * This class is to persis the data from the memory to the Cassendra / Any DB Storage
 * @author shahul-1094
 *
 */
public class DataPersister implements Runnable
{
	private static final String SELECT_ITEM_QUERY = "select {0} from {1} where item_id = {2}";

	private static final String INSERT_ITEM_QUERY = "insert into {0} (item_id, {1}) values ({2}, {3})";

	private static final String UPDATE_ITEM_QUERY = "update {0} set {1} = {2} where item_id={3}";

	String[] items = null;
	String filePath = null;

	public DataPersister(String[] items) {
		this.items = items;
	}

	public DataPersister(String absolutePath) {
		this.filePath = absolutePath;
	}

	@Override
	public void run()
	{
		if(filePath != null) {
			BufferedReader br = null;
			try {
				br = new BufferedReader(new FileReader(this.filePath));
				String line = null;
				while((line = br.readLine()) != null) {
					processRecord(line);
				}
				deletePartitionFile();
			} catch(Exception ex) {
				ex.printStackTrace();
			} finally {
				if(br != null) {
					try {
						br.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		} else if(items != null) {
			for(String item : items) {
				processRecord(item);
			}
		}
	}
	
	private void deletePartitionFile() {
		File f = new File(this.filePath);
		if(f.exists()) {
			f.delete();
			System.out.println("@@@@ Partition file deleted " + this.filePath);
		}
	}

	private void processRecord(String item) {
		String[] values = item.split(",");
		if(values.length != 3) {
			System.out.println("Invalid item entry. Item: " + item);
			return;
		}
		int itemId = -1;
		try {
			itemId = Integer.parseInt(values[0]);
		}
		catch(NumberFormatException e) {
			e.printStackTrace(System.err);
			return;
		}

		if(StringUtil.isEmptyString(values[1], values[2])) {
			System.out.println("Invalid item entry. Item: " + item);
			return;
		}
		try {
			persistItem(itemId, values[1].toLowerCase(), values[2].toLowerCase());
		} catch(Exception ex) {
			System.out.println("Exception at the data entry " + item);
			ex.printStackTrace();
		}
	}

	public void persistItem(int itemId, String attrName, String attrValue) {
		persistItem(itemId, attrName, attrValue, "items");
		String currDiffColumnFamily = "";
		ResultSet rs = CassandraAdapter.executeQuery("select curr_columnfamily from globals");
		for(Row r : rs) {
			currDiffColumnFamily = r.getString("curr_columnfamily");
		}
		if(StringUtil.isEmptyString(currDiffColumnFamily)) {
			return;
		}
		/*

	try {

	Properties p = new Properties();

	p.load(new FileInputStream(new File("/Volumes/Official/temp/columnfamily")));

	currDiffColumnFamily = p.getProperty("curr_columnfamily");

	}

	catch(Exception e) {

	e.printStackTrace();

	} */

		if(!StringUtil.isEmptyString(currDiffColumnFamily)) {

			persistItem(itemId, attrName, attrValue, currDiffColumnFamily);

		}

		// CassandraAdapter.close();

	}



	private void persistItem(int itemId, String attrName, String attrValue, String colFamily)
	{
		/*
		 * check if any such item exists
		 */
		Object dbAttrValue = null;
		String cql = MessageFormat.format(SELECT_ITEM_QUERY, new Object[] {attrName, colFamily, String.valueOf(itemId)});
		System.out.println(cql);

		ResultSet rs = CassandraAdapter.executeQuery(cql);
		for(Row r :rs) {
			dbAttrValue = r.getObject(attrName);
		}
		/*
		 * check if the value is still the same.
		 */
		if(dbAttrValue != null) {
			if(!attrValue.equals(dbAttrValue)) {
				// update the record and update to the diff column family.
				String updateQueryAttrValue = null;
				if("list_price".equals(attrName) || "release_date".equals(attrName)) {
					updateQueryAttrValue = attrValue;
				}
				else {
					updateQueryAttrValue += "'" + attrValue + "'";
				}
				String updateQuery = MessageFormat.format(UPDATE_ITEM_QUERY, new Object[] {colFamily, attrName, updateQueryAttrValue, String.valueOf(itemId)});
				System.out.println(updateQuery);
				CassandraAdapter.executeQuery(updateQuery);
			}
			else {
				System.out.println("Still the same");
			}
		}
		else {
			String insertQueryAttrValue = null;
			if("list_price".equals(attrName) || "release_date".equals(attrName)) {
				insertQueryAttrValue = attrValue;
			}
			else {
				insertQueryAttrValue = "'" + attrValue + "'";
			}
			String insertQuery = MessageFormat.format(INSERT_ITEM_QUERY, new Object[] {colFamily, attrName, String.valueOf(itemId), insertQueryAttrValue});
			System.out.println(insertQuery);
			CassandraAdapter.executeQuery(insertQuery);
		}
	}

	public static void main(String[] args)
	{
		new DataPersister(new String[] {"4,authors,parath1 sarathy"}).run();
	}
}
