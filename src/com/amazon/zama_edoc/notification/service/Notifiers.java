package com.amazon.zama_edoc.notification.service;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.Set;

import com.amazon.zama_edoc.cassandra.*;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public class Notifiers implements Runnable
{
	private boolean completedFlag;
	
	private final Map<String, List<String>> configurations = new HashMap<String, List<String>>();
	
	public Notifiers(int subscriberStartRange, int subscriberEndRange)
	{
		init(subscriberStartRange, subscriberEndRange);
	}
	
	private  void init(int subscriberStartRange, int subscriberEndRange)
	{
		String subsQuery = "select notify_to, notify_on from subscribers where id >=" +
					subscriberStartRange  + " and id <=" + subscriberEndRange + " allow filtering";
		
		System.out.println(subsQuery);
		
		ResultSet rs = CassandraAdapter.executeQuery(subsQuery);
		
		if(rs == null) {
			return;
		}
		for(Row r : rs) {
			String notifyTo = r.getString("notify_to");
			List<String> notifyOn = r.getList("notify_on", String.class);
			
			configurations.put(notifyTo, notifyOn);
			
			System.out.println(this + " " + configurations);
		}
	}
	
	public void run()
	{
		completedFlag = false;
		
		String oldDiffColumnFamily = null;
		try {
			Properties p = new Properties();
			p.load(new FileInputStream(new File("/Volumes/Official/temp/columnfamily")));
			oldDiffColumnFamily = p.getProperty("old_columnfamily");
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
		for(Entry<String, List<String>> entry : configurations.entrySet()) {
			for(String config : entry.getValue()) {
				String query = "select item_id from " + oldDiffColumnFamily + " where " + config + "  allow filtering";
				System.out.println("query: " + query);
				ResultSet rs = CassandraAdapter.executeQuery(query);
				if(rs == null) {
					continue;
				}
				Integer itemId = -1;
				for(Row r : rs) {
					itemId = r.getInt("item_id");
				}
				if(itemId != -1) {
					/*
					 * notify to customer. 
					 */
					System.out.println("notifying to customer : " + entry.getKey());
				}
			}
		}
		
		completedFlag = true;
	}
	
	public boolean isCompleted()
	{
		return completedFlag;
	}
}
