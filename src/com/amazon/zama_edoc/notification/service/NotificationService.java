package com.amazon.zama_edoc.notification.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.amazon.zama_edoc.cassandra.*;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public class NotificationService
{
	
	public static final Notifiers[] notifiers = { new Notifiers(1, 100), new Notifiers(101, 200), new Notifiers(201, 300) };

	public NotificationService()
	{
		
	}
	
	public void start()
	{
		while(true) {
			try {
				try {
					String currDiffColumnFamily = "";
					String oldDiffColumnFamily = "";
					
					
					
					ResultSet rs = CassandraAdapter.executeQuery("select curr_columnfamily, old_columnfamily from globals");
					for(Row r : rs) {
						currDiffColumnFamily = r.getString("curr_columnfamily");
					
						oldDiffColumnFamily = r.getString("old_columnfamily");
					}
					
					try {
						CassandraAdapter.executeQuery("update globals set curr_columnfamily = '" + oldDiffColumnFamily  + "' where id=1");
						CassandraAdapter.executeQuery("update globals set old_columnfamily = '" + currDiffColumnFamily  + "' where id=1");
					}
					catch(Exception e) {
						e.printStackTrace();
					}
					
					for(Notifiers n : notifiers) {
						System.out.println("notifier " + n + " started.");
						new Thread(n).start();
					}
					
					checkTaskCompletion();
					System.out.println("Notified successfully to all subscribers.");
					
					// CassandraAdapter.executeQuery("truncate " + currDiffColumnFamily);
				}
				catch(Exception e) {
					e.printStackTrace();
				}
				Thread.sleep(TimeUnit.SECONDS.toMillis(60));
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public void checkTaskCompletion()
	{
		while(true) {
			int allCompleted = 0;
			for(Notifiers n : notifiers) {
				if(n.isCompleted()) {
					allCompleted++;
				}
				if(allCompleted == notifiers.length) {
					return;
				}
			}
			try {
				Thread.sleep(TimeUnit.SECONDS.toMillis(1));
			}
			catch(Exception e) {
				e.printStackTrace();
			}
			
		}
	}
	
	public static void main(String[] args)
	{
		new NotificationService().start();
	}
}
