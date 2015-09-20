//$Id$
package com.amazon.zama_edoc;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class FileScheduler {

	private static ThreadPoolExecutor fileDataPartitionerPoolExecutor = null;
	private static ThreadPoolExecutor fileDataPersisterPoolExecutor = null;
	
	/**
	 * Used singleton pattern variable for the Data partition pool executor
	 * @return
	 */
	public static ThreadPoolExecutor getFileDataPartitionerPoolExecutor() {
		if(fileDataPartitionerPoolExecutor == null) {
			initTaskPoolExecutor();
		}
		return fileDataPartitionerPoolExecutor;
	}
	
	private static synchronized void initTaskPoolExecutor() {
		if(fileDataPartitionerPoolExecutor == null) {
			RejectedExecutionHandler rejectHandler = new ThreadPoolExecutor.AbortPolicy();
			TaskThreadFactory ttf = new TaskThreadFactory("FileDataPartitionerPoolExecutor");	//No I18N
			fileDataPartitionerPoolExecutor = new ThreadPoolExecutor(2, 4, 1L, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>(), ttf, rejectHandler);
		}
	}
	
	/**
	 * Used singleton pattern variable for the Data persister pool executor
	 * @return
	 */
	public static ThreadPoolExecutor getFileDataPersisterPoolExecutor() {
		if(fileDataPersisterPoolExecutor == null) {
			initDataPersisterTaskPoolExecutor();
		}
		return fileDataPersisterPoolExecutor;
	}
	
	private static synchronized void initDataPersisterTaskPoolExecutor() {
		if(fileDataPersisterPoolExecutor == null) {
			RejectedExecutionHandler rejectHandler = new ThreadPoolExecutor.AbortPolicy();
			TaskThreadFactory ttf = new TaskThreadFactory("FileDataPersisterPoolExecutor");	//No I18N
			fileDataPersisterPoolExecutor = new ThreadPoolExecutor(2, 4, 1L, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>(), ttf, rejectHandler);
		}
	}
	
	/**
	 * This Task executor to write the thred name for identification
	 * @author shahul-1094
	 *
	 */
	private static class TaskThreadFactory implements ThreadFactory {

		private String name = null;
		private int counter = 0;
		
		protected TaskThreadFactory(String name) {
			this.name = name;
		}
		
		@Override
		public Thread newThread(Runnable r) {
			String tname = this.name + "-" + counter++;
			Thread t = new Thread(r, tname);
			return t;
		}
	}
}
