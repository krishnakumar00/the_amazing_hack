//$Id$
package com.amazon.zama_edoc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazon.zama_edoc.util.FileUtil;
import com.amazon.zama_edoc.util.FileUtil.FileLastModifiedTimeBasedComparator;
import com.csvreader.CsvReader;

/*
 * This class is the main program and do the follows
 * 1) It'll check the shared folder csv files based on last modified time
 * 2) Distributes each file to the node with index partition (For StandAlone, Used separate threads instead of node)
 * 3) It'll rerun the locked files if any disturbances like shutdown, etc
 *  
 */
public class FilePartitionDistributor {
	
	private static final Logger LOGGER = Logger.getLogger(FilePartitionDistributor.class.getName());
	
	private static final String LAST_PROCESSED_HIDDEN_FILE = ".last_processed_time";
	public static final String HIDDEN_FOLDER = "/Users/shahul-1094/Documents/records/.tmp";
	private static final int PARTITION_SIZE = 5242880;	//5 MB data in bytes

	public static void main(String[] args) {
		try {
			String folder = "/Users/shahul-1094/Documents/records";
			File hiddenFolder = new File(HIDDEN_FOLDER);
			if(!hiddenFolder.exists()) {
				hiddenFolder.mkdir();
			}
			boolean start = true;
			while(start) {
				long lastReadTime = readLastReadTime();
				waitToFinishLockedFiles(folder, lastReadTime);
				processSharedFolder(folder, lastReadTime);
				//Sleep for 10 seconds and try again
				Thread.currentThread().sleep(10000);
			}
		} catch(Exception ex) {
			LOGGER.log(Level.WARNING, "@@@LARGESCALE: Exception at main thread", ex);
			ex.printStackTrace();
		}
	}
	
	/**
	 * Suppose if any abruption / suddenly killed the partition process, it'll rerun the locked files to recover from data loss.
	 * @param folder
	 * @param lastReadTime
	 */
	private static void waitToFinishLockedFiles(String folder, long lastReadTime) {
		while(reRunLockedFiles(folder, lastReadTime)) {
			try {
				Thread.currentThread().sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		partitionFutures.clear();
	}
	
	private static boolean reRunLockedFiles(String sharedFolder, long lastReadTime) {
		File hiddenFolder = new File(HIDDEN_FOLDER);
		File[] lockedFiles = hiddenFolder.listFiles(new FileUtil.LockedFilesFilter());
		if(lockedFiles != null && lockedFiles.length > 0) {
			
			for(File lockedFile : lockedFiles) {
				try {
					CsvReader csv = new CsvReader(lockedFile.getAbsolutePath());
					if(lockedFile.exists() && csv.readRecord()) {
						String filename = csv.get(0);
						int startIdx = Integer.parseInt(csv.get(2));
						int endIdx = Integer.parseInt(csv.get(3));
						FileDataPartitioner fdp = new FileDataPartitioner(filename, startIdx, endIdx);
						if(partitionFutures.containsKey(fdp.getFileName())) {
							Future future = partitionFutures.get(fdp.getFileName());
							if(future.isDone() || future.isCancelled()) {
								partitionFutures.remove(fdp.getFileName());
								if(future.isCancelled()) {
									FileScheduler.getFileDataPartitionerPoolExecutor().submit(fdp);
								}
							}
						} else {
							FileScheduler.getFileDataPartitionerPoolExecutor().submit(fdp);
						}
					}
				} catch(Exception ex) {
					LOGGER.log(Level.WARNING, "@@@LARGESCALE: Exception at reading lock csv files " + lockedFile.getAbsolutePath() + " : ", ex);
					ex.printStackTrace();
				}
			}
			return true;
		}
		return false;
	}

	/**
	 * This method is to process the shared folder, iterate all files and distribute each file to each node
	 * @param sharedFolder
	 */
	private static void processSharedFolder(String sharedFolder, long lastReadTime) {
		
		File shared = new File(sharedFolder);
		if(shared.isDirectory()) {
			//Get the files last modified time and check with the last read time. If it's greater, read those files and process
			List<File> sortedFiles = FileUtil.sortFiles(shared.listFiles(), new FileLastModifiedTimeBasedComparator());
			if(sortedFiles != null) {
				for(File file : sortedFiles) {
					if(file.isFile()) {
						distributeFileWithPartitionIndex(file, lastReadTime);
					}
				}
			}
		} else if(shared.isFile()) {
			//Process file
			distributeFileWithPartitionIndex(shared, lastReadTime);
		}
		writeLastReadTime();
	}

	private static HashMap<String, Future> partitionFutures = new HashMap<String, Future>();
	
	/**
	 * This method is to distribute the CSV file to the listening node.
	 * @param csvFile
	 * @param lastReadTime
	 */
	private static void distributeFileWithPartitionIndex(File file, long lastReadTime) {
		long lastModifiedTime = file.lastModified();
		if(lastReadTime == -1 || lastReadTime <= lastModifiedTime) {
			//Distribute this file as new / updated one
			long fileSize = file.length();
			int startIdx = 0;
			int endIdx = PARTITION_SIZE - 1;
			while(endIdx < fileSize) {
				FileDataPartitioner partitioner = new FileDataPartitioner(file.getAbsolutePath(), startIdx, endIdx);
				Future<?> future = FileScheduler.getFileDataPartitionerPoolExecutor().submit(partitioner);
				partitionFutures.put(partitioner.getFileName(), future);
				startIdx = endIdx + 1;
				endIdx += PARTITION_SIZE;
			}
			endIdx = (int) fileSize;
			FileDataPartitioner partitioner = new FileDataPartitioner(file.getAbsolutePath(), startIdx, endIdx);
			Future<?> future = FileScheduler.getFileDataPartitionerPoolExecutor().submit(partitioner);
			partitionFutures.put(partitioner.getFileName(), future);
			waitToFinishLockedFiles(file.getParent(), lastReadTime);
		} else {
			LOGGER.log(Level.INFO, "@@@LARGESCALE: CSV File is not changed : {0}, {1} ", new Object[]{file.getAbsolutePath(), new Date(System.currentTimeMillis())});
		}
	}
	
	/**
	 * This method is to write the last read time as an hidden file
	 */
	private static void writeLastReadTime() {
		BufferedWriter bw = null;
		try {
			File lastReadFile = new File(HIDDEN_FOLDER + "/" + LAST_PROCESSED_HIDDEN_FILE);
			if(!lastReadFile.exists()) {
				lastReadFile.createNewFile();
			}
			FileWriter fw = new FileWriter(lastReadFile);
			bw = new BufferedWriter(fw);
			bw.write(String.valueOf(System.currentTimeMillis()));
		} catch(Exception ex) {
			LOGGER.log(Level.WARNING, "@@@LARGESCALE: Exception at writing the last time in file : ", ex);
			ex.printStackTrace();
		} finally {
			if(bw != null) {
				try {
					bw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/*
	 * Method to get last read time which is we read the shared folder csv files last time
	 */
	private static long readLastReadTime() {
		File lastReadFile = new File(HIDDEN_FOLDER + "/" + LAST_PROCESSED_HIDDEN_FILE);
		if(!lastReadFile.exists()) {
			return -1L;
		}
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(lastReadFile));
			String line = null;
			while((line = reader.readLine()) != null ) {
				return Long.parseLong(line);
			}
		} catch(Exception ex) {
			LOGGER.log(Level.WARNING, "@@@LARGESCALE: Exception at reading the last time from file : ", ex);
			ex.printStackTrace();
		} finally {
			if(reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return -1L;
	}
}
