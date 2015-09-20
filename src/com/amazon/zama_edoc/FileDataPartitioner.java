//$Id$
package com.amazon.zama_edoc;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazon.zama_edoc.persistance.DataPersister;

/**
 * Process each csv file i.e each time reads upto some threshold size and invokes task to update data into cassendra.
 * @author shahulhameed.n
 *
 */
public class FileDataPartitioner implements Runnable {

	private static final Logger LOGGER = Logger.getLogger(FileDataPartitioner.class.getName());
	
	private static final int CONTENT_SIZE = 2000;
	
	private String filePath = null;
	private String fileName = null;
	private int startIdx = 0;
	private int endIdx = 0;
	
	public FileDataPartitioner(String absolutePath, int startIdx, int endIdx) {
		this.filePath = absolutePath;
		this.fileName = new File(filePath).getName();
		this.startIdx = startIdx;
		this.endIdx = endIdx;
	}
	
	public String getFileName() {
		return this.fileName + "_" + startIdx + "_" + endIdx;
	}

	@Override
	public void run() {
		LOGGER.log(Level.INFO, "@@@LARGESCALE: Partition called for {0}", this.getFileName());
		File f = new File(this.filePath);
		if(f.exists() && f.isFile()) {
			int size = (endIdx - startIdx) + 1;
	        FileInputStream fis = null;
	        try {
	        	createLockFile();
	        	fis = new FileInputStream(f);
	        	
	        	boolean breakUptoNewLine = false;
	        	if(startIdx != 0) {
	        		fis.skip(startIdx - 2);
	        		byte[] b = new byte[1];
	        		int prevread = fis.read(b);
	        		if(prevread != -1 && b[0] != '\n') {
        				breakUptoNewLine = true;
        			}
	        	}
	        	
	        	byte[] c = new byte[size];
	        	int readChars = fis.read(c);
	        	if(readChars != -1) {
	        		int newLineCount = 0;
	        		StringBuilder sb = new StringBuilder();
	        		int fileCount = 0;
	        		for(byte cbyte : c) {
	        			if(breakUptoNewLine) {
	        				if(cbyte == '\n') {
	        					breakUptoNewLine = false;
	        				}
	        				continue;
	        			}
	        			sb.append((char) cbyte);
	        			if(cbyte == '\n') {
	        				newLineCount++;
	        				if(newLineCount == CONTENT_SIZE) {
	        					writePartitionFile(sb.toString(), fileCount++);
	        					newLineCount = 0;
	        					sb = new StringBuilder();
	        				}
	        			}
	        		}
	        		if(c[size - 1] != '\n') {
	        			int nextByte = -1;
        				while((nextByte = fis.read()) != -1) {
        					char nextChar = (char)nextByte;
        					sb.append(nextChar);
        					if(nextChar == '\n') {
        						break;
        					}
        				}
	        		}
	        		String data = sb.toString();
	        		if(!"".equals(data)) {
	        			writePartitionFile(data, fileCount++);
	        		}
	        	}
	        	deleteLockFile();
	        } catch(Exception ex) {
	        	LOGGER.log(Level.WARNING, "@@@@ Exception at file partitioning ", ex);
	        	ex.printStackTrace();
	        } finally {
	        	try {
	        		if(fis != null) {
	        			fis.close();
	        		}
	        	} catch(IOException ex) {
	        		LOGGER.log(Level.WARNING, "@@@@ Exception at closing file streams partitioning ", ex);
		        	ex.printStackTrace();
	        	}
	        }
	       
//			BufferedReader reader = null;
//			try {
//				reader = new BufferedReader(new FileReader(f));
//				StringBuilder sb = new StringBuilder();
//				String line = null;
//				int counter = 0;
//				while((line = reader.readLine()) != null) {
//					sb.append(line);
//					counter++;
//					if(counter >= CONTENT_SIZE) {
//						//invoke thread to process this content & reset the counter
//						FileScheduler.getFileDataProcessor().submit(new FileDataPartProcessor(f.getName(), counter, sb.toString()));
//						sb = new StringBuilder();
//						counter = 0;
//					}
//				}
//			} catch(Exception ex) {
//				
//			}
		}
	}

	/**
	 * Deletes the lock file after the data partition
	 */
	private void deleteLockFile() {
		File lockFile = new File(FilePartitionDistributor.HIDDEN_FOLDER + "/" + getFileName() + ".lock");
		if(lockFile.exists() && lockFile.delete()) {
			LOGGER.log(Level.INFO, "@@@@ lock file deleted {0} ", lockFile.getAbsolutePath());
		}
	}

	/**
	 * Creates lock file to process the data partition
	 */
	private void createLockFile() {
		OutputStream os = null;
		try {
			File lockFile = new File(FilePartitionDistributor.HIDDEN_FOLDER + "/" + getFileName() + ".lock");
			if(!lockFile.exists() && lockFile.createNewFile()) {
				os = new BufferedOutputStream(new FileOutputStream(lockFile));
				String data = this.filePath + ",," + startIdx + "," + endIdx;
				os.write(data.getBytes("UTF-8"));
				LOGGER.log(Level.INFO, "@@@@ lock file created {0}", lockFile.getAbsolutePath());
			}
		} catch(Exception ex) {
			LOGGER.log(Level.WARNING, "@@@@ Exception at creating lock file ", ex);
        	ex.printStackTrace();
		} finally {
			if(os != null) {
    			try {
					os.close();
				} catch (IOException e) {
					LOGGER.log(Level.WARNING, "@@@@ Exception at closing lock file streams ", e);
					e.printStackTrace();
				}
    		}
		}
	}

	private void writePartitionFile(String data, int fileCount) {
		
//		DataPersister dp = new DataPersister(data.split("\n"));
//		FileScheduler.getFileDataPersisterPoolExecutor().submit(dp);
		
		OutputStream os = null;
		try {
			File outFile = new File(FilePartitionDistributor.HIDDEN_FOLDER + "/" + getFileName() + "-" + fileCount + ".partition");
			if(outFile.exists()) {
				outFile.delete();
			}
			outFile.createNewFile();
			os = new BufferedOutputStream(new FileOutputStream(outFile));
			os.write(data.getBytes("UTF-8"));
			DataPersister dp = new DataPersister(outFile.getAbsolutePath());
			FileScheduler.getFileDataPersisterPoolExecutor().submit(dp);
		} catch(Exception ex) {
			LOGGER.log(Level.WARNING, "@@@@ Exception at writing file partitioning ", ex);
        	ex.printStackTrace();
		} finally {
			if(os != null) {
    			try {
					os.close();
				} catch (IOException e) {
					LOGGER.log(Level.WARNING, "@@@@ Exception at closing write partition file streams ", e);
					e.printStackTrace();
				}
    		}
		}
	}

}
