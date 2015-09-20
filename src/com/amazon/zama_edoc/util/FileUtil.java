//$Id$
package com.amazon.zama_edoc.util;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class FileUtil {

	public static List<File> sortFiles(File[] files, Comparator c) {
		if(files != null && files.length > 0) {
			List<File> csvFiles = new ArrayList<File>(Arrays.asList(files));
			Collections.sort(csvFiles, c);
			return csvFiles;
		}
		return null;
	}
	
	/**
	 * This class is to sort the Files based on the last modified time.
	 * @author shahul-1094
	 *
	 */
	public static class FileLastModifiedTimeBasedComparator implements Comparator<File> {

		@Override
		public int compare(File f1, File f2) {
			if(f1.lastModified() < f2.lastModified()) {
				return 1;
			}
			return 0;
		}
	}
	
	/**
	 * This class is to check the Files whether it's ends with .lock
	 * @author shahul-1094
	 *
	 */
	public static class LockedFilesFilter implements FilenameFilter {

		@Override
		public boolean accept(File dir, String name) {
			return name.endsWith(".lock");
		}
	}
}
