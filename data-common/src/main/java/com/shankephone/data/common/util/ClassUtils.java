
package com.shankephone.data.common.util;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URL;



public abstract class ClassUtils {
	
	public static ClassLoader getDefaultClassLoader() {
		ClassLoader cl = null;
		try {
			cl = Thread.currentThread().getContextClassLoader();
		}
		catch (Throwable ex) {
			// Cannot access thread context ClassLoader - falling back...
		}
		if (cl == null) {
			// No thread context class loader -> use class loader of this class.
			cl = ClassUtils.class.getClassLoader();
			if (cl == null) {
				// getClassLoader() returning null indicates the bootstrap ClassLoader
				try {
					cl = ClassLoader.getSystemClassLoader();
				}
				catch (Throwable ex) {
					// Cannot access system ClassLoader - oh well, maybe the caller can live with null...
				}
			}
		}
		return cl;
	}
	
	public static URL getResource(String path) throws FileNotFoundException {
		URL url = getDefaultClassLoader().getResource(path);
		if (url == null) {
			throw new FileNotFoundException("class path resource [" + path + "] cannot be resolved to URL because it does not exist");
		}
		return url;
	}
	
	public static InputStream getResourceAsStream(String path) throws FileNotFoundException {
		InputStream is = getDefaultClassLoader().getResourceAsStream(path);
		if (is == null) {
			throw new FileNotFoundException("class path resource [" + path + "] cannot be resolved to URL because it does not exist");
		}
		return is;
	}
	
}
