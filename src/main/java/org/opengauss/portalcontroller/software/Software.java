package org.opengauss.portalcontroller.software;

import java.util.ArrayList;
import java.util.Hashtable;

/**
 * The interface Software.
 */
public interface Software {
    /**
     * Init critical file list array list.
     *
     * @return the array list
     */
    ArrayList<String> initCriticalFileList();

    /**
     * Init parameter hashtable hashtable.
     *
     * @return the hashtable
     */
    Hashtable<String, String> initParameterHashtable();

    /**
     * Download package.
     */
    void downloadPackage();

    /**
     * Install.
     *
     * @param download the download
     */
    void install(boolean download);
}
