import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;

import static org.junit.jupiter.api.Assertions.*;


public class HdfsFileSystemTest {
    private static final int EXISTS = 1;
    private static final int NOCREATE = -1;
    private static final int NOEXISTS = 0;

    private final String rootPath = "hdfs://localhost:9000";
    private final String user = "root";
    private final HdfsFileSystem test = new HdfsFileSystem(rootPath, user);


    public HdfsFileSystemTest() throws URISyntaxException, IOException, InterruptedException {
    }

    @Test
    public void createDir() throws FileAlreadyExistsException, FileNotFoundException {
        // ok
        assertTrue(test.createDir("/test"));

        // exception
        assertFalse(test.createDir("test"));    // not found
        assertFalse(test.createDir(""));        // not found
        assertFalse(test.createDir("/test"));   // exists
    }

    @Test
    public void createFile() throws FileAlreadyExistsException, FileNotFoundException {
        // ok
        assertTrue(test.createFile("/test/hello_api.txt", "hello java api\n"));

        // exception
        assertFalse(test.createFile("test/hello_api.txt", "hello java api\n"));     // not found
        assertFalse(test.createFile("", "hello java api\n"));                       // not found
        assertFalse(test.createFile("/test/hello_api.txt", "hello java api\n"));    // exists

    }

    @Test
    public void deleteFile() throws FileNotFoundException {
        // ok
        assertTrue(test.deleteFile("/test/hello_api.txt"));

        // exception
        assertFalse(test.deleteFile("/test/hello_api.txt"));    // not found
        assertFalse(test.deleteFile(""));                       // not found
    }

    @Test
    public void uploadFile() throws FileAlreadyExistsException, FileNotFoundException {
        // ok
        assertTrue(test.uploadFile("upload.txt", "/upload.txt", true));

        // exception
        assertFalse(test.uploadFile("upload.txt", "/upload.txt", false));   // not found
        assertFalse(test.uploadFile("", "/upload.txt", false));             // exists
    }

    @Test
    public void downloadFile() throws FileAlreadyExistsException, FileNotFoundException {
        // ok
        assertTrue(test.downloadFile("/upload.txt", "download.txt", true));

        // exception
        assertFalse(test.downloadFile("upload.txt", "download.txt", false));    // not found
        assertFalse(test.downloadFile("/upload.txt", "", false));               // not found
        assertFalse(test.downloadFile("", "download.txt", false));              // not found
        assertFalse(test.downloadFile("/upload.txt", "download.txt", false));   // exists
    }

    @Test
    public void listDirFile() throws FileNotFoundException {
        test.listDirFile("/");
    }

    @Test
    public void viewTextFile() throws FileNotFoundException {
        test.viewTextFile("/upload.txt");
    }

    @Test
    public void isFileExists() throws Exception {
        // reflect invoke
        Method test_method = this.test.getClass().getDeclaredMethod("isPathExists", String.class);
        test_method.setAccessible(true);

        // ok
        assertEquals(EXISTS, (int) test_method.invoke(this.test, "/test"));
        assertEquals(NOCREATE, (int) test_method.invoke(this.test, "/test/incorrect"));
        // exception
        assertEquals(NOEXISTS, (int) test_method.invoke(this.test, "test/incorrect"));
        assertEquals(NOEXISTS, (int) test_method.invoke(this.test, ""));
    }
}