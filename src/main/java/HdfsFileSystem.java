import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

import java.sql.Timestamp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

public class HdfsFileSystem {
    private final FileSystem rootFs;
    private final String rootPath;
    private final String user;

    public static final int EXISTS = 1;
    public static final int NOCREATE = -1;
    public static final int NOEXISTS = 0;

    public HdfsFileSystem(String rootPath, String user) throws URISyntaxException, IOException, InterruptedException {
        this.rootPath = rootPath;
        this.user = user;
        this.rootFs = FileSystem.get(new URI(rootPath), new Configuration(), user);
    }

    // 新建目录
    public boolean createDir(String path) throws FileNotFoundException, FileAlreadyExistsException {
        boolean flag = false;
        int status = isPathExists(path);

        if (status == NOCREATE) {
            try {
                flag = this.rootFs.mkdirs(new Path(path));
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            if (status == NOEXISTS)
                throw new FileNotFoundException();
            else
                throw new FileAlreadyExistsException();
        }

        return flag;
    }

    // 新建文本文件
    public boolean createFile(String filePath, String text) throws FileNotFoundException, FileAlreadyExistsException {
        boolean flag = false;
        int status = isPathExists(filePath);

        if (status == NOCREATE) {
            try {
                FSDataOutputStream new_file = this.rootFs.create(new Path(filePath));
                // 写入文本内容并刷新缓冲区提交
                new_file.writeUTF(text);
                new_file.flush();
                new_file.close();
                flag = true;
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            if (status == NOEXISTS)
                throw new FileNotFoundException();
            else
                throw new FileAlreadyExistsException();
        }

        return flag;
    }

    // 删除文件/目录
    public boolean deleteFile(String delPath) throws FileNotFoundException {
        boolean flag = false;
        int status = isPathExists(delPath);

        if (status == EXISTS) {
            try {
                // 第二个boolean参数选择是否递归删除（用于目录）
                flag = this.rootFs.delete(new Path(delPath), true);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            throw new FileNotFoundException();
        }

        return flag;
    }

    // 上传文件
    public boolean uploadFile(String srcPath, String targetPath, boolean isForceCopy) throws FileNotFoundException, FileAlreadyExistsException {
        boolean flag = true;

        boolean src_status = isLocalPathExists(srcPath);
        int tar_status = isPathExists(targetPath);

        if (src_status && (tar_status == NOCREATE || (tar_status == EXISTS && isForceCopy))) {
            try {
                this.rootFs.copyFromLocalFile(new Path(srcPath), new Path(targetPath));
            } catch (IOException e) {
                e.printStackTrace();
                flag = false;
            }
        } else {
            if (!src_status || tar_status == NOEXISTS)
                throw new FileNotFoundException();
            if (tar_status == EXISTS)
                throw new FileAlreadyExistsException();
        }

        return flag;
    }

    // 下载文件
    public boolean downloadFile(String srcPath, String targetPath, boolean isForceCopy) throws FileNotFoundException, FileAlreadyExistsException {
        boolean flag = true;

        int src_status = isPathExists(srcPath);
        boolean tar_status = isLocalPathExists(targetPath);

        if (src_status == EXISTS && (!tar_status || isForceCopy)) {
            try {
                this.rootFs.copyToLocalFile(new Path(srcPath), new Path(targetPath));
            } catch (IOException e) {
                e.printStackTrace();
                flag = false;
            }
        } else {
            if (tar_status && !isForceCopy)
                throw new FileAlreadyExistsException();
            else
                throw new FileNotFoundException();
        }

        return flag;
    }

    // 显示指定目录下内容
    public void listDirFile(String path) throws FileNotFoundException {
        // 构造指定目录路径
        String filePath = this.rootPath + path;
        int status = isPathExists(path);

        if (status == EXISTS) {
            try {
                // 新建指定目录下的HDFS对象
                FileSystem pathContent = FileSystem.get(new URI(filePath), new Configuration(), this.user);
                FileStatus pathStatus = pathContent.getFileStatus(new Path(filePath));

                // 输出指定路径目录下内容
                System.out.println("Path: " + pathStatus.getPath());
                for (FileStatus fs : pathContent.listStatus(new Path(filePath)))
                    System.out.printf("%s %s %s %s %s\n",
                            fs.getPermission(),
                            pathStatus.getOwner(),
                            pathStatus.getGroup(),
                            new Timestamp(pathStatus.getModificationTime()),
                            fs.getPath().toString().replace(this.rootPath, "")
                    );

            } catch (IOException | URISyntaxException | InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            throw new FileNotFoundException();
        }
    }

    // 查看文本文件内容
    public void viewTextFile(String file_path) throws FileNotFoundException {
        int status = isPathExists(file_path);

        if (status == EXISTS) {
            try {
                FSDataInputStream fileInput = this.rootFs.open(new Path(file_path));
                IOUtils.copyBytes(fileInput, System.out, 1024);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            throw new FileNotFoundException();
        }
    }

    // 判断路径是否存在
    public boolean isLocalPathExists(String path) {
        return new File(path).exists();
    }

    public int isPathExists(String path) {
        int flag = NOEXISTS;
        String testPath = this.rootPath + path;
        try {
            FileSystem fs = FileSystem.get(new URI(testPath), new Configuration(), this.user);
            flag = fs.exists(new Path(testPath)) ? EXISTS : NOCREATE;
        } catch (IllegalArgumentException | URISyntaxException | IOException | InterruptedException e) {
            e.printStackTrace();
        }

        return flag;
    }

}