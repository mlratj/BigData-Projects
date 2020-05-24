import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Preparation {
    //hdfs
    static Configuration config = null;
    static FileSystem dfs = null;
    //local
    static Configuration conf = null;
    // static FileSystem localFileSystem = null;

    public Preparation() throws IOException {
        //hdfs conf
        config = new Configuration();
        config.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
        config.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
        config.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        config.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName());
        dfs = FileSystem.get(config);
    }

    public static Configuration getConfiguration() throws IOException {
        //hdfs conf
        config = new Configuration();
        config.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
        config.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));
        config.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        config.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName());
        return config;
    }

    public static void main(String[] project_name) throws IOException {
        new Preparation();
        Scanner user_input = new Scanner(System.in);

        deleteDir(project_name[0] + "/input/");
        deleteDir(project_name[0] + "/output/");
        createDir(project_name[0] + "/input/");
        System.out.println("Provide names of files to upload: ");
        String files_names = user_input.next();
        copyFromLocal(files_names, project_name[0] + "/input/");
        System.out.println("Preparation phase is finished!");
    }

    public static void createDir(String directoryName) throws IOException {
        System.out.println(dfs.getWorkingDirectory());
        if (dfs.exists(new Path(directoryName))) {
            System.out.println("The directory already exists - " + directoryName );
        } else {
            Path src = new Path(directoryName);
            dfs.mkdirs(src);
            System.out.println("Directory created - " + directoryName );
        }
    }

    public static void deleteDir(String directoryName) throws IOException {
        if (dfs.exists(new Path(directoryName))) {
            dfs.delete(new Path(directoryName), true);
            System.out.println("Directory deleted - " + directoryName );
        } else{
            System.out.println("The directory doesn't' exist - " + directoryName );
        }
    }

    public static boolean copyFromLocal(String files, String location) {
        boolean success = false;
        System.out.println("Executing BASH commands...");
        Runtime r = Runtime.getRuntime();
        String[] commands = {"bash", "-c", "hadoop fs -copyFromLocal " + files + " " + location};
        try {
            Process p = r.exec(commands);

            p.waitFor();
            BufferedReader b = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = "";

            while ((line = b.readLine()) != null) {
                System.out.println(line);
            }

            b.close();
            success = true;
        } catch (Exception e) {
            System.err.println("Failed to execute bash with provided files or location.");
            e.printStackTrace();
        }
        return success;
    }
}