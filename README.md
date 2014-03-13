HadoopFileDiff
==============

HadoopFileDiff an application that is used to perform the distributed unified diff operation with the help of Apache Hadoop on a cluster. The purpose of this tool is used to find the difference between corresponding files in 2 different versions of the same software project repostiory. The output obtained is a patch file for each matched file, indicating the modifications (code insertions and deletions) line by line. This tool can be deployed using Hadoop 1.0.3 and other compatible versions.

Implementation
==============

This application does the directory traversal of the source code of both the versions, looking for matching file. Both the repositories should be uploaded on the HDFS.


For example:-

Let the 2 repositories be stored in the following paths on the HDFS:-

hdfs://localhost:50070/user/testuser/FileDiff/repo-1
and
hdfs://localhost:50070/user/testuser/FileDiff/repo-2

Now let us say there is a Java source code file (NetworkConnector.java) for both thte repositories, located in the following paths:-

hdfs://localhost:50070/user/testuser/FileDiff/repo-1/src/org/projects/network/NetworkConnector.java
and
hdfs://localhost:50070/user/testuser/FileDiff/repo-2/src/org/projects/network/NetworkConnector.java

Obviously both files are the same that could have been modified between the 2 versions. This is one of the candidate files for comparison and this tool performs the comparison by associating the lines of both the files to the key:

src/org/projects/NetworkConnector.java which is the path of the file from the repository root.

In this way the tool identifies all the candidate comparison files, and finally generates the required output patch files, named by this key.

Deployment & Execution
======================

First, the source code needs to be packaged into jar with all the hadoop dependencies, in order to be deployed by hadoop.

This is done with the help of Apache Maven. Run the following commands from the root directory of this project (after installing maven):

% mvn -f m2-pom.xml clean
% mvn -f m2-pom.xml install
% mvn -f m2-pom.xml package

2 jar packages, one with the dependencies and the other without, are created in the target/ directory.

Start all the hadoop processes (having installed Apache Hadoop and setting the environment variables)

% $HADOOP_PREFIX/bin/start-all.sh

The HDFS namenode process will also be launched. Download the repositories on which the tool should be used.

Copy the repositories to the HDFS.

% bin/hadoop dfs -copyFromLocal <path_to_older_repo_version> <any_path_on_hdfs>/repo-1
% bin/hadoop dfs -copyFromLocal <path_to_newer_repo_version> <any_path_on_hdfs>/repo-2

Once the upload is complete, the jar package of this tool can be launched on hadoop

% bin/hadoop -jar FileDiff-jar-with-dependencies.jar <root_directory_of_the_repositories_on_hdfs> <output_directory_on_hdfs>

Note:- The root directory of the repositories is the one directly above repo-1 and repo-2 directories.

After the execution, the output files can be downloaded back onto the local filesystem.

% bin/hadoop dfs -copyToLocal <output_directory_on_hdfs> <path_on_local_filesystem>
