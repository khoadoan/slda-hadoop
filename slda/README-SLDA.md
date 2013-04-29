SLDA
================

**KHOA: The current description is for text data. This will change later; for now, this is for testing the runnability of SLDA**

Install and Build
----------
To download all the dependency packages, please run the following command (assuming you are on the directory of the project)
    ant

Tokenizing and Indexing
----------
To tokenize, parse and index the raw text file, please run either the following command

	etc/hadoop-local.sh cc.slda.ParseCorpus -input /path_to_corpus.txt -output /path_to_output
	etc/hadoop-cluster.sh cc.slda.ParseCorpus -input /path_to_corpus.txt -output /path_to_output -mapper 10 -reducer 5
	*Note* Remember to set the number of mappers and reducers for small cluster(defaults are 100).

To print the help information and usage hints, please run the following command

    etc/hadoop-cluster cc.slda.ParseCorpus -help

By the end of execution, you will end up with three files/dirtories in the specified output, for example,

    hadoop fs -ls /hadoop/index/document/output/directory/
    Found 3 items
    drwxr-xr-x   - user supergroup          0 2012-01-12 12:18 /hadoop/index/document/output/directory/document
    -rw-r--r--   3 user supergroup        282 2012-01-12 12:18 /hadoop/index/document/output/directory/term
    -rw-r--r--   3 user supergroup        189 2012-01-12 12:18 /hadoop/index/document/output/directory/title

File `/hadoop/index/document/output/directory/term` stores the mapping between a unique token and its unique integer ID. Similarly, `/hadoop/index/document/output/directory/title` stores the mapping between a document title to its unique integer ID. Both of these two files are in sequence file format, key-ed by `IntWritable.java` and value-d by `Text.java`. You may use the following command to browse a sequence file in general

     hadoop jar Mr.LDA.jar edu.umd.cloud9.io.ReadSequenceFile /hadoop/index/document/output/directory/term
     hadoop jar Mr.LDA.jar edu.umd.cloud9.io.ReadSequenceFile /hadoop/index/document/output/directory/term 20

and option '20' specifies the first 20 records to be displayed.