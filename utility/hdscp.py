#!/usr/bin/env python

import os
import sys
from os import listdir
from os.path import isfile
from os import walk

'''
SCP from a local directory to an HDFS directory on clusters

Command-line:
	./hdscp <local-directory> <username>@<cluster-url> <hdfs-directory>

History:
	Created by Ang Li, 22 APR 2013
'''

def copy(local, cluster, hdfs):
	cmd = ["cat", local, "|", "ssh", cluster, "'hadoop fs -put -", hdfs, "'"]
	print ' '.join(cmd)
	os.system(" ".join(cmd))

def scp(localpath, cluster, hdfspath):
	print localpath, cluster, hdfspath
	for entry in listdir(localpath):
		currentpath = os.path.join(localpath, entry);
		if os.path.isdir(currentpath):
			scp(currentpath, cluster, os.path.join(hdfspath, entry))
		elif os.path.isfile(currentpath):
			copy(currentpath, cluster, os.path.join(hdfspath, entry))

def usage():
	print 'Usage:'
	print './hdscp <local-directory> <username>@<cluster-url> <hdfs-directory>'

if __name__ == "__main__":
	if len(sys.argv) != 4:
		usage()
	else:
		scp(sys.argv[1], sys.argv[2], sys.argv[3])
