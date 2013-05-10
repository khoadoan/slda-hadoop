#!/usr/env python

import os
import re
import sys
import gzip

def process(line, path):
	arg = line.split('\t')
	image_id = int(arg[0])
	pat = re.compile(r'\(\d+, \d+\)=\d+')
	image = re.findall(pat, arg[1])
	filename = path + '/' + arg[0] + '.data'
	f = open(filename, 'w')
	for t in image:
		x = re.findall(re.compile('\d+'), t)
		f.write('\t'.join(x) + '\n')
	f.close()

def main(gzpath, folder):
	if not os.path.exists(folder):
		os.makedirs(folder)
	f = gzip.open(gzpath, 'rb')
	content = f.read()
	f.close()
	line = content.split('\n')
	for row in line:
		process(row, folder)

if __name__ == "__main__":
	if len(sys.argv) < 3:
		gzpath = '../data/msrc-topic-14.out.gz'
		folder = 'msrc-topic-14'
	else:
		gzpath = sys.argv[1]
		folder = sys.argv[2]
	main(gzpath, folder)
