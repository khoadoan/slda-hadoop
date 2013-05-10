#!/usr/env python

import os
from PIL import Image

def main(path, output, dataset, origin):
	if not os.path.exists(origin):
		os.makedirs(origin)
	f = open(path, "r")
	fout = open(output, "w")
	for row in f:
		rows = row.strip().split()
		imageid = rows[0]
		filename = os.path.basename(rows[1])
		im = Image.open(dataset + "/" + filename)
		im.save(origin + "/" + imageid + ".bmp", "BMP")
		fout.write('%s\t%s\t%d\t%d\n' % (imageid, filename, im.size[0], im.size[1]))
		print imageid, filename, im.size[0], im.size[1]
	fout.close()

if __name__ == "__main__":
	path = "../data/MSRCv1.input"
	output = "MSRCv1.data"
	dataset = "../MSRCv1"
	origin = "origin"
	main(path, output, dataset, origin)
