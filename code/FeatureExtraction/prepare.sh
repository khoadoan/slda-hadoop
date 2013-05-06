#!/bin/bash
hadoop fs -ls /user/ubuntu/ang/MSRCv1/data/*s.bmp | awk 'BEGIN{cnt=0} $8!=""{++cnt;print cnt"\t"$8}' > MSRCv1.input
hadoop fs -put MSRCv1.input ang/MSRCv1/input.path
