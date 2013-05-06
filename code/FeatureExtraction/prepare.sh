#!/bin/bash
hadoop fs -ls /user/ubuntu/ang/data/MSRCv1/*s.bmp | awk 'BEGIN{cnt=0} $8!=""{++cnt;print cnt"\t"$8}' > MSRCv1.input

