
Adjusting block size
-----------------------
From the doc page of the library: "The SIFT-descriptor consists of n×n gradient histograms, each from a 4×4px block. n is this value." The value n is fdSize in the java code inside the FloatArray2DSIFT class. Since it is public we can set the value directly before calling the init method. The default value is 4, resulting the feature vector size 4x4x8 (assuming the bins size is 8). If the value is set to 2, the feature vector size is 2x2x8.

In the modified code of ExtractDenseSIFTFromImage, we can pass the descriptor size in the constructor. For example,
if we want to have the features where the step size is 8 and the descriptor size is 2, we can just write new ExtractDenseSIFTFromImage(stream, 8, 2).
