# s3-dist-cp
hacked s3-dist-cp with no compress/decompress function to improve performance

s3-dist-cp will automatically decompress and compress files with supported extention ( like gzip lzo etc. ) 

- CopyFilesReducer.java source code with remarked compress/decompress method
- CopyFilesReducer.class class file compiled on AWS EMR
- s3-dist-cp-2.19.0.jar packed jar to replace the original s3-dist-cp
