#!/usr/bin/python

import sys
import os
import numpy as np

filename = sys.argv[1]
numClusters = sys.argv[2]
h_distrCache = "kmeans/"
centroidFile="centroids.txt"
newCentroidFile="centroids_new.txt"
medicare_inputDir = "medicare_input/"
medicare_outputDir = "medicare_out/"
provider_list = ["Clinical Laboratory", "Internal Medicine", "Dermatology", "Cardiology", "Ambulatory Surgical Center"]
pig_filename = "normalize.pig"
pig_out = "medicare_scaled/"
h_outputDir = "kmeans_output"

'''
os.system("hdfs dfs -rm " + medicare_inputDir + filename)
os.system("hdfs dfs -put " + filename + " " + medicare_inputDir)
'''
# Part 1 : Pre-processing
for provider_type in provider_list:
    os.system("hdfs dfs -rm -r " + medicare_outputDir)
    os.system("hadoop jar Preprocessor.jar " + "\"" + provider_type + "\" " + medicare_inputDir + " " + medicare_outputDir)
 
    os.system("hdfs dfs -getmerge " + medicare_outputDir + "  medicare_sub.txt")
    
# Part 2 : Normalize
    os.system("hdfs dfs -rm -r " + pig_out)
    os.system("hdfs dfs -rm " + medicare_inputDir + "medicare_sub.txt")
    os.system("hdfs dfs -put medicare_sub.txt " + medicare_inputDir)
    os.system("pig " + pig_filename)
    os.system("hdfs dfs -getmerge " + pig_out + " medicare_sub.txt")   

# Part 3: K-means
    os.system("shuf medicare_sub.txt | head -n " + numClusters + " | nl > " + centroidFile)
    os.system("sed -i 's/^ *//' " + centroidFile)
    
    os.system("hdfs dfs -rm " + h_distrCache + centroidFile)
    os.system("hdfs dfs -put " + centroidFile + " " + h_distrCache)
 
    for i in range(1,10):
        print("Iteration: ", i)
        os.system("hdfs dfs -rm -r " + h_outputDir)
        os.system("hadoop jar Kmeans.jar " + pig_out + " " + h_outputDir)
        os.system("hdfs dfs -getmerge " + h_outputDir + " " + newCentroidFile);
        
        # Add distance measure for error bound check
        centroids_old = np.loadtxt(centroidFile)
        centroids_new = np.loadtxt(newCentroidFile)
        distance = np.linalg.norm(centroids_new - centroids_old)
        os.system("mv " + newCentroidFile + " " + centroidFile)
        if (distance<0.01):
            print("Converged. Exiting...")
            break;
        
        os.system("hdfs dfs -rm " + h_distrCache + centroidFile)
        os.system("hdfs dfs -put " + centroidFile + " " + h_distrCache) 
    os.system("mv " + centroidFile + " " + "centroids" + "_" + provider_type.replace(" ", "_"))
