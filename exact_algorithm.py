import findspark
findspark.init()
from pyspark import SparkContext
from pyspark import SparkConf
import math
import os
import random as rand

def read_file(file_name):
    points = []
    with open(file_name, 'r') as file:
        for line in file:
            x, y = line.split(',') 
            points.append((float(x), float(y))) 
    return points

def ExactOutliers(points, D, M, K):
    
    distances = [0]*len(points) #Array to store the number of points with a distance less than D
    total_points = len(points)
    outliers_num = 0
    outliers = []
    
    def distance_p2p(point_a,point_b):
        total_squares=0
        for i in range(len(point_a)):
            total_squares=total_squares+(pow(abs(point_a[i]-point_b[i]),2))
        return math.sqrt(total_squares)            
    
    for i in range(len(points)):
        for j in range(i+1,len(points)):
            if distance_p2p(points[i],points[j])<D:
                distances[i]+=1
                distances[j]+=1
    
    for i in range(len(distances)):
        #print(distances[i])
        if distances[i]<M:
            outliers_num+=1
            outliers.append(points[i])

    
    print("Number of points: ", total_points)
    print("Number of Outliers: ", outliers_num)
    for i in range(K):
        if i<len(outliers):
            print(outliers[i])
         
    

    
def main():
    ExactOutliers(read_file("TestN15-input.txt"),1,3,12)
    

if __name__ == "__main__":
    main()