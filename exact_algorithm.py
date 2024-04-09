import findspark
findspark.init()
from pyspark import SparkContext
from pyspark import SparkConf
import math
import os
import time
import random as rand

def read_file(file_name):
    points = []
    with open(file_name, 'r') as file:
        for line in file:
            x, y = line.split(',') 
            points.append((float(x), float(y))) 
    return points

def ExactOutliers(points, D, M, K):
    
    points_inside_radius = [0]*len(points) #Array to store the number of points with a distance less than D
    total_points = len(points)
    outliers_num = 0
    outliers = []
    Dis = D*D #optimization of computational time by not using sqrt
    
    def distance_p2p(point_a,point_b): #calculates the squared distance between 2 points 
        total_squares=0
        for i in range(len(point_a)):
            total_squares=total_squares+(pow(point_a[i]-point_b[i],2))
        return total_squares           
    
    for i in range(len(points)): #method performing  a maximum of N^2 steps, it works well with a small number of outliers
        for j in range(len(points)):
            if distance_p2p(points[i],points[j])<Dis and i!=j:
                points_inside_radius[i]+=1
            if points_inside_radius[i]>M:
                break
    #another method with N*(N-1)/2 steps that works better with high numbers of outliers
    """
        for j in range(i+1,len(points)):
            if distance_p2p(points[i],points[j])<Dis:
                points_inside_radius[i]+=1
                points_inside_radius[j]+=1
        another method with N^2 max steps but working better with low number of outliers
    """               

    for i in range(len(points_inside_radius)): #calculates outliers number
        #print(distances[i])
        if points_inside_radius[i]<M:
            outliers_num+=1
            outliers.append([points[i],points_inside_radius[i]])
    sorted_outliers = sorted(outliers, key=lambda x: x[1])

    
    print("Number of points: ", total_points)
    print("Number of Outliers: ", outliers_num)
    for i in range(K):
        if i<len(sorted_outliers):
            print(sorted_outliers[i][0])
         
    

    
def main():
    ExactOutliers(read_file("uber-100k.csv"),0.02,10,5)
    

if __name__ == "__main__":
    start_time=time.time()
    main()
    final_time=time.time()
    print("Computation time: ",round((final_time-start_time)*1000), " ms")