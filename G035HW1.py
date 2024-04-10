import findspark
findspark.init()
from pyspark import SparkContext
from pyspark import SparkConf
import math
import os
import sys
import time
import random as rand

def read_points(line):
    x, y = map(float, line.strip().split(',')) # Split each line by comma and convert to float
    return (x, y)

def MRApproxOutliers(points_rdd, D, M, K):

        # Step A: Transform input RDD into RDD with non-empty cells and their point counts
        def map_to_cells(point):
            x, y = point
            i = int(x / (D / (2 * math.sqrt(2)))) # Calculate i coordinate of the cell
            j = int(y / (D / (2 * math.sqrt(2)))) # Calculate j coordinate of the cell
            return ((i, j), 1)  # Emit cell identifier with count 1

        def reduce_to_points(a, b):
            return a + b

        # Transform points RDD into an RDD of cell counts
        cells_rdd = points_rdd.map(map_to_cells).reduceByKey(reduce_to_points)

        # Collect non-empty cells to the driver
        collected_cells = cells_rdd.collect()
        
        # Step B: Determine outliers
        num_sure_outliers = 0
        num_uncertain_points = 0
        outliers = []

        for cell in collected_cells:
            (i, j), points = cell

            # Define the region R3(Cp) and R7(Cp)# Define the region R3(Cp) and R7(Cp) with positive coordinates
            region_7 = [(i + dx, j + dy) for dx in range(-3, 4) for dy in range(-3, 4)]  # Region R7(Cp)
            region_3 = [(i + dx, j + dy) for dx in range(-1, 2) for dy in range(-1, 2)]  # Region R3(Cp)

            # Calculate N3(Cp) and N7(Cp) based on the points within the cell
            N3 = sum(cell[1] for cell in collected_cells if cell[0] in region_3)  # Number of points in R3(Cp)∩S
            N7 = sum(cell[1] for cell in collected_cells if cell[0] in region_7)  # Number of points in R7(Cp)∩S

            # Determine sure outliers, uncertain points, and outliers
            if N7<=M :
                num_sure_outliers += points
            else:
                if N3<=M :
                    num_uncertain_points += points


        # Sort cells by size in non-decreasing order and take the first K cells
        sorted_cells = cells_rdd.sortBy(lambda x: x[1], ascending=True).take(K)
        print("Number of sure outliers =", num_sure_outliers)
        print("Number of uncertain points =", num_uncertain_points)
        for cell in sorted_cells:
            print("Cell:", cell[0], "Size =", cell[1])


def ExactOutliers(points, D, M, K):
    
    points_inside_radius = [0]*len(points) #Array to store the number of points with a distance less than D
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

    print("Number of outliers =", outliers_num)
    for i in range(K):
        if i<len(sorted_outliers):
            print("Point:",sorted_outliers[i][0])
         
    

    # Main function
def main():    

    # CMD input
    assert len(sys.argv) >= 4, "Usage: python MRApproxOutliers.py <file_name> [D] [M] [K] [L]"

    # Spark setup
    conf = SparkConf().setAppName('MRApproxOutliers')
    sc = SparkContext(conf=conf)

    # Read number of partitions
    file_name = sys.argv[1]
    D = float(sys.argv[2]) if len(sys.argv) > 3 else 1
    M = int(sys.argv[3]) if len(sys.argv) > 4 else 3
    K = int(sys.argv[4])
    L = int(sys.argv[5])
    print("File name:",file_name,"  D:",D,"  M:",M,"  K:",K,"  L:",L)
    
    # Read input file and subdivide it into L random partitions
    data_path = file_name
    assert os.path.isfile(data_path), "File or folder not found"
    rawData = sc.textFile(data_path)
    inputPoints = rawData.map(read_points).repartition(L)
    
    # Total number of points
    points_num=inputPoints.count()
    print("Number of points =",points_num)
    
    # Exwcute exact algorithm if points are less than 200000
    if points_num<200000:
        plain_points=inputPoints.collect()
        start_time=time.time()
        ExactOutliers(plain_points, D, M, K)
        final_time=time.time()
        print("Running time of Exact outliers:",round((final_time-start_time)*1000), " ms") 
         
    start_time=time.time()
    MRApproxOutliers(inputPoints, D, M, K)
    final_time=time.time()
    print("Running time of Approximate outliers:",round((final_time-start_time)*1000), " ms") 

if __name__ == "__main__":
    main()
    