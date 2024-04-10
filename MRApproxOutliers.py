import os
import sys
import math
import time
import findspark
findspark.init()        
from pyspark import SparkContext  
from pyspark import SparkConf

# Function to read points from a file
def read_points(file_name):
    points = []
    with open(file_name, 'r') as file:
        for line in file:
            x, y = map(float, line.strip().split(',')) # Split each line by comma and convert to float
            points.append((x, y)) # Add the point to the list
    return points

def MRApproxOutliers(points_rdd, D, M, K):
        start_time = time.time() # Record start time

        # Step A: Transform input RDD into RDD with non-empty cells and their point counts
        def map_to_cells(point):
            x, y = point
            i = int(x / (D / (2 * math.sqrt(2)))) # Calculate i coordinate of the cell
            j = int(y / (D / (2 * math.sqrt(2)))) # Calculate j coordinate of the cell
            return ((i, j), 1)  # Emit cell identifier with count 1

        def reduce_to_points_within_partition(iterator):
            cell_points = {}
            for point in iterator:
                cell = point[0]  # Get the cell identifier for the point
                if cell in cell_points:
                    cell_points[cell] += 1
                else:
                    cell_points[cell] = 1
            return cell_points.items()

        def reduce_to_points(a, b):
            return a + b

        # Transform points RDD into an RDD of cell counts
        cells_rdd = points_rdd.map(map_to_cells).reduceByKey(reduce_to_points)

        # Collect non-empty cells to the driver
        collected_cells = cells_rdd.collect()
        
        #print(collected_cells)
        # Step B: Determine outliers
        num_sure_outliers = 0
        num_uncertain_points = 0
        outliers = []

        for cell in collected_cells:
            (i, j), points = cell
            #print(cell)

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

            #print(N7)
            #print(num_sure_outliers)


        # Sort cells by size in non-decreasing order and take the first K cells
        sorted_cells = cells_rdd.sortBy(lambda x: x[1], ascending=True).take(K)

        end_time = time.time()
        running_time = int((end_time - start_time) * 1000)  # Convert to milliseconds

        return num_sure_outliers, num_uncertain_points, sorted_cells, running_time
    
# Main function
def main():    

    # CHECKING NUMBER OF CMD LINE PARAMETERS
    assert len(sys.argv) >= 3, "Usage: python MRApproxOutliers.py <K> <file_name> [D] [M]"

    # SPARK SETUP
    conf = SparkConf().setAppName('MRApproxOutliers')
    sc = SparkContext(conf=conf)

    # INPUT READING

    # 1. Read number of partitions
    file_name = sys.argv[2]
    K = int(sys.argv[1])
    D = float(sys.argv[3]) if len(sys.argv) > 3 else 1
    M = int(sys.argv[4]) if len(sys.argv) > 4 else 3

    # 2. Read input file and subdivide it into K random partitions
    data_path = sys.argv[2]
    assert os.path.isfile(data_path), "File or folder not found"
    docs = sc.textFile(data_path, minPartitions=K).repartition(numPartitions=K).cache()

    try:
        points = read_points(file_name) # Read points from the input file
        points_rd = sc.parallelize(points)  # Convert points to RDD

        # Call the function to determine outliers
        num_sure_outliers, num_uncertain_points, sorted_cells, running_time = MRApproxOutliers(points_rd, D, M, K)

        # Print the results
        print("Number of sure outliers =", num_sure_outliers)
        print("Number of uncertain points =", num_uncertain_points)
        for cell in sorted_cells:
            print("Cell:", cell[0], "Size =", cell[1])
        print("Running time of MRApproxOutliers =", running_time, "ms")

    finally:
        sc.stop()  # Stop SparkContext

if __name__ == "__main__":
    main()