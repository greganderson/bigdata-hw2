# bigdata-hw2

To get instructions on how to run these files, run them without any parameters
and the script will help you know how to run the particular file.

Part 1:

To run:
	- Use the scrub_vector.py script on any vector file to produce the correctly
	formatted output file.
	- Run spark-submit with the correct parameters.  For example:

>$ spark-submit part1.py matrices/a_100x200.txt 100x200 matrices/b_200x100.txt 200x100

For the part1 matrix and vector multiplication, we went with the one pass
approach to solving the problem. When we read the file we decided to group i, j,
and value together in the following format (i, j, value), i and j as integers
and value as floats.

Then for each value of the matrices we created copies and mapped them to their
correct position in the resulting matrix. After, we took the corresponding
values, and joined them together so that we could apply the dot product to each
value of the resulting matrix.

Part 2:

To run:
	- Run spark-submit with the correct parameters.  For example:

>$ spark-submit part2.py graphs/Assign2_100.txt 100x100

To find out if a graph is a shallow graph we needed to compute A^2 + A (A
representing a matrix). Since we already had the matrix multiplication sorted
out from part1, we re-used that code to compute A*A. Then we added the result to
the original matrix. However, to determine if a graph is shallow we need to
check our results to make sure the graph doesn't contain any 0s. So to do that
we used a filter to see if we found any 0s in there.  If there were, we said
that the graph was not shallow, otherwise shallow.
