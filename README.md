# Get started

## Graph based recommender system
The recommender system uses an Neo4j graph database to store the data and do predictions in a graph based manner.
If you want to run the entire system you need to download active1000.zip from black-board, extract the directory on root, uncomment all code in `graph_based_db.ipynb` and run in. NB importing the data will use ca 8 hours and doing all popularity prediction will use ca 12 hours.

Since predictions on all users takes a lot of time, the predictions are stored in .feather files. To run the evaluation, just run `graph_based_db.ipynb` without uncommenting anything.

The file `info.log` prints the progress while running the code.