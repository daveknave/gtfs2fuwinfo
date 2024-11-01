# gtfs2fuwinfo
-----------------------------

# First time using it 

1. conda env create -f environment.yml


2. conda activate ./env

(Dependencies should be in /env directory)

-----------------------------
# After adding library 
conda env update --prefix ./env --file environment.yml --prune

-----------------------------
# To deactivate
conda deactivate