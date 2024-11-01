# gtfs2fuwinfo
-----------------------------

# First time using it 

1.
conda env create -f environment.yml

2. Dependencies should be in /env directory
conda activate ./env

-----------------------------
# After adding library 
conda env update --prefix ./env --file environment.yml --prune

-----------------------------
# To deactivate
conda deactivate