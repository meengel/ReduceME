# ReduceME
A small tool for parallelized reducing method in Python. It is built for tasks where the reducing of two items is very costly already. For these cases, this tool results in a significant performance increase! Have fun and ask questions if necessary!

Please note that we also provide the possibility to reduce more than two items in one iteration. That is, you can use M-ary trees for reduction. This enables the user to either sum up a vast amount of numbers or to merge multiple files (i.e. Images, Raster, Polygons) in one step, for example. However, the reducing methods have to be implemented such that they accept 2 to M items. We recommend using a signature like fun(*items) or similar.

# Installation
Should work with standard Python installation as it is based on builtins and standard libraries only:
- contextlib
- math
- multiprocessing
- queue
- sys

The TEST-script imports the time package as well.