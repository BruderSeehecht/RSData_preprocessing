# RSData_preprocessing
Short workflow for resampling, renaming and mosaicking RS raster data

1. rename.py 
The script ensures that the downloaded data with complex names can be processed easier. A Sentinel folder is downloaded for each image, which is named with a variety of information (e.g. orbit, date, processing pipeline, etc.). The length of the names alone makes it difficult to process the data, so this script renames the folders variably. 

This is not absolutely necessary for the other preprocessing steps, but can also be used for any other data.

2. resampling.py 
The script iterates through the folder searches for the desired bands. Depending on the target resolution, available bands in that resolution are retained and all other are resampled. The bands in jp2 format are resmpled as GeoTIFFs and stacked together to a multiband GeoTIFF afterwards.

All images of the different bands must have the exact same geometrics and must be a GeoTIFF file to prevent errors while concatenation. Therefore, the CRS, the width and the height of the first processed band is copied and assigned to all further bands. The script ensures that the resampled images are saved as GeoTIFFs. 

It is possible to accelerate the resampling via parallelisation depending on the available threads of the CPU and the RAM memory size. Activated parallelisation yields in parallel resampling of the bands inside a folder. The most efficient way is to set max_workers = desired output bands. 

It is highly recommented to read and write the data from and on an SSD to prevent performance limitations due to the speed of the drive. Possibly limited SSD storage is handled via the max_temp_files function, which allows writing the resampled data fast on the SSD and moves it in the background to an Server, NAS or HDD. 

3. mosaic.py 
Overlapping and nodata zones from multiple images are handled with this script. The input data must be multiband GeoTIFFs and the output is a multiband GeoTIFF as well with the extend of the valid pixels of the input data. Overlapping pixels are calculated by the nearest neigbours algorithm and nodata values can be specified. Since the mosaicking requires the same crs for all input datasets, it is possibly to define the desired crs and to transform all deviating datasets. 

The input is loaded to the RAM to achieve fast processing. To prevent RAM overtrain, it is possible to define a block_size. Thereby the script devides the hole input dataset into blocks of the defined size, mosaices them one after the other and mosaices the blocks afterwards. 
