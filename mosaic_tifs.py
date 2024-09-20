# mosaicking tif files based on their coordinates and dates

import os
import shutil
import subprocess
from tqdm import tqdm
import argparse
import time
import psutil
from osgeo import gdal
import uuid

# List all files with a specific extension in a folder
def list_extension(folder, extension):
    return [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(f".{extension}")]

# Transform a TIF file to a specified EPSG coordinate system
def transform_to_epsg(input_file, output_file, epsg="EPSG:32633", nodata_value=0):
    cmd_warp = [
        "gdalwarp",
        "-t_srs", epsg,
        "-multi",
        "-wo", "NUM_THREADS=ALL_CPUS",
        "-srcnodata", str(nodata_value),
        "-dstnodata", str(nodata_value),
        input_file, output_file,
    ]
    subprocess.run(cmd_warp, check=True)

# Check and transform the coordinate system of TIF files if necessary
def check_and_transform_crs(tif_files, temp_folder, epsg="EPSG:32633", nodata_value=0):
    transformed_files = []
    for tif in tif_files:
        dataset = gdal.Open(tif)
        crs = dataset.GetProjection()
        if epsg not in crs:
            unique_suffix = uuid.uuid4().hex
            transformed_file = os.path.join(temp_folder, os.path.basename(tif).replace(".tif", f"_transformed_{unique_suffix}.tif"))
            transform_to_epsg(tif, transformed_file, epsg, nodata_value)
            transformed_files.append(transformed_file)
        else:
            # Copy the file to the temp folder to ensure all processing happens there
            temp_file = os.path.join(temp_folder, os.path.basename(tif))
            shutil.copy(tif, temp_file)
            transformed_files.append(temp_file)
    return transformed_files

# Merge multiple TIF files into one using gdalwarp
def gdal_warp_merge(input_files, output_file, nodata_value=0):
    cmd_warp = [
        "gdalwarp",
        "-multi",
        "-wo", "NUM_THREADS=ALL_CPUS",
        "-srcnodata", str(nodata_value),
        "-dstnodata", str(nodata_value),
        "-co", "BIGTIFF=YES",
        "-co", "COMPRESS=LZW",
        "-overwrite",
    ] + input_files + [output_file]
    subprocess.run(cmd_warp, check=True)

# Process a block of TIF files and merge them
def process_block(block, date, output_path, block_num, nodata_value=0):
    block_output_file = os.path.join(output_path, f"{date}_block_{block_num}.tif")
    gdal_warp_merge(block, block_output_file, nodata_value)
    return block_output_file

# Parse the date from the TIF file name
def date_parser(tif):
    return os.path.basename(tif)[:8]

# Combine TIF files based on their dates and coordinates
def combine_tifs(tif_files, temp_folder, output_folder, block_size=10, nodata_value=0, epsg="EPSG:32633"):
    date_index = {}
    for tif in tif_files:
        date = date_parser(tif)
        if date not in date_index:
            date_index[date] = []
        date_index[date].append(tif)

    for date, files in date_index.items():
        print(f"Processing date: {date}")
        print(f"Number of images to merge: {len(files)}")

        transformed_files = check_and_transform_crs(files, temp_folder, epsg, nodata_value)

        if len(transformed_files) <= block_size:
            final_output_file = os.path.join(temp_folder, f"{date}.tif")
            gdal_warp_merge(transformed_files, final_output_file, nodata_value)
            print(f"Saved combined TIF for date: {date}")
        else:
            blocks = [transformed_files[i:i + block_size] for i in range(0, len(transformed_files), block_size)]
            block_mosaics = []

            for block_num, block in enumerate(tqdm(blocks, desc="Processing blocks")):
                block_mosaics.append(process_block(block, date, temp_folder, block_num, nodata_value))

            final_output_file = os.path.join(temp_folder, f"{date}.tif")
            gdal_warp_merge(block_mosaics, final_output_file, nodata_value)
            print(f"Saved combined TIF for date: {date}")

            # Delete temporary block files
            for block_file in block_mosaics:
                os.remove(block_file)

            block_mosaics.clear()
            psutil.virtual_memory()

        # Move the final output file to the output folder
        shutil.move(final_output_file, os.path.join(output_folder, f"{date}.tif"))

        # Delete temporary transformed files
        for transformed_file in transformed_files:
            os.remove(transformed_file)

        files.clear()
        psutil.virtual_memory()

def main():
    parser = argparse.ArgumentParser(description="Combine TIF files based on their coordinates.")
    parser.add_argument("--input_folder", type=str, required=True, help="Input folder containing the TIF files")
    parser.add_argument("--output_folder", type=str, required=True, help="Output folder for the combined TIF files")
    parser.add_argument("--temp_folder", type=str, required=True, help="Temporary folder on SSD for processing")
    parser.add_argument("--block_size", type=int, default=10, help="Number of files to process in each block")
    parser.add_argument("--nodata_value", type=int, default=0, help="Value to use for nodata pixels")
    parser.add_argument("--epsg", type=str, default="EPSG:32633", help="EPSG code for the coordinate system")

    args = parser.parse_args()

    start_time = time.time()

    tif_files = list_extension(args.input_folder, "tif")
    combine_tifs(tif_files, args.temp_folder, args.output_folder, args.block_size, args.nodata_value, args.epsg)

    end_time = time.time()
    print(f"Processing completed in {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()
