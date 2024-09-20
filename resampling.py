# resampling all input bands to a common resolution and saving them as a single multiband TIFF file

import os
import glob
import argparse
import rasterio
from rasterio.warp import reproject, Resampling
import earthpy.spatial as es
import numpy as np
import psutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Thread, Event
import shutil
import gc
import re


def resample_band(src, dst, band_index, transform, resolution):
    if band_index > src.count:
        raise ValueError(
            f"Band index {band_index} out of range for dataset with {src.count} bands."
        )
    reproject(
        source=rasterio.band(src, band_index),
        destination=rasterio.band(dst, band_index),
        src_transform=src.transform,
        src_crs=src.crs,
        dst_transform=transform,
        dst_crs=src.crs,
        resampling=Resampling.nearest,
    )
    print(f"Band {band_index} resampled.")


def get_unique_filename(output_folder, base_filename):
    output_path = os.path.join(output_folder, base_filename)
    if not os.path.exists(output_path):
        return output_path

    base, ext = os.path.splitext(base_filename)
    counter = 1
    while True:
        new_filename = f"{base}_{counter}{ext}"
        new_output_path = os.path.join(output_folder, new_filename)
        if not os.path.exists(new_output_path):
            return new_output_path
        counter += 1


def get_unique_foldername(base_folder, base_foldername):
    output_path = os.path.join(base_folder, base_foldername)
    if not os.path.exists(output_path):
        return output_path

    counter = 1
    while True:
        new_foldername = f"{base_foldername}_{counter}"
        new_output_path = os.path.join(base_folder, new_foldername)
        if not os.path.exists(new_output_path):
            return new_output_path
        counter += 1


def resample_and_save_band(band_path, temp_path, transform, width, height, resolution):
    with rasterio.open(band_path) as src:
        kwargs = src.meta.copy()
        kwargs.update(
            {
                "crs": src.crs,
                "transform": transform,
                "width": width,
                "height": height,
                "count": 1,  # Single band
                "driver": "GTiff",  # Ensure the output is a GeoTIFF
            }
        )

        with rasterio.open(temp_path, "w", **kwargs) as dst:
            resample_band(src, dst, 1, transform, resolution)


def move_files(temp_output_folder, final_output_folder):
    for file_name in os.listdir(temp_output_folder):
        file_path = os.path.join(temp_output_folder, file_name)
        shutil.move(file_path, final_output_folder)
    print(f"Dateien wurden nach {final_output_folder} verschoben.")


def list_processed_files(output_folder):
    processed_files = set()
    for file_name in os.listdir(output_folder):
        if file_name.endswith(".tif"):
            processed_files.add(file_name)
    return processed_files


def delete_processed_folders(base_folder, processed_files):
    subfolders = [f.path for f in os.scandir(base_folder) if f.is_dir()]
    for folder in subfolders:
        folder_name = os.path.basename(folder)
        date = folder_name[11:19]
        base_filename = f"{date}_{folder_name}.tif"
        if base_filename in processed_files:
            print(f"Lösche bereits verarbeiteten Ordner: {folder}")
            shutil.rmtree(folder, ignore_errors=True)


def resample_and_save_bands(
    input_folder,
    base_folder,
    temp_output_folder,
    final_output_folder,
    bands,
    resolution,
    max_workers,
    max_temp_files,
    processed_files,
):
    folder_name = os.path.basename(input_folder)
    date = folder_name[11:19]

    # Use the date and the rest of the input folder name for the filename
    base_filename = f"{date}_{folder_name}.tif"
    if base_filename in processed_files:
        print(f"Datei {base_filename} wurde bereits verarbeitet. Überspringe...")
        return

    # Create a worker-specific temporary directory within the base folder
    worker_temp_dir = get_unique_foldername(base_folder, f"temp_{folder_name}")
    os.makedirs(worker_temp_dir, exist_ok=True)

    band_paths = []
    for band in bands:
        resolution_folder = "R10m" if band in ["B02", "B03", "B04", "B08"] else "R20m"
        pattern = os.path.join(
            input_folder,
            "GRANULE",
            "*",
            "IMG_DATA",
            resolution_folder,
            f"*_{band}_{resolution_folder[-3:]}.jp2",
        )
        print(f"Suche nach Dateien mit Muster: {pattern}")
        paths = glob.glob(pattern)
        if not paths:
            print(f"Keine Dateien für Band {band} im Ordner {resolution_folder} gefunden.")
            continue
        band_paths.append(paths[0])

    if not band_paths:
        print(f"Keine Bänder zum Resamplen gefunden in {input_folder}.")
        return

    temp_files = []
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for idx, band_path in enumerate(band_paths):
                temp_filename = f"temp_band_{idx + 1}.tif"
                temp_path = os.path.join(worker_temp_dir, temp_filename)
                temp_files.append(temp_path)

                with rasterio.open(band_path) as src:
                    bbox = src.bounds
                    transform, width, height = rasterio.warp.calculate_default_transform(
                        src.crs,
                        src.crs,
                        src.width,
                        src.height,
                        left=bbox.left,
                        bottom=bbox.bottom,
                        right=bbox.right,
                        top=bbox.top,
                        resolution=resolution,
                    )

                futures.append(
                    executor.submit(
                        resample_and_save_band,
                        band_path,
                        temp_path,
                        transform,
                        width,
                        height,
                        resolution,
                    )
                )

            for future in as_completed(futures):
                future.result()

        # Stack the temp files and save the multiband TIFF
        temp_stack_path = os.path.join(worker_temp_dir, f"temp_stack_{folder_name}.tif")
        stack_array, stack_meta = es.stack(temp_files, out_path=temp_stack_path)
        stack_meta.update({"count": len(temp_files), "driver": "GTiff"})

        output_path = get_unique_filename(
            temp_output_folder if temp_output_folder else final_output_folder,
            base_filename,
        )

        with rasterio.open(output_path, "w", **stack_meta) as dst:
            for idx in range(stack_array.shape[0]):
                dst.write(stack_array[idx], idx + 1)

        print(f"Multiband-TIFF gespeichert als {output_path}")

        # Check if the number of files in the temp_output_folder exceeds the threshold
        if temp_output_folder and len(os.listdir(temp_output_folder)) >= max_temp_files:
            move_files(temp_output_folder, final_output_folder)

    finally:
        # Clean up temporary files and directory
        shutil.rmtree(worker_temp_dir, ignore_errors=True)
        # Explicitly call garbage collector
        gc.collect()


def process_all_folders(
    base_folder,
    temp_output_folder,
    final_output_folder,
    bands,
    resolution,
    max_workers,
    max_temp_files,
):
    start_time = time.time()  # Start time measurement

    processed_files = list_processed_files(final_output_folder)
    delete_processed_folders(base_folder, processed_files)  # Delete already processed folders

    subfolders = [f.path for f in os.scandir(base_folder) if f.is_dir()]

    # Extract numeric part from folder names and sort numerically
    def extract_numeric_part(folder_name):
        match = re.search(r"\d+", folder_name)
        return int(match.group()) if match else float("inf")

    subfolders.sort(key=lambda x: extract_numeric_part(os.path.basename(x)))
    total_folders = len(subfolders)
    print(f"Es wurden {total_folders} Unterordner gefunden.")

    for idx, folder in enumerate(subfolders):
        print(f"Verarbeite Ordner {idx + 1} von {total_folders}: {folder}")
        try:
            resample_and_save_bands(
                folder,
                base_folder,
                temp_output_folder,
                final_output_folder,
                bands,
                resolution,
                max_workers,
                max_temp_files,
                processed_files,
            )
            print(f"Erfolgreich verarbeitet: {folder}")
        except Exception as e:
            print(f"Fehler bei der Verarbeitung von {folder}: {e}")
        finally:
            # Explicitly call garbage collector after processing each folder
            gc.collect()

    # Ensure any remaining files are moved
    if temp_output_folder and len(os.listdir(temp_output_folder)) > 0:
        move_files(temp_output_folder, final_output_folder)

    end_time = time.time()  # End time measurement
    elapsed_time = end_time - start_time
    print(f"Alle Ordner wurden verarbeitet. Gesamtdauer: {elapsed_time:.2f} Sekunden.")


def monitor_resources(stop_event, interval=1):
    while not stop_event.is_set():
        cpu_usages = psutil.cpu_percent(interval=interval, percpu=True)
        memory_info = psutil.Process(os.getpid()).memory_info()
        for i, cpu_usage in enumerate(cpu_usages):
            print(f"CPU Core {i} Usage: {cpu_usage}%")
        print(f"Memory Usage: {memory_info.rss / (1024 * 1024)} MB")
        time.sleep(interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Resample and save Sentinel-2 bands.")
    parser.add_argument(
        "--base_folder",
        type=str,
        required=True,
        help="Base folder containing subfolders with Sentinel-2 data.",
    )
    parser.add_argument(
        "--temp_output_folder",
        type=str,
        help="Temporary folder to save the output files.",
    )
    parser.add_argument(
        "--final_output_folder",
        type=str,
        required=True,
        help="Final folder to move the output files.",
    )
    parser.add_argument(
        "--bands",
        type=str,
        nargs="+",
        default=["B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B11", "B12"],
        help="List of bands to process.",
    )
    parser.add_argument(
        "--resolution",
        type=int,
        default=10,
        help="Target resolution for resampling (default: 10m).",
    )
    parser.add_argument(
        "--max_workers",
        type=int,
        default=10,
        help="Number of workers for parallel processing (default: 10).",
    )
    parser.add_argument(
        "--max_temp_files",
        type=int,
        default=10,
        help="Maximum number of files in the temporary folder before moving (default: 10).",
    )
    parser.add_argument("--monitor", action="store_true", help="Enable CPU and memory monitoring.")
    args = parser.parse_args()

    stop_event = Event()

    if args.monitor:
        # Start resource monitoring in a separate thread
        monitor_thread = Thread(target=monitor_resources, args=(stop_event,), daemon=False)
        monitor_thread.start()

    print("Starte Verarbeitung...")
    process_all_folders(
        args.base_folder,
        args.temp_output_folder,
        args.final_output_folder,
        args.bands,
        args.resolution,
        args.max_workers,
        args.max_temp_files,
    )
    print("Verarbeitung abgeschlossen.")

    if args.monitor:
        stop_event.set()
        monitor_thread.join()
