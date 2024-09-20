# renaming the raw sentinel files to a more readable format

import os
import re
import argparse


def parse_filename(filename):
    if "S1A_IW_GRDH" in filename:
        match = re.search(
            r"_(\d{8})T\d{6}_\d{8}T\d{6}_\d{6}_\w{6}_\w{4}_Cal_Spk_dB_TC\.tif$",
            filename,
        )
        if match:
            date = match.group(1)
            return f"{date}_S1A_IW_GRDH.tif"
    elif "S2A_MSIL2A" in filename or "S2B_MSIL2A" in filename:
        match = re.search(
            r"_(\d{8})T\d{6}_N\d{4}_R\d{3}_T\w{5}_\d{8}T\d{6}\.SAFE_B(\d{2}[A]?)\.tif$",
            filename,
        )
        if match:
            date = match.group(1)
            band = match.group(2)
            satellite = "S2A" if "S2A_MSIL2A" in filename else "S2B"
            return f"{date}_{satellite}_MSIL2A_B{band}.tif"
    elif "Subset_S1B_IW_GRDH_1SDV" in filename:
        match = re.search(
            r"Subset_S1B_IW_GRDH_1SDV_(\d{8})T\d{6}_\d{8}T\d{6}_\d{6}_\w{6}_\w{4}_Cal_Spk_dB_TC\.tif$",
            filename,
        )
        if match:
            date = match.group(1)
            return f"{date}_S1A_IW_GRDH.tif"
    elif "S1B_IW_GRDH_1SDV" in filename:
        match = re.search(r"(\d{8})_S1B_IW_GRDH_1SDV\.tif$", filename)
        if match:
            date = match.group(1)
            return f"{date}_S1B_IW_GRDH.tif"
    elif re.match(
        r"S2A_MSIL2A_\d{8}T\d{6}_N\d{4}_R\d{3}_T\w{5}_\d{8}T\d{6}\.SAFE_B\d{2}[A]?\.tif$",
        filename,
    ):
        match = re.search(
            r"S2A_MSIL2A_(\d{8})T\d{6}_N\d{4}_R\d{3}_T\w{5}_(\d{8})T\d{6}\.SAFE_B(\d{2}[A]?)\.tif$",
            filename,
        )
        if match:
            date = match.group(1)
            band = match.group(3)
            return f"{date}_S2A_MSIL2A_B{band}.tif"
    elif re.match(
        r"S2A_MSIL2A_\d{8}T\d{6}_N\d{4}_R\d{3}_T\w{5}_\d{8}T\d{6}\.SAFE_B8A\.tif$",
        filename,
    ):
        match = re.search(
            r"S2A_MSIL2A_(\d{8})T\d{6}_N\d{4}_R\d{3}_T\w{5}_(\d{8})T\d{6}\.SAFE_B8A\.tif$",
            filename,
        )
        if match:
            date = match.group(1)
            return f"{date}_S2A_MSIL2A_B8A.tif"
    return None


def rename_files_in_directory(directory):
    existing_filenames = set()
    for filename in os.listdir(directory):
        if os.path.isfile(os.path.join(directory, filename)):
            new_filename = parse_filename(filename)
            if new_filename:
                original_new_filename = new_filename
                counter = 1
                while new_filename in existing_filenames:
                    new_filename = f"{os.path.splitext(original_new_filename)[0]}_{counter}.tif"
                    counter += 1
                existing_filenames.add(new_filename)
                os.rename(
                    os.path.join(directory, filename),
                    os.path.join(directory, new_filename),
                )
                print(f"Renamed {filename} to {new_filename}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Rename Sentinel files in a directory based on a schema."
    )
    parser.add_argument(
        "--directory",
        type=str,
        required=True,
        help="Path to the directory containing the files",
    )

    args = parser.parse_args()

    rename_files_in_directory(args.directory)
