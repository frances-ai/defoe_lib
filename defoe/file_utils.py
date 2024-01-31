"""
Helper functions for accessing files within Python modules.
"""

import os
import zipfile

import requests
from tqdm import tqdm


def get_path(module, *name):
    """
    Gets path to file in module, given module and relative path.

    :param module: module
    :type module: module
    :param *name: file name components
    :type *name: str or unicode
    :return: path to file
    :rtype: str or unicode
    """
    return os.path.join(os.path.dirname(module.__file__), *name)


def open_file(module, *name):
    """
    Gets path to file in module, given module and relative path,
    and returns open file.

    :param module: module
    :type module: module
    :param *name: file name components
    :type *name: str or unicode
    :return: file handle
    :rtype: file
    """
    return open(get_path(module, *name))


def load_content(module, *name):
    """
    Gets path to file in module, given module and relative path,
    and returns file content.

    :param module: module
    :type module: module
    :param *name: file name components
    :type *name: str or unicode
    :return: file content
    :rtype: str or unicode
    """
    with open_file(module, *name) as f:
        result = f.read()
    return result


def download_and_unzip(url, output_path):
    try:
        # Step 1: Download the zip file
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Check if the request was successful

        # Step 2: Get the total file size from the response headers
        total_size = int(response.headers.get("content-length", 0))

        # Step 3: Save the zip file to a temporary location
        temp_zip_path = "temp.zip"
        with open(temp_zip_path, "wb") as file:
            # Use tqdm to show the progress bar
            with tqdm(total=total_size, unit="B", unit_scale=True, unit_divisor=1024) as pbar:
                for data in response.iter_content(chunk_size=1024):
                    file.write(data)
                    pbar.update(len(data))

        # Step 4: Unzip the contents to the specified output path
        with zipfile.ZipFile(temp_zip_path, "r") as zip_ref:
            zip_ref.extractall(output_path)

        # Step 5: Clean up temporary zip file
        os.remove(temp_zip_path)

        print(f"Downloaded and unzipped successfully to '{output_path}'.")
    except requests.exceptions.HTTPError as errh:
        print(f"HTTP Error occurred: {errh}")
    except requests.exceptions.RequestException as err:
        print(f"An error occurred during the request: {err}")
    except zipfile.BadZipFile as errz:
        print(f"Bad Zip file encountered: {errz}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
