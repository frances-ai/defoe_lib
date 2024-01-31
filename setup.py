import atexit
import os
import sys
import zipfile

import requests
from setuptools import setup, find_packages
from setuptools.command.install import install
from tqdm import tqdm


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


def _post_install():
    from subprocess import call
    call([sys.executable, '-m', 'nltk.downloader', 'all'])
    call([sys.executable, '-m', 'spacy', 'download', 'en_core_web_lg'])
    print("---Downloading geoparser library---")
    download_and_unzip("https://storage.googleapis.com/damon_public_files/geoparser.zip", "/tmp/defoe")


class CustomInstaller(install):
    def run(self):
        install.run(self)
        _post_install()


def setup_package():
    setup(
        name="defoe_lib",
        version="0.0.5",
        description="Analysis of historical books and newspapers data",
        author="Lilin Yu",
        author_email="damonyu97@gmail.com",
        keywords="text mining, historical, defoe, workflows",
        packages=find_packages(),
        classifiers=[
            "Programming Language :: Python :: 3.9",
            "License :: OSI Approved :: MIT License",
            "Operating System :: OS Independent",
        ],
        install_requires=[
            "lxml",
            "spacy",
            "nltk",
            "pep8",
            "pillow",
            "pylint",
            "pytest",
            "PyYAML",
            "regex",
            "requests",
            "SPARQLWrapper",
            "pandas"
        ],
        python_requires='>=3.9',
        cmdclass={
            'install': CustomInstaller,
        },
    )


if __name__ == '__main__':
    setup_package()