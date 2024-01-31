import os
import platform

import re

from defoe.config import GEO_PARSER_PATH
from defoe.file_utils import download_and_unzip


def download_geoparser():
    # Download geoparser lib if it has not been downloaded
    if not os.path.exists(GEO_PARSER_PATH):
        # Create related parent directories
        unzip_dir = os.path.dirname(GEO_PARSER_PATH)
        os.makedirs(unzip_dir)
        try:
            print("---Downloading geoparser library---")
            download_and_unzip("https://storage.googleapis.com/damon_public_files/geoparser.zip", unzip_dir)
        except Exception as e:
            print(f"Error encountered: {e}")


def get_geo_supported_os_type():
    # Get platform information
    platform_name = platform.system() + " " + platform.release() + " " + platform.machine()
    print(platform_name)
    if re.match("Darwin?1[012345]*", platform_name):
        return "sys-i386-snow-leopard"

    if re.match("Darwin?1[6789]*", platform_name):
        return "sys-x86-64-sierra"

    if re.match("Darwin?2*", platform_name):
        return "sys-x86-64-sierra"

    if re.match("Linux(.)*x86_64(.)*", platform_name):
        return "sys-x86-64-el7"

    raise Exception("Geoparser does not support platform " + platform_name)