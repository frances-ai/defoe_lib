from os.path import dirname, abspath
import platform

import re


def get_root_path():
    root_path = dirname(abspath(__file__))
    zip_index = root_path.find(".zip")
    if zip_index > -1:
        root_path = root_path[:zip_index]
    return root_path


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
