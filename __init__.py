import os
import platform

import regex


def get_root_path():
    return os.path.abspath(__file__)


def get_geo_supported_os_type():
    # Get platform information
    platform_name = platform.system() + " " + platform.release() + " " + platform.machine()
    print(platform_name)
    if regex.match("Darwin?1[012345]*", platform_name):
        return "sys-i386-snow-leopard"

    if regex.match("Darwin?1[6789]*", platform_name):
        return "sys-x86-64-sierra"

    if regex.match("Darwin?2*", platform_name):
        return "sys-x86-64-sierra"

    if regex.match("Linux*x86_64*", platform_name):
        return "sys-x86-64-el7"

    raise Exception("Geoparser does not support platform " + platform_name)
