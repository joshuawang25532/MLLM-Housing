import os
import re

def generate_tile_filename_by_coords(ne_lat, sw_long):
    """Use the north and west coords"""
    lat_parts = str(ne_lat).split('.')
    lat_str = f"{lat_parts[0]}_{lat_parts[1]}"
    long_str = str(sw_long).replace('.', '_')
    filename = f"tile_{lat_str}_long_{long_str}.json"
    return filename

if __name__ == "__main__":
    north = 37.730169051308685
    west = -122.38933369885972
    print(generate_tile_filename_by_coords(north, west))

