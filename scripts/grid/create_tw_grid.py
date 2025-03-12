import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon

gdf = gpd.read_file('/bucket/TW_TOWN/TOWN_MOI_1131028.shp')
gdf_ocean = gpd.read_file('/bucket/TW_TOWN_OCEAN/tw_map_o.shp')

gdf_ = pd.concat([gdf, gdf_ocean])
dissolved_gdf = gdf_.dissolve()

# dissolved_gdf.to_file('tw_dissolved.shp', driver='ESRI Shapefile')


import numpy as np
import bisect

def convert_coor_to_grid(x, y, grid):
    list_x = np.arange(-180, 180+grid, grid)
    list_y = np.arange(-90, 90+grid, grid)
    grid_x = bisect.bisect(list_x, x)-1
    grid_y = bisect.bisect(list_y, y)-1
    return grid_x, grid_y




# >>> dissolved_gdf.total_bounds
# array([114.35928247,  10.37134766, 124.78313461,  26.43722222])

# 114.35928247, 10.37134766 - 5887, 2007
# 114.35928247, 26.43722222 - 5887, 2328
# 124.78313461, 10.37134766 - 6095, 2007
# 124.78313461, 26.43722222 - 6095, 2328

# 應該用bounds的範圍去切分網格
# 看看網格四邊形是否有和台灣範圍相交
# 若有的話就納入


def convert_grid_to_square(grid_x, grid_y, grid):
    list_x = np.arange(-180, 180+grid, grid)
    list_y = np.arange(-90, 90+grid, grid)
    x1 = round(list_x[grid_x],4)
    x2 = round(list_x[grid_x+1],4)
    y1 = round(list_y[grid_y],4)
    y2 = round(list_y[grid_y+1],4)
    return [[x1,y1],[x2,y1],[x2,y2],[x1,y2],[x1,y1]]


# x: 5887 - 6095
# y: 2007 - 2328

# convert_grid_to_square(5887,2007,0.05)
# convert_grid_to_square(5886,2006,0.05)


# Polygon([[114.35, 10.35], [114.4, 10.35], [114.4, 10.4], [114.35, 10.4], [114.35, 10.35]])


results = []
for x in range(5887, 6095+1):
    for y in range(2007, 2328+1):
        has = dissolved_gdf.intersects(Polygon(convert_grid_to_square(x,y,0.05))).values[0]
        if has:
            results.append({'grid_5':f'{x}_{y}'})

results = pd.DataFrame(results)

results.to_csv('TW_grid_5.csv',index=None)