import re
import pandas as pd
import twd97
import math
from shapely.geometry import Point
import geopandas as gpd


geo_keys = ['verbatimRawLongitude', 'verbatimRawLatitude', 'standardRawLongitude','standardRawLatitude','raw_location_rpt','verbatimLongitude', 'verbatimLatitude', 'standardLongitude','standardLatitude','location_rpt',
            'grid_1','grid_1_blurred','grid_5','grid_5_blurred','grid_10','grid_10_blurred','grid_100','grid_100_blurred','rawCounty','rawMunicipality','county','municipality']

 
gdf = gpd.read_file('/bucket/TW_TOWN/TOWN_MOI_1131028.shp')
if gdf.crs is None:
    gdf = gdf.set_crs('EPSG:4326')
gdf_ocean = gpd.read_file('/bucket/TW_TOWN_OCEAN/tw_map_o.shp')
if gdf_ocean.crs is None:
    gdf_ocean = gdf_ocean.set_crs('EPSG:4326')
 

def _coor_to_grid(x, y, grid):
    grid_x = int(math.floor((x + 180) / grid))
    grid_y = int(math.floor((y + 90) / grid))
    return grid_x, grid_y


def _convert_to_decimal(lon, lat):
    def _parse_one(value):
        if pd.isna(value) or str(value).strip() == '':
            return None
        text = str(value).strip().upper().replace('\\', '').replace('_', '')
        sign = 1
        if 'S' in text or 'W' in text:
            sign = -1
        elif text.startswith('-'):
            sign = -1
        numbers = re.findall(r"(\d+(?:\.\d*)?|\.\d+)", text)
        try:
            nums = [float(n) for n in numbers if n != '.']
        except (TypeError, ValueError):
            return None
        if not nums:
            return None
        if len(nums) == 1:
            val = nums[0]
        elif len(nums) == 2:
            val = nums[0] + nums[1] / 60
        else:
            val = nums[0] + nums[1] / 60 + nums[2] / 3600
        return val * sign
    return _parse_one(lon), _parse_one(lat)


def _try_parse_coor(value, valid_range, exclude=None):
    if value is None or str(value) in (exclude or set()):
        return None
    try:
        val = float(value)
    except (TypeError, ValueError):
        return None
    if valid_range[0] <= val <= valid_range[1]:
        return val
    return None


def _standardize_coor(lon, lat):
    std_lon = _try_parse_coor(lon, valid_range=(-180, 180), exclude={'', '0', 'WGS84'})
    std_lat = _try_parse_coor(lat, valid_range=(-90, 90), exclude={''})
    if std_lon is None or std_lat is None:
        try:
            std_lat, std_lon = twd97.towgs84(float(lon), float(lat))
            if not (-180 <= std_lon <= 180):
                std_lon = None
            if not (-90 <= std_lat <= 90):
                std_lat = None
        except Exception:
            std_lon, std_lat = None, None
    if std_lon is not None and std_lat is not None:
        return std_lon, std_lat, f'POINT({std_lon} {std_lat})'
    return None, None, None


def _resolve_coordinate_precision(df):
    if 'coordinatePrecision' in df.columns:
        cp = pd.to_numeric(df['coordinatePrecision'], errors='coerce')
    else:
        cp = pd.Series(float('nan'), index=df.index)
    if 'sensitiveCategory' in df.columns:
        sc = df['sensitiveCategory']
    else:
        sc = pd.Series('', index=df.index, dtype=object)
    cp = cp.where(cp.notna() & (cp != 0),
                  sc.map({'輕度': 0.01, '重度': 0.1}))
    return cp


def _compute_grid_values(lon, lat):
    result = {}
    for level in [1, 5, 10, 100]:
        if lon is not None and lat is not None:
            gx, gy = _coor_to_grid(lon, lat, level / 100)
            result[f'grid_{level}'] = f'{int(gx)}_{int(gy)}'
        else:
            result[f'grid_{level}'] = '-1_-1'
    return result


def _blur_coordinate(raw_lon, raw_lat, coordinatePrecision, is_full_hidden):
    if is_full_hidden:
        return None, None
    if coordinatePrecision is None or pd.isna(coordinatePrecision):
        return raw_lon, raw_lat
    cp = float(coordinatePrecision)
    if 0 < cp < 1:
        decimal_places = round(-math.log10(cp))
        ten_times = 10 ** decimal_places
        return (math.floor(float(raw_lon) * ten_times) / ten_times,
                math.floor(float(raw_lat) * ten_times) / ten_times)
    elif cp == 1:
        return str(raw_lon).split('.')[0], str(raw_lat).split('.')[0]
    else:
        return raw_lon, raw_lat


def _compute_geo_row(verbatimLongitude, verbatimLatitude, coordinatePrecision, dataGeneralizations, is_full_hidden=False):
    result = {}
    is_blurred = bool(dataGeneralizations) or is_full_hidden
    if any(c in str(verbatimLongitude) for c in 'NSWE') or any(c in str(verbatimLatitude) for c in 'NSWE'):
        lon, lat = _convert_to_decimal(verbatimLongitude, verbatimLatitude)
    else:
        lon, lat = verbatimLongitude, verbatimLatitude
    raw_lon, raw_lat, raw_rpt = _standardize_coor(lon, lat)
    result['_raw_lon'] = raw_lon
    result['_raw_lat'] = raw_lat
    if is_blurred:
        result['standardRawLongitude'] = raw_lon
        result['standardRawLatitude'] = raw_lat
        result['raw_location_rpt'] = raw_rpt
        result['verbatimRawLongitude'] = verbatimLongitude
        result['verbatimRawLatitude'] = verbatimLatitude
    raw_grids = _compute_grid_values(raw_lon, raw_lat)
    for level in [1, 5, 10, 100]:
        result[f'grid_{level}'] = raw_grids[f'grid_{level}']
    if raw_lon is not None and raw_lat is not None:
        blur_lon, blur_lat = _blur_coordinate(raw_lon, raw_lat, coordinatePrecision, is_full_hidden)
        std_lon, std_lat, loc_rpt = _standardize_coor(blur_lon, blur_lat)
    else:
        std_lon, std_lat, loc_rpt = None, None, None
    result['standardLongitude'] = std_lon
    result['standardLatitude'] = std_lat
    result['location_rpt'] = loc_rpt
    result['_blur_lon'] = std_lon
    result['_blur_lat'] = std_lat
    blur_grids = _compute_grid_values(std_lon, std_lat)
    for level in [1, 5, 10, 100]:
        result[f'grid_{level}_blurred'] = blur_grids[f'grid_{level}']
    if std_lon is not None or is_full_hidden:
        result['verbatimLongitude'] = std_lon
        result['verbatimLatitude'] = std_lat
    else:
        result['verbatimLongitude'] = verbatimLongitude
        result['verbatimLatitude'] = verbatimLatitude
    return result


def _lookup_county_batch(lons, lats):
    n = len(lons)
    counties = pd.Series([None] * n, dtype=object)
    municipalities = pd.Series([None] * n, dtype=object)

    geom = [Point(lo, la) if (lo is not None and la is not None) else None
            for lo, la in zip(lons, lats)]
    points = gpd.GeoDataFrame({'_idx': range(n), 'geometry': geom}, crs='EPSG:4326')
    valid = points[~(points.geometry.is_empty | points.geometry.isna())].copy()

    if valid.empty:
        return counties, municipalities

    # 陸地 sjoin
    land = gpd.sjoin(valid, gdf[['geometry', 'COUNTYNAME', 'TOWNNAME']], how='inner', predicate='within')
    single = land.groupby('_idx').filter(lambda g: len(g) == 1)
    if not single.empty:
        counties.iloc[single._idx.values] = single.COUNTYNAME.values
        municipalities.iloc[single._idx.values] = single.TOWNNAME.values

    # 海域 sjoin（僅處理陸地未匹配的）
    unmatched_idx = set(valid._idx) - set(single._idx)
    if unmatched_idx:
        unmatched = valid[valid._idx.isin(unmatched_idx)].copy()
        ocean = gpd.sjoin(unmatched, gdf_ocean[['geometry', 'COUNTYO']], how='inner', predicate='within')
        ocean_single = ocean.groupby('_idx').filter(lambda g: len(g) == 1)
        if not ocean_single.empty:
            counties.iloc[ocean_single._idx.values] = ocean_single.COUNTYO.values

    return counties, municipalities


def _normalize_data_generalizations(series):
    """將 dataGeneralizations 統一轉為 bool，處理 'Y'/'N'/True/False/None"""
    return series.map(lambda v: v is True or v == 'Y' or v == 'y')


def process_geo_batch(df, is_full_hidden=False, skip_blur=False, infer_generalizations='auto'):
    
    """
    infer_generalizations:
        'auto'  (預設) - 用 df 既有的 dataGeneralizations 欄位 (寫法 2/3 的單位)
        True           - 從 coordinatePrecision 推斷 (寫法 1 的單位，
                         該單位 API 沒給 dataGeneralizations 欄位)
        False          - 強制 dataGeneralizations 全部設 False
    """

    df = df.copy()

    if skip_blur:
        df['coordinatePrecision'] = float('nan')
        df['dataGeneralizations'] = False
        df['_is_hidden'] = False
    else:
        df['coordinatePrecision'] = _resolve_coordinate_precision(df)

        if infer_generalizations is True:
            df['dataGeneralizations'] = df['coordinatePrecision'].apply(
                lambda x: True if x else False
            )
        elif infer_generalizations is False:
            df['dataGeneralizations'] = False
        else:  # 'auto'
            df['dataGeneralizations'] = _normalize_data_generalizations(
                df.get('dataGeneralizations', pd.Series(False, index=df.index))
            )

        if is_full_hidden == 'auto':
            if 'sensitiveCategory' in df.columns:
                df['_is_hidden'] = df['sensitiveCategory'].isin(['縣市', '座標不開放'])
            else:
                df['_is_hidden'] = False
        else:
            df['_is_hidden'] = bool(is_full_hidden)

    geo_results = df.apply(
        lambda x: _compute_geo_row(
            x.verbatimLongitude, x.verbatimLatitude,
            x.coordinatePrecision, x.dataGeneralizations,
            is_full_hidden=x._is_hidden
        ), axis=1
    )
    geo_df = pd.DataFrame(geo_results.tolist(), index=df.index)
    raw_county, raw_muni = _lookup_county_batch(
        geo_df['_raw_lon'].tolist(), geo_df['_raw_lat'].tolist()
    )
    geo_df['county'] = raw_county.values
    geo_df['municipality'] = raw_muni.values
    geo_df['rawCounty'] = None
    geo_df['rawMunicipality'] = None
    is_blurred = df['dataGeneralizations'] | df['_is_hidden']
    if is_blurred.any():
        blur_idx = is_blurred[is_blurred].index
        blur_county, blur_muni = _lookup_county_batch(
            geo_df.loc[blur_idx, '_blur_lon'].tolist(),
            geo_df.loc[blur_idx, '_blur_lat'].tolist()
        )
        geo_df.loc[blur_idx, 'rawCounty'] = raw_county.loc[blur_idx].values
        geo_df.loc[blur_idx, 'rawMunicipality'] = raw_muni.loc[blur_idx].values
        geo_df.loc[blur_idx, 'county'] = blur_county.values
        geo_df.loc[blur_idx, 'municipality'] = blur_muni.values
    for k in geo_keys:
        if k not in geo_df.columns:
            geo_df[k] = None
    return geo_df[geo_keys]


def parse_verbatim_coords(coord_str):
    # 如果是空值或非字串，回傳 None
    if pd.isna(coord_str) or str(coord_str).strip() == '':
        return None, None
    text = str(coord_str).strip()
    # 使用正規表示式切割，支援「全形逗號」與「半形逗號」
    # 這裡假設分隔符號是逗號
    parts = re.split(r'[，,]', text)
    # 去除每個部分的空白
    parts = [p.strip() for p in parts if p.strip()]
    v_lat = None
    v_lon = None
    for part in parts:
        upper_part = part.upper()
        # 判斷緯度 (N 或 S)
        if 'N' in upper_part or 'S' in upper_part:
            v_lat = part
        # 判斷經度 (E 或 W)
        elif 'E' in upper_part or 'W' in upper_part:
            v_lon = part
    # 特殊補救：如果有兩段，且其中一段沒找到方向，依照常見順序補齊 (通常是 經度, 緯度)
    # 例如資料中的 '0244748， 121431N' (只有後面有 N)
    if len(parts) == 2:
        if v_lat and not v_lon:
            # 已經找到緯度，剩下那個大概是經度
            v_lon = parts[0] if parts[0] != v_lat else parts[1]
        elif v_lon and not v_lat:
            # 已經找到經度，剩下那個大概是緯度
            v_lat = parts[0] if parts[0] != v_lon else parts[1]
    return v_lat, v_lon