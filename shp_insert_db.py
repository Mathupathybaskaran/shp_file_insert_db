import os
import glob
from pathlib import Path
import geopandas as gpd
import pandas as pd
from multiprocessing import Pool
from tqdm import tqdm
from sqlalchemy import create_engine,VARCHAR
from geoalchemy2 import Geometry
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon
import datetime
 
start_time = datetime.datetime.now()
print(start_time)
# Shapefile directory
def process_shapefile(shapefile):
    try:
        gdf = gpd.read_file(shapefile)
        return gdf
    except Exception as e:
        print('An exceptional thing happened - {} - on file - {}'.format(e, shapefile))
        return None
 
if __name__ == '__main__':
    # Find all shapefiles in the root directory
    shapefiles = glob.glob('F:/PYTHON/input/*.shp',recursive=True)

    

    gdf['district_code'] = p.parts[7]
    gdf['taluk_code'] = p.parts[8]
    gdf['village_code'] = p.parts[9]
    gdf['file_path'] = shapefile
    gdf['phase'] = p.parts[5]
 
    # Set the number of parallel processes
    num_processes = 4
 
    # Create a multiprocessing pool
    with Pool(num_processes) as pool:
        # Process shapefiles in parallel
        results = list(tqdm(pool.imap(process_shapefile, shapefiles), total=len(shapefiles), desc='Reading Shapefiles', position=0, colour='blue'))
 
    # Concatenate the results into a single DataFrame
    df = pd.concat(results, ignore_index=True)
 
    # print(df)
    # exit()
 
    HOST = '192.168.4.243'
    DB = 'cadastral_boundary'
    USER = 'postgres'
    PORT = 5432
    PWD = 'postgres'
    con = f"postgresql://{USER}:{PWD}@{HOST}:{PORT}/{DB}"
    engine = create_engine(con)
    table_polygons = 'cuddalore_updated'
    batch_size = 1000
    with engine.begin() as conn:
        gdf_polygons = df[df.geometry.geom_type == 'Polygon']
       
        if(gdf_polygons.empty):
            print("No polygon")
        else:
            total_rows = len(gdf_polygons)
            num_batches = (total_rows - 1) // batch_size + 1
            for index in tqdm(range(num_batches), desc="Write Polygon",position=1, colour="Green"):
                try:
                    start = index * batch_size
                    end = min((index + 1) * batch_size, total_rows)
                    batch = gdf_polygons.iloc[start:end]
                    batch_without_z = batch[~batch.geometry.has_z]
                    data_types_without_z = {'geometry': Geometry('GEOMETRY', srid=4326)}
                    for column in batch_without_z.columns:
                        if column != 'geometry':
                            data_types_without_z[column] = VARCHAR
                    batch_without_z.to_postgis(name =table_polygons, con=engine, if_exists='append', schema="public", dtype=data_types_without_z)
                   
                except Exception as e:
                    f = open('F:/errors/fmb_error.txt', 'a')
                    f.write("\n")
                    f.write('An Poly exceptional thing happend - %s - on file - %s' % (e,gdf_polygons.iloc[[index]]['file_path']))
                    f.close()
                    pass
        print("Polygon Completed")
 
end_time = datetime.datetime.now()
print(end_time)
