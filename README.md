# gps-to-rijksdriehoek
Convert GPS coordinates to Rijksdriehoek coordinates efficiently in PySpark

Worked further on https://github.com/thomasvnl/rd-to-wgs84

## Usage
```python
from jatog import wgs84_to_rds

lat_col = 'lat'
lon_col = 'lon'

utrecht_df = spark.createDataFrame(
    [
        {lat_col: 52.092876, lon_col: 5.104480}
    ]
)

x, y = wgs84_to_rds(lat_col, lon_col)

gps_df = utrecht_df.select(
    '*',
    x.alias('x'),
    y.alias('y')
)

gps_df.show()
```