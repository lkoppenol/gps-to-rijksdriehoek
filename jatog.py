import pyspark
from pyspark.sql import functions as f

x0 = 155000
y0 = 463000
phi0 = 52.15517440
lam0 = 5.38720621

# Coefficients for the conversion from WGS84 to RD
Rp = [0, 1, 2, 0, 1, 3, 1, 0, 2]
Rq = [1, 1, 1, 3, 0, 1, 3, 2, 3]
Rpq = [190094.945, -11832.228, -114.221, -32.391, -0.705, -2.340, -0.608, -0.008, 0.148]

Sp = [1, 0, 2, 1, 3, 0, 2, 1, 0, 1]
Sq = [0, 2, 0, 2, 0, 1, 2, 1, 4, 4]
Spq = [309056.544, 3638.893, 73.077, -157.984, 59.788, 0.433, -6.439, -0.032, 0.092, -0.054]


def wgs84_to_rds(latitude_col: str, longitude_col: str) -> [pyspark.sql.column.Column]:
    """
    Creates two column definitions based on lat/lon column names

    Example usage:
    ```
    # df = yourdataframe_with_columns_lat_and_lon
    x, y = wgs_84_to_rds('lat', 'lon')

    # method 1: select
    df.select(
        '*',
        x.alias('rd_x'),
        y.alias('rd_y')
    )

    # method 2: withColumn
    df \
        .withColumn('rd_x', x) \
        .withColumn('rd_y', y)

    # method 3: one-liner
    df.select('*', *wgs_84_to_rds('lat', 'lon'))
    ```

    :param latitude_col: name of latitude column
    :param longitude_col: name of longitude column
    :returns: tuple column definitions for x and y
    """
    d_lat = 0.36 * (f.col(latitude_col) - phi0)
    d_lon = 0.36 * (f.col(longitude_col) - lam0)

    x = x0
    for i, v in enumerate(Rpq):
        x += v * f.pow(d_lat, Rp[i]) * f.pow(d_lon, Rq[i])

    y = y0
    for i, v in enumerate(Spq):
        y += v * f.pow(d_lat, Sp[i]) * f.pow(d_lon, Sq[i])

    return x, y
