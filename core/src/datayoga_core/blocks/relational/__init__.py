import sys

try:
    # This hack makes it possible to use the new driver with SQLAlchemy 1.4.*
    # More: https://lnk.pw/swif

    import oracledb

    oracledb.version = "8.3.0"
    sys.modules["cx_Oracle"] = oracledb
except ImportError:
    pass
