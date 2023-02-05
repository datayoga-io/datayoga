try:
    # This hack makes it possible to use the new driver with SQLAlchemy 1.4.*
    # More: https://lnk.pw/swif

    import sys

    import oracledb
    from oracledb.exceptions import DatabaseError

    oracledb.version = "8.3.0"
    sys.modules["cx_Oracle"] = oracledb
    try:
        oracledb.init_oracle_client()
    except DatabaseError:
        pass
except ImportError:
    pass
