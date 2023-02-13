try:
    # This hack makes it possible to use the new driver with SQLAlchemy 1.4.*
    # More: https://lnk.pw/swif

    import sys

    import oracledb

    oracledb.version = "8.3.0"
    sys.modules["cx_Oracle"] = oracledb
    try:
        oracledb.init_oracle_client()
    # Here we're ignoring the DatabaseError from oracledb.
    # Because there may be side effects of re-importing in some cases,
    # we must mute all exceptions.
    except:  # noqa
        pass
except ImportError:
    pass
