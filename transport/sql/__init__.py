"""
This namespace/package wrap the sql functionalities for a certain data-stores
    - netezza, postgresql, mysql and sqlite
    - mariadb, redshift (also included)
"""
from . import postgresql, mysql, netezza, sqlite


#
# Creating aliases for support of additional data-store providerss
#
mariadb     = mysql
redshift    = postgresql
sqlite3     = sqlite


# from transport import sql

