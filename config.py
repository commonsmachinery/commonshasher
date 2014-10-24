WMC_RATE_LIMIT = 5
WMC_USER = ''
WMC_PASSWORD = ''
BLOCKHASH_COMMAND = 'blockhash'
SQLALCHEMY_URL = 'postgresql://user:pass@localhost/test'


try:
    from config_local import *
except ImportError:
    pass

