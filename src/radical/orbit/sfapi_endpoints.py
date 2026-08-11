
__author__    = 'Radical Development Team'
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2024, RADICAL@Rutgers'
__license__   = 'MIT'


# Hardcoded SFAPI endpoints supported by this plugin.  Each entry describes
# the Superfacility API base URL, its display label, and the OIDC token
# endpoint used for the OAuth2 client-credentials flow (private_key_jwt).
SFAPI_ENDPOINTS = {
    'nersc': {
        'url'      : 'https://api.nersc.gov/api/v1.2',
        'label'    : 'NERSC (SFAPI)',
        'token_url': 'https://oidc.nersc.gov/c2id/token',
    },
}
