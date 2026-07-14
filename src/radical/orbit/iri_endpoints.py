
__author__    = 'Radical Development Team'
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2024, RADICAL@Rutgers'
__license__   = 'MIT'


# Hardcoded IRI endpoints supported by this plugin.
# Each entry describes the REST base URL and the authentication method used.
IRI_ENDPOINTS = {
    'nersc': {
        'url'  : 'https://api.iri.nersc.gov',
        'label': 'NERSC (Perlmutter)',
        'auth' : 'globus',
    },
    'olcf': {
        'url'  : 'https://amsc-open.s3m.olcf.ornl.gov',
        'label': 'OLCF (Frontier/Odo)',
        'auth' : 's3m',
    },
}

# Job states from the IRI compute API
IRI_JOB_STATES_TERMINAL = {'completed', 'failed', 'canceled'}
IRI_JOB_STATES_ACTIVE   = {'new', 'queued', 'held', 'active'}
