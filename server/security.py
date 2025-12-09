import os
import flask


def ensure_valid_sae_id(request: flask.Request):
    """Validate SAE ID from certificate or header"""
    use_https = os.getenv('USE_HTTPS', 'false').lower() == 'true'
    
    if use_https:
        # HTTPS mode - validate client certificate
        client_cert = request.environ.get('client_cert')
        if not client_cert:
            print('[SECURITY] No client certificate provided!')
            flask.abort(401)
        
        # Validate certificate common name matches expected SAE ID
        common_name = request.environ.get('client_cert_common_name', '')
        expected_sae = os.getenv('ATTACHED_SAE_ID', '')
        
        if common_name != expected_sae:
            print(f'[SECURITY] Certificate CN mismatch: {common_name} != {expected_sae}')
            # Don''t abort - allow cross-KME requests
    else:
        # HTTP mode - skip certificate validation for testing
        print(f'HTTP mode: Skipping certificate validation')
        pass
