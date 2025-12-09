import os
import flask
import requests
from keys.key_store import KeyStore
from network.scanner import Scanner
from server import security


class External:
    def __init__(self, scanner: Scanner, key_store: KeyStore):
        self.scanner = scanner
        self.key_store = key_store

    def get_status(self, request: flask.Request, slave_sae_id: str):
        security.ensure_valid_sae_id(request)
        kme = self.scanner.find_kme(slave_sae_id)
        if kme is None:
            return {'message': 'The given slave SAE ID is unknown by this KME.'}, 400
        
        is_this_sae_slave = slave_sae_id == os.getenv('ATTACHED_SAE_ID')
        master_sae_id = kme[1] if is_this_sae_slave else os.getenv('ATTACHED_SAE_ID')
        
        default_key_size_bytes = int(os.getenv('DEFAULT_KEY_SIZE'))
        max_key_size_bytes = int(os.getenv('MAX_KEY_SIZE'))
        min_key_size_bytes = int(os.getenv('MIN_KEY_SIZE'))
        
        return {
            'source_KME_ID': kme[0] if is_this_sae_slave else os.getenv('KME_ID'),
            'target_KME_ID': os.getenv('KME_ID') if is_this_sae_slave else kme[0],
            'master_SAE_ID': master_sae_id,
            'slave_SAE_ID': slave_sae_id,
            'key_size': default_key_size_bytes * 8,
            'stored_key_count': len(
                self.key_store.get_keys(master_sae_id, slave_sae_id)
                + self.key_store.get_keys(slave_sae_id, master_sae_id)
            ),
            'max_key_count': int(os.getenv('MAX_KEY_COUNT')),
            'max_key_per_request': int(os.getenv('MAX_KEYS_PER_REQUEST')),
            'max_key_size': max_key_size_bytes * 8,
            'min_key_size': min_key_size_bytes * 8,
            'max_SAE_ID_count': 0
        }

    def get_key(self, request: flask.Request, slave_sae_id: str):
        security.ensure_valid_sae_id(request)
        
        if request.method == 'POST':
            data = request.get_json()
            number_of_keys = data.get('number', 1)
            key_size = data.get('size', int(os.getenv('DEFAULT_KEY_SIZE')) * 8)
        else:
            number_of_keys = 1
            key_size = int(os.getenv('DEFAULT_KEY_SIZE'))
        
        if number_of_keys > int(os.getenv('MAX_KEYS_PER_REQUEST')):
            return {'message': 'Number of requested keys exceed allowed max.'}, 400
        
        max_key_size_bits = int(os.getenv('MAX_KEY_SIZE')) * 8
        min_key_size_bits = int(os.getenv('MIN_KEY_SIZE')) * 8
        
        if key_size > max_key_size_bits:
            return {'message': 'The requested key size is too large.'}, 400
        if key_size < min_key_size_bits:
            return {'message': 'The requested key size is too small.'}, 400
        
        kme = self.scanner.find_kme(slave_sae_id)
        if kme is None:
            print(f'[ENC_KEYS] SAE {slave_sae_id} not discovered - using direct mode')
            master_sae_id = os.getenv('ATTACHED_SAE_ID')
        else:
            is_this_sae_slave = slave_sae_id == os.getenv('ATTACHED_SAE_ID')
            master_sae_id = kme[1] if is_this_sae_slave else os.getenv('ATTACHED_SAE_ID')
        
        stored_keys = self.key_store.get_keys(master_sae_id, slave_sae_id)
        if len(stored_keys) + number_of_keys > int(os.getenv('MAX_KEY_COUNT')):
            return {'message': 'Too many keys would be stored.'}, 400
        
        keys = []
        acquire_timeout = float(os.getenv('KEY_ACQUIRE_TIMEOUT', '5'))
        
        for i in range(number_of_keys):
            key = self.key_store.get_new_key(key_size, timeout=acquire_timeout, remove=False)
            if key is None:
                return {'message': 'Timed out waiting for quantum keys.'}, 503
            keys.append(key)
        
        print(f'[ENC_KEYS] Generated {len(keys)} keys')
        self.key_store.append_keys(master_sae_id, slave_sae_id, keys, do_broadcast=True)
        
        return {'keys': keys}

    def get_key_with_ids(self, request: flask.Request, master_sae_id: str):
        security.ensure_valid_sae_id(request)
        
        use_https = os.getenv('USE_HTTPS', 'false').lower() == 'true'
        
        if use_https:
            slave_sae_id = request.environ.get('client_cert_common_name', '')
        else:
            slave_sae_id = request.headers.get('X-SAE-ID', os.getenv('ATTACHED_SAE_ID', ''))
        
        try:
            if request.method == 'POST':
                data = request.get_json()
                requested_keys = [x['key_ID'] for x in data['key_IDs']]
            else:
                key_id_params = request.args.getlist('key_ID')
                if len(key_id_params) == 0:
                    all_keys = self.key_store.get_keys(master_sae_id, slave_sae_id)
                    requested_keys = [k['key_ID'] for k in all_keys]
                else:
                    requested_keys = []
                    for param in key_id_params:
                        requested_keys.extend(param.split(','))
            
            keys_master_to_slave = self.key_store.get_keys(master_sae_id, slave_sae_id)
            keys_slave_to_master = self.key_store.get_keys(slave_sae_id, master_sae_id)
            all_available_keys = keys_master_to_slave + keys_slave_to_master
            
            selected_keys = [k for k in all_available_keys if k['key_ID'] in requested_keys]
            
            # Check shared pool for missing keys
            found_ids = [k['key_ID'] for k in selected_keys]
            missing_ids = [kid for kid in requested_keys if kid not in found_ids]
            
            if missing_ids:
                print(f'[DEC_KEYS] {len(missing_ids)} keys missing, checking shared pool')
                if hasattr(self.key_store.key_pool, 'get_key_by_id'):
                    for key_id in missing_ids:
                        key = self.key_store.key_pool.get_key_by_id(key_id)
                        if key:
                            selected_keys.append(key)
            
        except (IndexError, KeyError) as e:
            return {'message': 'Invalid data format.'}, 400
        except Exception as e:
            return {'message': f'Error: {str(e)}'}, 400
        
        if len(selected_keys) == 0:
            return {'message': 'None of the requested keys exist.'}, 404
        
        if len(requested_keys) != len(selected_keys):
            return {'message': 'Some keys missing.', 'keys': selected_keys}, 206
        
        print(f'[DEC_KEYS] Removing {len(selected_keys)} keys (OTP consumption)')
        self.key_store.remove_keys(master_sae_id, slave_sae_id, selected_keys, do_broadcast=True)
        
        return {'keys': selected_keys}

    def mark_consumed(self, request: flask.Request):
        try:
            data = request.get_json()
            key_id = data.get('key_id')
            
            if not key_id:
                return {'message': 'Missing key_id'}, 400
            
            print(f'[MARK_CONSUMED] Request to consume key: {key_id}')
            
            try:
                from keys.shared_key_pool import get_shared_pool_server
                pool = get_shared_pool_server()
                kme_id = os.getenv('KME_ID', '1')
                
                key = pool.get_key_by_id(key_id, kme_id, remove=True)
                
                if key:
                    print(f'[MARK_CONSUMED] Successfully removed key {key_id}')
                    return {'message': 'Key consumed', 'key_id': key_id}, 200
                else:
                    return {'message': 'Key not found or already consumed'}, 404
            except Exception as e:
                print(f'[MARK_CONSUMED ERROR] Shared pool error: {e}')
                return {'message': f'Shared pool error: {str(e)}'}, 500
                
        except Exception as e:
            print(f'[MARK_CONSUMED ERROR] Unexpected error: {e}')
            return {'message': f'Error: {str(e)}'}, 500
