import os
import flask
from keys.key_store import KeyStore
from keys.shared_key_pool import get_shared_pool_server


class Internal:
    def __init__(self, key_store: KeyStore):
        self.key_store = key_store

    def get_kme_status(self):
        """Return KME status information"""
        pool = get_shared_pool_server()
        status = pool.get_status()
        
        return {
            'KME_ID': os.getenv('KME_ID'),
            'ATTACHED_SAE_ID': os.getenv('ATTACHED_SAE_ID'),
            'pool_status': status
        }

    def get_key_pool(self):
        """Return key pool status"""
        pool = get_shared_pool_server()
        return pool.get_status()

    def get_shared_key(self, request: flask.Request):
        """Get keys from shared pool for KME2"""
        data = request.get_json()
        kme_id = data.get('kme_id', '2')
        count = data.get('count', 1)
        
        pool = get_shared_pool_server()
        keys = pool.get_keys(count, kme_id, timeout=10.0, remove=False)
        
        return {'keys': keys}

    def get_reserved_key_by_id(self, request: flask.Request):
        """Get a specific key by ID from shared pool"""
        data = request.get_json()
        key_id = data.get('key_id')
        kme_id = data.get('kme_id', '2')
        remove = data.get('remove', True)
        
        if not key_id:
            return {'message': 'Missing key_id'}, 400
        
        pool = get_shared_pool_server()
        key = pool.get_key_by_id(key_id, kme_id, remove=remove)
        
        if key:
            return {'key': key}
        else:
            return {'message': 'Key not found'}, 404

    def do_kme_key_exchange(self, request: flask.Request):
        """Receive keys from another KME"""
        data = request.get_json()
        return self.key_store.append_keys(
            data['master_sae_id'],
            data['slave_sae_id'],
            data['keys'],
            do_broadcast=False
        )

    def do_remove_kme_key(self, request: flask.Request):
        """Remove keys (received from another KME)"""
        data = request.get_json()
        self.key_store.remove_keys(
            data['master_sae_id'],
            data['slave_sae_id'],
            data['keys'],
            do_broadcast=False
        )
        return {'message': 'Keys have been removed from the local key store.'}
