"""
QKD Key Pool Router - Persistent MongoDB-backed key block management.

New endpoints for bulk key generation, pending key queries, and key fetching.
These extend the existing Next-Door-Key-Simulator without breaking backward compatibility.
"""

import os
import base64
import uuid
from datetime import datetime
from typing import Optional
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend
import secrets

import flask
from flask import jsonify

from db.mongo import QkdBlock, is_mongo_available, get_mongo_client


# Constants
MAX_BLOCKS_PER_REQUEST = 10000
KEY_BLOCK_SIZE_BYTES = 1024  # Each key block is exactly 1KB


def generate_key_block() -> tuple:
    """
    Generate a single 1KB (1024 bytes) key block.
    Returns (key_id, key_data_base64)
    """
    key_id = str(uuid.uuid4())
    key_data = secrets.token_bytes(KEY_BLOCK_SIZE_BYTES)
    key_data_b64 = base64.b64encode(key_data).decode('utf-8')
    return key_id, key_data_b64


class QkdPoolRouter:
    """
    Router for QKD key pool operations with MongoDB persistence.
    
    Endpoints:
    - POST /qkd/keys/pool - Request multiple key blocks (sender)
    - GET /qkd/keys/pending - Query pending keys (receiver)
    - POST /qkd/keys/fetch - Fetch keys by IDs (receiver)
    - GET /qkd/keys/status - Check pool and MongoDB status
    """
    
    def __init__(self):
        # Initialize MongoDB connection on first use
        self._mongo_initialized = False
    
    def _ensure_mongo(self) -> bool:
        """Ensure MongoDB is connected."""
        if not self._mongo_initialized:
            get_mongo_client()
            self._mongo_initialized = True
        return is_mongo_available()
    
    def request_key_pool(self, request: flask.Request):
        """
        POST /qkd/keys/pool
        
        Request multiple 1KB key blocks for a sender-receiver pair.
        Blocks are stored in MongoDB for later retrieval by the receiver.
        
        Request body:
        {
            "senderId": "sender-sae-id",
            "receiverId": "receiver-sae-id", 
            "count": 100  // Number of 1KB blocks to generate (max 10000)
        }
        
        Response:
        {
            "success": true,
            "senderId": "...",
            "receiverId": "...",
            "count": 100,
            "keyIds": ["uuid1", "uuid2", ...],
            "blockSizeBytes": 1024
        }
        """
        try:
            if not self._ensure_mongo():
                return jsonify({
                    'success': False,
                    'error': 'MongoDB not available. Set MONGODB_URI environment variable.'
                }), 503
            
            data = request.get_json()
            if not data:
                return jsonify({
                    'success': False,
                    'error': 'Missing JSON body'
                }), 400
            
            sender_id = data.get('senderId')
            receiver_id = data.get('receiverId')
            count = data.get('count', 1)
            
            # Validation
            if not sender_id:
                return jsonify({
                    'success': False,
                    'error': 'Missing senderId'
                }), 400
            
            if not receiver_id:
                return jsonify({
                    'success': False,
                    'error': 'Missing receiverId'
                }), 400
            
            if not isinstance(count, int) or count < 1:
                return jsonify({
                    'success': False,
                    'error': 'count must be a positive integer'
                }), 400
            
            if count > MAX_BLOCKS_PER_REQUEST:
                return jsonify({
                    'success': False,
                    'error': f'count exceeds maximum allowed ({MAX_BLOCKS_PER_REQUEST})'
                }), 400
            
            print(f"[QKD_POOL] Generating {count} key blocks for {sender_id} -> {receiver_id}")
            
            # Generate key blocks
            blocks = []
            key_ids = []
            created_at = datetime.utcnow()
            
            for _ in range(count):
                key_id, key_data_b64 = generate_key_block()
                block = QkdBlock(
                    key_id=key_id,
                    sender_id=sender_id,
                    receiver_id=receiver_id,
                    key_data=key_data_b64,
                    delivered_to_receiver=False,
                    created_at=created_at
                )
                blocks.append(block)
                key_ids.append(key_id)
            
            # Bulk insert to MongoDB
            inserted = QkdBlock.bulk_insert(blocks)
            
            if inserted != count:
                print(f"[QKD_POOL] WARNING: Requested {count} but inserted {inserted}")
            
            print(f"[QKD_POOL] Generated and stored {inserted} key blocks")
            
            # Check if sender wants key data returned (for local storage)
            include_keys = data.get('includeKeys', True)  # Default to include for sender
            
            response_data = {
                'success': True,
                'senderId': sender_id,
                'receiverId': receiver_id,
                'count': inserted,
                'keyIds': key_ids[:inserted],
                'blockSizeBytes': KEY_BLOCK_SIZE_BYTES
            }
            
            # Include key data for sender's local storage
            if include_keys:
                response_data['keys'] = [
                    {
                        'keyId': b.key_id,
                        'keyData': b.key_data,
                        'senderId': sender_id,
                        'receiverId': receiver_id
                    }
                    for b in blocks[:inserted]
                ]
            
            return jsonify(response_data), 201
            
        except Exception as e:
            print(f"[QKD_POOL] Error in request_key_pool: {e}")
            import traceback
            traceback.print_exc()
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    def get_pending_keys(self, request: flask.Request):
        """
        GET /qkd/keys/pending
        
        Query pending (undelivered) key IDs for a receiver.
        
        Query parameters:
        - receiverId (required): The receiver's SAE ID
        - senderId (optional): Filter by specific sender
        - limit (optional): Max number of keyIds to return (default 1000)
        
        Response:
        {
            "success": true,
            "receiverId": "...",
            "senderId": "..." or null,
            "pendingCount": 150,
            "pendingKeyIds": ["uuid1", "uuid2", ...]
        }
        """
        try:
            if not self._ensure_mongo():
                return jsonify({
                    'success': False,
                    'error': 'MongoDB not available'
                }), 503
            
            receiver_id = request.args.get('receiverId')
            sender_id = request.args.get('senderId')  # Optional
            limit = request.args.get('limit', 1000, type=int)
            
            if not receiver_id:
                return jsonify({
                    'success': False,
                    'error': 'Missing receiverId query parameter'
                }), 400
            
            if limit < 1 or limit > 10000:
                limit = 1000
            
            print(f"[QKD_POOL] Querying pending keys for receiver={receiver_id}, sender={sender_id}")
            
            # Get pending key IDs
            pending_ids = QkdBlock.find_pending_for_receiver(
                receiver_id=receiver_id,
                sender_id=sender_id,
                limit=limit
            )
            
            # Get total count
            total_pending = QkdBlock.count_pending(receiver_id, sender_id)
            
            print(f"[QKD_POOL] Found {len(pending_ids)} pending keys (total: {total_pending})")
            
            return jsonify({
                'success': True,
                'receiverId': receiver_id,
                'senderId': sender_id,
                'pendingCount': total_pending,
                'pendingKeyIds': pending_ids
            }), 200
            
        except Exception as e:
            print(f"[QKD_POOL] Error in get_pending_keys: {e}")
            import traceback
            traceback.print_exc()
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    def fetch_keys(self, request: flask.Request):
        """
        POST /qkd/keys/fetch
        
        Fetch multiple keys by their IDs. Marks them as delivered.
        
        Request body:
        {
            "receiverId": "receiver-sae-id",
            "senderId": "sender-sae-id" (optional),
            "keyIds": ["uuid1", "uuid2", ...]
        }
        
        Response:
        {
            "success": true,
            "receiverId": "...",
            "keys": [
                {"keyId": "uuid1", "keyData": "base64...", "senderId": "..."},
                ...
            ],
            "fetchedCount": 10,
            "missingKeyIds": ["uuid-not-found", ...]
        }
        """
        try:
            if not self._ensure_mongo():
                return jsonify({
                    'success': False,
                    'error': 'MongoDB not available'
                }), 503
            
            data = request.get_json()
            if not data:
                return jsonify({
                    'success': False,
                    'error': 'Missing JSON body'
                }), 400
            
            receiver_id = data.get('receiverId')
            sender_id = data.get('senderId')  # Optional
            key_ids = data.get('keyIds', [])
            
            if not receiver_id:
                return jsonify({
                    'success': False,
                    'error': 'Missing receiverId'
                }), 400
            
            if not isinstance(key_ids, list) or len(key_ids) == 0:
                return jsonify({
                    'success': False,
                    'error': 'keyIds must be a non-empty array'
                }), 400
            
            if len(key_ids) > MAX_BLOCKS_PER_REQUEST:
                return jsonify({
                    'success': False,
                    'error': f'Too many keyIds (max {MAX_BLOCKS_PER_REQUEST})'
                }), 400
            
            print(f"[QKD_POOL] Fetching {len(key_ids)} keys for receiver={receiver_id}")
            
            # Fetch keys (this also marks them as delivered)
            fetched_keys = QkdBlock.fetch_keys_by_ids(
                receiver_id=receiver_id,
                key_ids=key_ids,
                sender_id=sender_id
            )
            
            # Find which keys were missing
            fetched_ids = {k['keyId'] for k in fetched_keys}
            missing_ids = [kid for kid in key_ids if kid not in fetched_ids]
            
            print(f"[QKD_POOL] Fetched {len(fetched_keys)} keys, {len(missing_ids)} missing")
            
            return jsonify({
                'success': True,
                'receiverId': receiver_id,
                'keys': fetched_keys,
                'fetchedCount': len(fetched_keys),
                'missingKeyIds': missing_ids
            }), 200
            
        except Exception as e:
            print(f"[QKD_POOL] Error in fetch_keys: {e}")
            import traceback
            traceback.print_exc()
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    def get_pool_status(self, request: flask.Request):
        """
        GET /qkd/keys/pool/status
        
        Get status of the QKD key pool and MongoDB connection.
        
        Response:
        {
            "success": true,
            "mongoConnected": true,
            "kmeId": "...",
            "blockSizeBytes": 1024,
            "maxBlocksPerRequest": 10000
        }
        """
        try:
            mongo_available = self._ensure_mongo()
            
            return jsonify({
                'success': True,
                'mongoConnected': mongo_available,
                'kmeId': os.getenv('KME_ID', 'unknown'),
                'blockSizeBytes': KEY_BLOCK_SIZE_BYTES,
                'maxBlocksPerRequest': MAX_BLOCKS_PER_REQUEST
            }), 200
            
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500


# Global router instance
_qkd_pool_router: Optional[QkdPoolRouter] = None


def get_qkd_pool_router() -> QkdPoolRouter:
    """Get or create the QKD pool router singleton."""
    global _qkd_pool_router
    if _qkd_pool_router is None:
        _qkd_pool_router = QkdPoolRouter()
    return _qkd_pool_router
