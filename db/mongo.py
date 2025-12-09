"""
MongoDB connection and QkdBlock model for persistent QKD key storage.
Extends the Next-Door-Key-Simulator to support MongoDB-backed key blocks.
"""

import os
from datetime import datetime
from typing import Optional, List, Dict, Any
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import ConnectionFailure, OperationFailure
import uuid

# Global MongoDB client and database references
_client: Optional[MongoClient] = None
_db = None
_qkd_blocks_collection = None


def get_mongo_client() -> Optional[MongoClient]:
    """Get or create MongoDB client singleton."""
    global _client, _db, _qkd_blocks_collection
    
    if _client is not None:
        return _client
    
    mongodb_uri = os.getenv('MONGODB_URI')
    if not mongodb_uri:
        print("[MongoDB] WARNING: MONGODB_URI not set, MongoDB persistence disabled")
        return None
    
    try:
        _client = MongoClient(mongodb_uri, serverSelectionTimeoutMS=5000)
        # Test connection
        _client.admin.command('ping')
        
        # Get database (use 'qumail_kme' or extract from URI)
        db_name = os.getenv('MONGODB_DATABASE', 'qumail_kme')
        _db = _client[db_name]
        _qkd_blocks_collection = _db['qkd_blocks']
        
        # Create indexes for efficient queries
        _qkd_blocks_collection.create_index([('keyId', ASCENDING)], unique=True)
        _qkd_blocks_collection.create_index([('senderId', ASCENDING), ('receiverId', ASCENDING)])
        _qkd_blocks_collection.create_index([('receiverId', ASCENDING), ('deliveredToReceiver', ASCENDING)])
        _qkd_blocks_collection.create_index([('createdAt', DESCENDING)])
        
        print(f"[MongoDB] Connected successfully to {db_name}")
        return _client
    except ConnectionFailure as e:
        print(f"[MongoDB] Connection failed: {e}")
        _client = None
        return None
    except Exception as e:
        print(f"[MongoDB] Unexpected error: {e}")
        _client = None
        return None


def is_mongo_available() -> bool:
    """Check if MongoDB is available and connected."""
    client = get_mongo_client()
    return client is not None


def get_qkd_blocks_collection():
    """Get the qkd_blocks collection, initializing if needed."""
    global _qkd_blocks_collection
    if _qkd_blocks_collection is None:
        get_mongo_client()
    return _qkd_blocks_collection


class QkdBlock:
    """
    QKD Key Block model for MongoDB storage.
    
    Schema:
    - keyId: UUID string (unique identifier for the key block)
    - senderId: SAE ID of the sender who requested the key
    - receiverId: SAE ID of the intended receiver
    - keyData: Base64-encoded key material (exactly 1024 bytes when decoded)
    - deliveredToReceiver: Boolean flag indicating if receiver has fetched this key
    - createdAt: Timestamp when the key was generated
    """
    
    COLLECTION_NAME = 'qkd_blocks'
    KEY_SIZE_BYTES = 1024  # Each key block is exactly 1KB
    
    def __init__(
        self,
        key_id: str,
        sender_id: str,
        receiver_id: str,
        key_data: str,
        delivered_to_receiver: bool = False,
        created_at: Optional[datetime] = None
    ):
        self.key_id = key_id
        self.sender_id = sender_id
        self.receiver_id = receiver_id
        self.key_data = key_data  # Base64 encoded
        self.delivered_to_receiver = delivered_to_receiver
        self.created_at = created_at or datetime.utcnow()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to MongoDB document format."""
        return {
            'keyId': self.key_id,
            'senderId': self.sender_id,
            'receiverId': self.receiver_id,
            'keyData': self.key_data,
            'deliveredToReceiver': self.delivered_to_receiver,
            'createdAt': self.created_at
        }
    
    @classmethod
    def from_dict(cls, doc: Dict[str, Any]) -> 'QkdBlock':
        """Create QkdBlock from MongoDB document."""
        return cls(
            key_id=doc['keyId'],
            sender_id=doc['senderId'],
            receiver_id=doc['receiverId'],
            key_data=doc['keyData'],
            delivered_to_receiver=doc.get('deliveredToReceiver', False),
            created_at=doc.get('createdAt')
        )
    
    def save(self) -> bool:
        """Save this block to MongoDB."""
        collection = get_qkd_blocks_collection()
        if collection is None:
            return False
        try:
            collection.insert_one(self.to_dict())
            return True
        except Exception as e:
            print(f"[QkdBlock] Error saving block {self.key_id}: {e}")
            return False
    
    @classmethod
    def bulk_insert(cls, blocks: List['QkdBlock']) -> int:
        """Insert multiple blocks at once. Returns count of inserted blocks."""
        collection = get_qkd_blocks_collection()
        if collection is None or len(blocks) == 0:
            return 0
        try:
            docs = [b.to_dict() for b in blocks]
            result = collection.insert_many(docs, ordered=False)
            return len(result.inserted_ids)
        except Exception as e:
            print(f"[QkdBlock] Error bulk inserting {len(blocks)} blocks: {e}")
            return 0
    
    @classmethod
    def find_by_key_id(cls, key_id: str) -> Optional['QkdBlock']:
        """Find a single block by keyId."""
        collection = get_qkd_blocks_collection()
        if collection is None:
            return None
        doc = collection.find_one({'keyId': key_id})
        return cls.from_dict(doc) if doc else None
    
    @classmethod
    def find_pending_for_receiver(
        cls,
        receiver_id: str,
        sender_id: Optional[str] = None,
        limit: int = 1000
    ) -> List[str]:
        """
        Find pending (undelivered) keyIds for a receiver.
        Optionally filter by sender.
        Returns list of keyIds only.
        """
        collection = get_qkd_blocks_collection()
        if collection is None:
            return []
        
        query = {
            'receiverId': receiver_id,
            'deliveredToReceiver': False
        }
        if sender_id:
            query['senderId'] = sender_id
        
        try:
            cursor = collection.find(
                query,
                {'keyId': 1, '_id': 0}  # Project only keyId
            ).sort('createdAt', ASCENDING).limit(limit)
            return [doc['keyId'] for doc in cursor]
        except Exception as e:
            print(f"[QkdBlock] Error finding pending keys: {e}")
            return []
    
    @classmethod
    def fetch_keys_by_ids(
        cls,
        receiver_id: str,
        key_ids: List[str],
        sender_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch multiple keys by keyIds for a receiver.
        Marks them as delivered after fetching.
        Returns list of {keyId, keyData} dicts.
        """
        collection = get_qkd_blocks_collection()
        if collection is None:
            return []
        
        query = {
            'keyId': {'$in': key_ids},
            'receiverId': receiver_id
        }
        if sender_id:
            query['senderId'] = sender_id
        
        try:
            # Find matching keys
            cursor = collection.find(query)
            results = []
            found_ids = []
            
            for doc in cursor:
                results.append({
                    'keyId': doc['keyId'],
                    'keyData': doc['keyData'],
                    'senderId': doc['senderId']
                })
                found_ids.append(doc['keyId'])
            
            # Mark as delivered
            if found_ids:
                collection.update_many(
                    {'keyId': {'$in': found_ids}},
                    {'$set': {'deliveredToReceiver': True}}
                )
                print(f"[QkdBlock] Marked {len(found_ids)} keys as delivered")
            
            return results
        except Exception as e:
            print(f"[QkdBlock] Error fetching keys: {e}")
            return []
    
    @classmethod
    def count_pending(cls, receiver_id: str, sender_id: Optional[str] = None) -> int:
        """Count pending keys for a receiver."""
        collection = get_qkd_blocks_collection()
        if collection is None:
            return 0
        
        query = {
            'receiverId': receiver_id,
            'deliveredToReceiver': False
        }
        if sender_id:
            query['senderId'] = sender_id
        
        try:
            return collection.count_documents(query)
        except Exception as e:
            print(f"[QkdBlock] Error counting pending: {e}")
            return 0
    
    @classmethod
    def delete_by_key_id(cls, key_id: str) -> bool:
        """Delete a key block by keyId."""
        collection = get_qkd_blocks_collection()
        if collection is None:
            return False
        try:
            result = collection.delete_one({'keyId': key_id})
            return result.deleted_count > 0
        except Exception as e:
            print(f"[QkdBlock] Error deleting key {key_id}: {e}")
            return False
    
    @classmethod
    def cleanup_old_delivered(cls, days_old: int = 7) -> int:
        """Remove delivered keys older than specified days."""
        collection = get_qkd_blocks_collection()
        if collection is None:
            return 0
        
        from datetime import timedelta
        cutoff = datetime.utcnow() - timedelta(days=days_old)
        
        try:
            result = collection.delete_many({
                'deliveredToReceiver': True,
                'createdAt': {'$lt': cutoff}
            })
            if result.deleted_count > 0:
                print(f"[QkdBlock] Cleaned up {result.deleted_count} old delivered keys")
            return result.deleted_count
        except Exception as e:
            print(f"[QkdBlock] Error cleaning up old keys: {e}")
            return 0
