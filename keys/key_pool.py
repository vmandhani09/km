import os
import threading
import time
from typing import Optional, Dict
from keys.key_generator import KeyGenerator


class KeyPool:
    def __init__(self, gen_lock: threading.Lock):
        self.keys: list[dict[str, str]] = []
        self.lock = gen_lock
        self.condition = threading.Condition(self.lock)
        self.stop = threading.Event()
        self.default_key_size = int(os.getenv('DEFAULT_KEY_SIZE'))
        self.max_key_count = int(os.getenv('MAX_KEY_COUNT'))
        self.generate_interval = float(os.getenv('KEY_GEN_SEC_TO_GEN'))
        self.acquire_timeout = float(os.getenv('KEY_ACQUIRE_TIMEOUT', '5'))
        self.batch_size = max(1, int(os.getenv('KEY_GEN_BATCH_SIZE', '1')))

    def _add_key_unlocked(self) -> None:
        self.keys.append(KeyGenerator.generate_key(self.default_key_size))

    def add_key(self) -> None:
        with self.condition:
            self._add_key_unlocked()
            self.condition.notify_all()

    def get_key(self, key_size: int, timeout: Optional[float] = None, remove: bool = False) -> Optional[Dict[str, str]]:
        """
        Get a key from the pool or generate a custom-sized key.
        
        Args:
            key_size: Key size in BITS (will be converted to bytes for generation)
            timeout: Timeout in seconds
            remove: If True, remove key from pool (OTP consumption). If False, copy key (for enc_keys).
        """
        configured_timeout = timeout if timeout is not None else self.acquire_timeout
        wait_timeout = None if configured_timeout is None or configured_timeout <= 0 else configured_timeout
        deadline = time.monotonic() + wait_timeout if wait_timeout is not None else None
        
        default_key_size_bits = self.default_key_size * 8
        
        with self.condition:
            if key_size and key_size != default_key_size_bits:
                key_size_bytes = (key_size + 7) // 8
                print(f'INFO: Generating key not from pool, for different size request: {key_size} bits ({key_size_bytes} bytes)')
                return KeyGenerator.generate_key(key_size_bytes)
            
            while len(self.keys) == 0:
                if wait_timeout is None:
                    self.condition.wait()
                else:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        print('WARNING: Timed out waiting for key from pool')
                        return None
                    self.condition.wait(remaining)
            
            if remove:
                key = self.keys.pop()
                print(f'INFO: Removing key from pool for OTP consumption ({len(self.keys)}/{self.max_key_count})')
            else:
                key = self.keys[0].copy()
                print(f'INFO: Copying key from pool for enc_keys ({len(self.keys)}/{self.max_key_count})')
            
            return key

    def start(self) -> None:
        while not self.stop.is_set():
            with self.condition:
                if len(self.keys) < self.max_key_count:
                    remaining_capacity = self.max_key_count - len(self.keys)
                    to_generate = min(self.batch_size, remaining_capacity)
                    if to_generate > 0:
                        for _ in range(to_generate):
                            self._add_key_unlocked()
                        print(f'INFO: Generated {to_generate} key(s) ({len(self.keys)}/{self.max_key_count})')
                        self.condition.notify_all()
                elif len(self.keys) > self.max_key_count:
                    print('INFO: Key pool size exceeded max, trimming extra keys')
                    while len(self.keys) > self.max_key_count:
                        self.keys.pop()
            self.stop.wait(self.generate_interval)
