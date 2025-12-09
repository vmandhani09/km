import os
import threading
import flask
from flask import Flask
from keys.shared_key_pool import get_shared_pool_server, create_pool_client
from keys.key_store import KeyStore
from network.broadcaster import Broadcaster
from network.scanner import Scanner
from router.external import External
from router.internal import Internal
from server import tls
from server.request_handler import PeerCertWSGIRequestHandler


class App:
    def __init__(self, app: Flask):
        self.app = app
        
        # Get KME ID
        self.kme_id = os.getenv('KME_ID', '1')
        print(f"[APP] Initializing KME{self.kme_id}")
        
        self.kme_list = []
        self.kme_lock = threading.Lock()
        self.scanner = Scanner(self.kme_list, self.kme_lock)
        self.gen_lock = threading.Lock()
        
        # Use shared key pool instead of individual pool
        # Both KME1 and KME2 share the same pool
        # KME1 generates, KME2 retrieves
        self.key_pool = create_pool_client(self.kme_id, self.gen_lock)
        print(f"[APP] KME{self.kme_id} using shared key pool")
        
        self.broadcaster = Broadcaster()
        self.key_store = KeyStore(self.key_pool, self.broadcaster)
        
        self.external_routes = External(self.scanner, self.key_store)
        self.internal_routes = Internal(self.key_store)

    def start(self):
        scanner_thread = threading.Thread(target=self.scanner.start, daemon=True)
        scanner_thread.start()
        
        key_pool_thread = threading.Thread(target=self.key_pool.start, daemon=True)
        key_pool_thread.start()
        
        print(f"[APP] KME{self.kme_id} threads started")
        self.__run()

    def stop(self):
        self.scanner.stop.set()
        
        # Stop shared pool only from KME1
        if self.kme_id == "1":
            pool_server = get_shared_pool_server()
            pool_server.stop.set()
            print(f"[APP] KME{self.kme_id} stopped shared pool generation")

    def before_request(self):
        # Don't acquire kme_lock on every request - this causes deadlocks
        # The scanner and key_store operations use locks internally when needed
        pass

    def after_request(self, response: flask.Response):
        # No locks to release since we don't acquire them in before_request
        return response

    def __run(self):
        use_https = os.getenv('USE_HTTPS', 'true').lower() == 'true'
        
        if use_https:
            self.app.run(
                host=os.getenv('HOST'),
                port=os.getenv('PORT'),
                ssl_context=tls.create_ssl_context(),
                request_handler=PeerCertWSGIRequestHandler
            )
        else:
            self.app.run(
                host=os.getenv('HOST'),
                port=os.getenv('PORT')
            )
