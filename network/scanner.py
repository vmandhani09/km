import os
import threading
import time
import requests


class Scanner:
    def __init__(self, kme_list: list, kme_lock: threading.Lock):
        self.kme_list = kme_list
        self.kme_lock = kme_lock
        self.stop = threading.Event()
        self.other_kmes = os.getenv('OTHER_KMES', '').split(',')
        self.timeout = float(os.getenv('NETWORK_TIMEOUT', '5'))
        self.scan_interval = float(os.getenv('SCAN_INTERVAL', '30'))

    def start(self):
        """Start scanning for other KMEs"""
        print('[SCANNER] Starting KME scanner')
        
        while not self.stop.is_set():
            self._scan_kmes()
            self.stop.wait(self.scan_interval)
        
        print('[SCANNER] Scanner stopped')

    def _scan_kmes(self):
        """Scan all configured KMEs for their attached SAEs"""
        for kme_url in self.other_kmes:
            kme_url = kme_url.strip()
            if not kme_url:
                continue
            
            try:
                response = requests.get(
                    f'{kme_url}/api/v1/kme/status',
                    timeout=self.timeout,
                    verify=False
                )
                
                if response.status_code == 200:
                    data = response.json()
                    kme_id = data.get('KME_ID')
                    sae_id = data.get('ATTACHED_SAE_ID')
                    
                    if kme_id and sae_id:
                        with self.kme_lock:
                            # Update or add KME entry
                            existing = [k for k in self.kme_list if k.get('KME_ID') == kme_id]
                            if not existing:
                                self.kme_list.append({
                                    'KME_ID': kme_id,
                                    'KME_URL': kme_url,
                                    'SAE_ID': sae_id
                                })
                                print(f'[SCANNER] Discovered KME: {kme_id} with SAE: {sae_id}')
                            else:
                                existing[0]['SAE_ID'] = sae_id
                                existing[0]['KME_URL'] = kme_url
            except requests.exceptions.RequestException as e:
                print(f'[SCANNER] Failed to contact {kme_url}: {e}')

    def find_kme(self, sae_id: str):
        """Find KME information by SAE ID"""
        with self.kme_lock:
            for kme in self.kme_list:
                if kme.get('SAE_ID') == sae_id:
                    return (kme.get('KME_ID'), kme.get('SAE_ID'), kme.get('KME_URL'))
        
        print(f'[SCANNER] SAE ID {sae_id} not found in discovered KMEs')
        return None
