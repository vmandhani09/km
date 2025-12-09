import os
import requests


class Broadcaster:
    def __init__(self):
        self.other_kmes = os.getenv('OTHER_KMES', '').split(',')
        self.timeout = float(os.getenv('NETWORK_TIMEOUT', '5'))
        
        # Check if certs are available
        use_https = os.getenv('USE_HTTPS', 'false').lower() == 'true'
        if use_https:
            cert_path = os.getenv('KME_CERT')
            key_path = os.getenv('KME_KEY')
            if cert_path and key_path and os.path.exists(cert_path) and os.path.exists(key_path):
                self.certs = (cert_path, key_path)
            else:
                self.certs = None
        else:
            self.certs = None

    def _broadcast(self, url: str, data: dict):
        for kme in self.other_kmes:
            kme = kme.strip()
            if not kme:
                continue
            try:
                print(f'[BROADCAST] Sending to {kme}{url}')
                
                cert_param = None
                if self.certs:
                    cert_param = self.certs
                else:
                    print(f'[BROADCAST] Proceeding without client cert')
                
                response = requests.post(
                    f'{kme}{url}',
                    verify=False,
                    cert=cert_param,
                    json=data,
                    timeout=self.timeout
                )
                print(f'[BROADCAST] Response status: {response.status_code}')
            except requests.exceptions.RequestException as exc:
                print(f'WARNING: Failed to broadcast to {kme}{url}: {exc}')

    def send_keys(self, master_sae_id: str, slave_sae_id: str, keys: list):
        self._broadcast(
            '/api/v1/kme/keys/exchange',
            {
                'master_sae_id': master_sae_id,
                'slave_sae_id': slave_sae_id,
                'keys': keys
            }
        )

    def remove_keys(self, master_sae_id: str, slave_sae_id: str, keys: list):
        self._broadcast(
            '/api/v1/kme/keys/remove',
            {
                'master_sae_id': master_sae_id,
                'slave_sae_id': slave_sae_id,
                'keys': keys
            }
        )
