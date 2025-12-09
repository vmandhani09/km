import OpenSSL
import werkzeug.serving


class PeerCertWSGIRequestHandler(werkzeug.serving.WSGIRequestHandler):
    def make_environ(self):
        environ = super(PeerCertWSGIRequestHandler, self).make_environ()
        
        try:
            x509_binary = self.connection.getpeercert(True)
            if x509_binary:
                x509 = OpenSSL.crypto.load_certificate(
                    OpenSSL.crypto.FILETYPE_ASN1,
                    x509_binary
                )
                common_name = tuple(filter(
                    lambda x: x[0] == b'CN',
                    x509.get_subject().get_components()
                ))
                environ['client_cert_common_name'] = '' if len(common_name) == 0 else common_name[0][1].decode('utf-8')
                environ['client_cert_serial_number'] = x509.get_serial_number()
                environ['client_cert'] = x509
        except Exception as e:
            print(f'[RequestHandler] Error parsing client cert: {e}')
            environ['client_cert_common_name'] = ''
            environ['client_cert_serial_number'] = None
            environ['client_cert'] = None
        
        return environ
