"""
panel/serve.py
--------------
Paneli yerel ağda sunar. Telefon/tablet da aynı WiFi'deyse
http://BİLGİSAYAR_IP:8765 adresinden erişebilir.

Çalıştırma:
  cd "Bölge Karmaları 2026/panel"
  python serve.py

Edirne yarışı sırasında generate_rankings_json.py'yi ayrı bir terminalde
döngüyle çalıştırın; panel otomatik güncellenir.
"""

import http.server, socketserver, os, socket

PORT = 8765
DIR  = os.path.dirname(os.path.abspath(__file__))


def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except Exception:
        return "localhost"
    finally:
        s.close()


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DIR, **kwargs)

    def log_message(self, format, *args):
        pass   # Sessiz mod


if __name__ == "__main__":
    ip = get_local_ip()
    with socketserver.TCPServer(("", PORT), Handler) as httpd:
        print(f"Panel sunuluyor:")
        print(f"  Bilgisayar:  http://localhost:{PORT}")
        print(f"  Ağdan:       http://{ip}:{PORT}")
        print(f"\n  Paneli tarayıcıda aç: http://localhost:{PORT}")
        print(f"  Durdurmak için: Ctrl+C\n")
        httpd.serve_forever()
