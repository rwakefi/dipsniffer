#!/usr/bin/env python3
import http.server
import socketserver
import json
import os
import sys

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8077
DIRECTORY = os.path.expanduser("~/.config/dipsniffer/dashboard")
PAUSE_FILE = os.path.expanduser("~/.config/dipsniffer/pause_trading.flag")

class DashboardAPIHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DIRECTORY, **kwargs)

    def _update_status_json_paused(self, is_paused):
        status_file = os.path.join(DIRECTORY, "status.json")
        try:
            if os.path.exists(status_file):
                with open(status_file, 'r') as f:
                    data = json.load(f)
                data['is_paused'] = is_paused
                with open(status_file, 'w') as f:
                    json.dump(data, f, indent=2)
        except Exception as e:
            print(f"Warning: Could not update status.json: {e}")

    def do_POST(self):
        if self.path == '/api/pause':
            try:
                # Create the pause flag
                with open(PAUSE_FILE, 'w') as f:
                    f.write('{"paused": true}')
                
                self._update_status_json_paused(True)

                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(b'{"status": "paused"}')
            except Exception as e:
                self.send_error(500, f"Error pausing: {e}")
            return
        elif self.path == '/api/resume':
            try:
                # Remove the pause flag
                if os.path.exists(PAUSE_FILE):
                    os.remove(PAUSE_FILE)
                
                self._update_status_json_paused(False)

                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(b'{"status": "resumed"}')
            except Exception as e:
                self.send_error(500, f"Error resuming: {e}")
            return
            
        self.send_error(404, "Not Found")


# Ensure dashboard dir exists
os.makedirs(DIRECTORY, exist_ok=True)

with socketserver.TCPServer(("", PORT), DashboardAPIHandler) as httpd:
    print(f"Serving at port {PORT}")
    httpd.serve_forever()
