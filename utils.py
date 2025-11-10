from http.server import BaseHTTPRequestHandler, HTTPServer
import json
def column_discovery(collectionId, cursor):
    sqlString = f"SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS where  table_name = '{collectionId}';"
    cursor.execute(sqlString)
    rs = cursor.fetchall()
    return rs

def column_discovery2(collectionId, cursor):
    sqlString = f"SELECT column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS where  table_name = '{collectionId}';"
    cursor.execute(sqlString)
    rs = cursor.fetchall()
    return rs

def send_json_response(self,status_code, json_data):
    self.send_response(status_code)
    self.send_header("Content-type", "application/json")
    self.end_headers()
    self.wfile.write(json_data.encode('utf-8'))
def handle_error(self,code, message):
    # Format error information into a JSON string
    error_response = json.dumps({"code": str(code), "description": message})

    # Send the JSON response
    self.send_response(code)
    self.send_header("Content-type", "application/json")
    self.end_headers()
    self.wfile.write(error_response.encode("utf-8"))
