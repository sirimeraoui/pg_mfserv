
def delete_collection(self, collectionId,connection, cursor):
    try:
        cursor.execute("DROP TABLE IF EXISTS public.%s" % collectionId)
        connection.commit()
        self.send_response(204)
        self.send_header("Content-type", "application/json")
        self.end_headers()
    except Exception as e:
        self.handle_error(500, str(e))