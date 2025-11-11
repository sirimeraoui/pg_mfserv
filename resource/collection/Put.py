


def put_collection(self, collectionId,connection, cursor):
    content_length = int(self.headers['Content-Length'])
    put_data = self.rfile.read(content_length)

    try:
        data_dict = json.loads(put_data)
        collectionId = collectionId.replace("'", "")

        cursor.execute(sql.SQL("UPDATE public.{table} SET title=%s, description=%s, itemtype=%s").format(
            table=sql.Identifier(collectionId)),
            (data_dict.get('title'), data_dict.get('description'), data_dict.get('itemType')))
        connection.commit()
        # Rows were updated successfully
        self.send_response(204)
        self.send_header("Content-type", "application/json")
        self.end_headers()
    except Exception as e:
        self.handle_error(404 if 'does not exist' in str(e) else 500,
                            'no collection was found' if 'does not exist' in str(e) else 'Server internal error')