from flask import Flask, Response
import psycopg2
import psycopg2.extras

app = Flask(__name__)
db = psycopg2.connect(host='localhost', dbname='xapi', user='xapi', password='xapi')
psycopg2.extras.register_hstore(db)

def stream_osm_data(cursor):
    """Streams OSM data from psql temp tables."""
    yield '<?xml version="1.0" encoding="UTF-8"?>\n'
    yield '<osm version="0.6" generator="pyxapi" copyright="OpenStreetMap and contributors" attribution="http://www.openstreetmap.org/copyright" license="http://opendatacommons.org/licenses/odbl/1-0/">\n'

    cursor.execute("SELECT id, version, changeset_id, ST_X(geom) as longitude, ST_Y(geom) as latitude, user_id, tstamp, tags FROM bbox_nodes ORDER BY id")

    for row in cursor:
        tags = row.get('tags', {})
        yield '<node id="{id}" version="{version}" changeset="{changeset_id}" lat="{latitude}" lon="{longitude}" uid="{user_id}" visible="true" timestamp="{timestamp}"'.format(timestamp=row.get('tstamp').isoformat(), **row).encode('utf-8')

        if tags:
            yield '>\n'

            for tag in tags.iteritems():
                print tag
                yield "<tag k='{}' v='{}'/>\n".format(*tag).encode('utf-8')

            yield "</node>\n"
        else:
            yield '/>\n'


    cursor.execute("SELECT * FROM bbox_ways ORDER BY id")

    for row in cursor:
        tags = row.get('tags', {})
        yield '<way id="{id}" version="{version}" changeset="{changeset_id}" uid="{user_id}" visible="true" timestamp="{timestamp}"'.format(timestamp=row.get('tstamp').isoformat(), **row).encode('utf-8')

        if tags:
            yield '>\n'

            for tag in tags.iteritems():
                yield "<tag k='{}' v='{}'/>\n".format(*tag).encode('utf-8')

            yield "</way>\n"
        else:
            yield '/>\n'

    cursor.execute("SELECT * FROM bbox_relations ORDER BY id")

    for row in cursor:
        tags = row.get('tags', {})
        yield '<relation id="{id}" version="{version}" changeset="{changeset_id}" uid="{user_id}" visible="true" timestamp="{timestamp}">\n'.format(timestamp=row.get('tstamp').isoformat(), **row).encode('utf-8')

        if tags:
            yield '>\n'
        else:
            yield '/>\n'

        for tag in tags.iteritems():
            yield "<tag k='{}' v='{}'/>\n".format(*tag).encode('utf-8')

        yield "</relation>\n"

    yield '</osm>\n'

    # Remove the temp tables
    cursor.connection.rollback()

@app.route("/api/capabilities")
def capabilities():
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<osm version="0.6" generator="pyxapi" copyright="OpenStreetMap and contributors" attribution="http://www.openstreetmap.org/copyright" license="http://opendatacommons.org/licenses/odbl/1-0/">
  <api>
    <version minimum="0.6" maximum="0.6"/>
    <area maximum="0.25"/>
    <timeout seconds="300"/>
  </api>
</osm>"""
    return Response(xml, mimetype='text/xml')

@app.route("/api/0.6/node/<string:ids>")
def nodes(ids):
    ids = ids.split(',')

    cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cursor.execute("""CREATE TEMPORARY TABLE bbox_nodes ON COMMIT DROP AS
                        SELECT *
                        FROM nodes
                        WHERE id IN %s""", (tuple(ids),))

    if cursor.rowcount < 1:
        return Response('Node %s not found.' % ids, status=404)

    cursor.execute("""CREATE TEMPORARY TABLE bbox_ways ON COMMIT DROP AS SELECT * FROM nodes WHERE FALSE""")

    cursor.execute("""CREATE TEMPORARY TABLE bbox_relations ON COMMIT DROP AS SELECT * FROM relations WHERE FALSE""")

    return Response(stream_osm_data(cursor), mimetype='text/xml')

@app.route("/api/0.6/way/<string:ids>")
def ways(ids):
    ids = ids.split(',')

    cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cursor.execute("""CREATE TEMPORARY TABLE bbox_nodes ON COMMIT DROP AS
                        SELECT *
                        FROM nodes
                        WHERE FALSE""")

    cursor.execute("""CREATE TEMPORARY TABLE bbox_ways ON COMMIT DROP AS
                        SELECT *
                        FROM ways
                        WHERE id IN %s""", (tuple(ids),))

    if cursor.rowcount < 1:
        return Response('Way %s not found.' % ids, status=404)

    cursor.execute("""ANALYZE bbox_ways""")

    cursor.execute("""CREATE TEMPORARY TABLE bbox_way_nodes (id bigint) ON COMMIT DROP""")
    cursor.execute("""SELECT unnest_bbox_way_nodes()""")
    cursor.execute("""CREATE TEMPORARY TABLE bbox_missing_way_nodes ON COMMIT DROP AS
                SELECT buwn.id FROM (SELECT DISTINCT bwn.id FROM bbox_way_nodes bwn) buwn
                WHERE NOT EXISTS (
                    SELECT * FROM bbox_nodes WHERE id = buwn.id
                );""")
    cursor.execute("""ALTER TABLE ONLY bbox_missing_way_nodes
                ADD CONSTRAINT pk_bbox_missing_way_nodes PRIMARY KEY (id)""")
    cursor.execute("""ANALYZE bbox_missing_way_nodes""")
    cursor.execute("""INSERT INTO bbox_nodes
                SELECT n.* FROM nodes n INNER JOIN bbox_missing_way_nodes bwn ON n.id = bwn.id;""")

    cursor.execute("""ANALYZE bbox_nodes""")

    cursor.execute("""CREATE TEMPORARY TABLE bbox_relations ON COMMIT DROP AS SELECT * FROM relations WHERE FALSE""")

    return Response(stream_osm_data(cursor), mimetype='text/xml')

@app.route("/api/0.6/relation/<string:ids>")
def relations(ids):
    ids = ids.split(',')

    cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cursor.execute("""CREATE TEMPORARY TABLE bbox_nodes ON COMMIT DROP AS
                        SELECT *
                        FROM nodes
                        WHERE FALSE""")

    cursor.execute("""CREATE TEMPORARY TABLE bbox_ways ON COMMIT DROP AS
                        SELECT *
                        FROM ways
                        WHERE FALSE""")

    cursor.execute("""CREATE TEMPORARY TABLE bbox_relations ON COMMIT DROP AS
                        SELECT *
                        FROM relations
                        WHERE id IN %s""", (tuple(ids),))

    if cursor.rowcount < 1:
        return Response('Relation %s not found.' % ids, status=404)

    return Response(stream_osm_data(cursor), mimetype='text/xml')

if __name__ == "__main__":
    app.run(debug=True)