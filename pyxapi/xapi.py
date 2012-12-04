from flask import Flask, Response, request
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

def query_nodes(cursor, where_str, where_obj=None):
    cursor.execute("""CREATE TEMPORARY TABLE bbox_nodes ON COMMIT DROP AS
                        SELECT *
                        FROM nodes
                        WHERE %s""" % where_str, where_obj)
    print cursor.query

def query_ways(cursor, where_str, where_obj=None):
    cursor.execute("""CREATE TEMPORARY TABLE bbox_ways ON COMMIT DROP AS
                        SELECT *
                        FROM ways
                        WHERE %s""" % where_str, where_obj)
    print cursor.query

def query_relations(cursor, where_str, where_obj=None):
    cursor.execute("""CREATE TEMPORARY TABLE bbox_relations ON COMMIT DROP AS
                        SELECT *
                        FROM relations
                        WHERE %s""" % where_str, where_obj)
    print cursor.query

def backfill_way_nodes(cursor):
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

def backfill_relations(cursor):
    cursor.execute("""CREATE TEMPORARY TABLE bbox_relations ON COMMIT DROP AS
                     SELECT r.* FROM relations r
                     INNER JOIN (
                        SELECT relation_id FROM (
                            SELECT rm.relation_id AS relation_id FROM relation_members rm
                            INNER JOIN bbox_nodes n ON rm.member_id = n.id WHERE rm.member_type = 'N'
                            UNION
                            SELECT rm.relation_id AS relation_id FROM relation_members rm
                            INNER JOIN bbox_ways w ON rm.member_id = w.id WHERE rm.member_type = 'W'
                         ) rids GROUP BY relation_id
                    ) rids ON r.id = rids.relation_id""")
    print cursor.query

def backfill_parent_relations(cursor):
    while True:
        rows = cursor.execute("""INSERT INTO bbox_relations
                    SELECT r.* FROM relations r INNER JOIN (
                        SELECT rm.relation_id FROM relation_members rm
                        INNER JOIN bbox_relations br ON rm.member_id = br.id
                        WHERE rm.member_type = 'R' AND NOT EXISTS (
                            SELECT * FROM bbox_relations br2 WHERE rm.relation_id = br2.id
                        ) GROUP BY rm.relation_id
                    ) rids ON r.id = rids.relation_id""")
        print cursor.query
        if cursor.rowcount == 0:
            break

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

    query_nodes(cursor, 'id IN %s', (tuple(ids),))

    if cursor.rowcount < 1:
        return Response('Node %s not found.' % ids, status=404)

    query_ways(cursor, 'FALSE')

    query_relations(cursor, 'FALSE')

    return Response(stream_osm_data(cursor), mimetype='text/xml')

@app.route("/api/0.6/way/<string:ids>")
def ways(ids):
    ids = ids.split(',')

    cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)

    query_nodes(cursor, 'FALSE')

    query_ways(cursor, 'id IN %s', (tuple(ids),))

    if cursor.rowcount < 1:
        return Response('Way %s not found.' % ids, status=404)

    cursor.execute("""ANALYZE bbox_ways""")

    backfill_way_nodes(cursor)

    cursor.execute("""ANALYZE bbox_nodes""")

    query_relations(cursor, 'FALSE')

    return Response(stream_osm_data(cursor), mimetype='text/xml')

@app.route("/api/0.6/relation/<string:ids>")
def relations(ids):
    ids = ids.split(',')

    cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)

    query_nodes(cursor, 'FALSE')

    query_ways(cursor, 'FALSE')

    query_relations(cursor, 'id IN %s', (tuple(ids),))

    if cursor.rowcount < 1:
        return Response('Relation %s not found.' % ids, status=404)

    return Response(stream_osm_data(cursor), mimetype='text/xml')

@app.route('/api/0.6/map')
def map():
    bbox = request.args.get('bbox')

    try:
        (bottom, left, top, right) = tuple(float(v) for v in bbox.split(','))
    except ValueError, e:
        return Response('Invalid bbox.', status=400)

    if left > right:
        return Response('Left > Right.', status=400)
    if bottom > top:
        return Response('Bottom > Top.', status=400)
    if bottom < -90 or bottom > 90:
        return Response('Bottom is out of range.', status=400)
    if top < -90 or top > 90:
        return Response('Top is out of range.', status=400)
    if left < -180 or left > 180:
        return Response('Left is out of range.', status=400)
    if right < -180 or right > 180:
        return Response('Right is out of range.', status=400)

    cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)

    query_nodes(cursor, 'ST_Intersects(geom, ST_GeometryFromText(\'POLYGON((%s %s, %s %s, %s %s, %s %s, %s %s))\', 4326))', (left, bottom, left, top, right, top, right, bottom, left, bottom))
    cursor.execute("""ALTER TABLE ONLY bbox_nodes ADD CONSTRAINT pk_bbox_nodes PRIMARY KEY (id)""")

    query_ways(cursor, 'ST_Intersects(linestring, ST_GeometryFromText(\'POLYGON((%s %s, %s %s, %s %s, %s %s, %s %s))\', 4326))', (left, bottom, left, top, right, top, right, bottom, left, bottom))
    cursor.execute("""ALTER TABLE ONLY bbox_ways ADD CONSTRAINT pk_bbox_ways PRIMARY KEY (id)""")

    backfill_relations(cursor)
    backfill_parent_relations(cursor)
    backfill_way_nodes(cursor)

    cursor.execute("""ANALYZE bbox_nodes""")
    cursor.execute("""ANALYZE bbox_ways""")
    cursor.execute("""ANALYZE bbox_relations""")

    return Response(stream_osm_data(cursor), mimetype='text/xml')

if __name__ == "__main__":
    app.run(debug=True)