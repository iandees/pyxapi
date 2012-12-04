from xml.dom.minidom import Document
from flask import Flask, Response, request
import psycopg2
import psycopg2.extras
import re
import itertools

app = Flask(__name__)
osmosis_work_dir = '/Users/iandees/.osmosis'
db = psycopg2.connect(host='localhost', dbname='xapi', user='xapi', password='xapi')
psycopg2.extras.register_hstore(db)

def stream_osm_data(cursor, bbox=None, timestamp=None):
    """Streams OSM data from psql temp tables."""
    try:
        doc = Document()

        yield '<?xml version="1.0" encoding="UTF-8"?>\n'

        osm_extra = ""
        if timestamp:
            osm_extra = ' xmlns:xapi="http://jxapi.openstreetmap.org/" xapi:timestamp="{}"'.format(timestamp)

        yield '<osm version="0.6" generator="pyxapi" copyright="OpenStreetMap and contributors" attribution="http://www.openstreetmap.org/copyright" license="http://opendatacommons.org/licenses/odbl/1-0/"{}>\n'.format(osm_extra)

        if bbox:
            yield '<bounds minlat="{1}" minlon="{0}" maxlat="{3}" maxlon="{2}"/>\n'.format(*bbox)

        cursor.execute("SELECT id, version, changeset_id, ST_X(geom) as longitude, ST_Y(geom) as latitude, user_id, tstamp, tags FROM bbox_nodes ORDER BY id")

        for row in cursor:
            tags = row.get('tags', {})

            elem = doc.createElement('node')
            elem.setAttribute("id", str(row.get('id')))
            elem.setAttribute("version", str(row.get('version')))
            elem.setAttribute("changeset", str(row.get('changeset_id')))
            elem.setAttribute("uid", str(row.get('user_id')))
            elem.setAttribute("visible", "true")
            elem.setAttribute("timestamp", row.get('tstamp').isoformat())
            elem.setAttribute("lat", str(row.get('latitude')))
            elem.setAttribute("lon", str(row.get('longitude')))

            for (k, v) in tags.iteritems():
                tag_elem = doc.createElement('tag')
                tag_elem.setAttribute("k", k)
                tag_elem.setAttribute("v", v)
                elem.appendChild(tag_elem)

            yield elem.toxml()
            yield '\n'

        cursor.execute("SELECT * FROM bbox_ways ORDER BY id")

        for row in cursor:
            tags = row.get('tags', {})
            nds = row.get('nodes', [])

            elem = doc.createElement('way')
            elem.setAttribute("id", str(row.get('id')))
            elem.setAttribute("version", str(row.get('version')))
            elem.setAttribute("changeset", str(row.get('changeset_id')))
            elem.setAttribute("uid", str(row.get('user_id')))
            elem.setAttribute("visible", "true")
            elem.setAttribute("timestamp", row.get('tstamp').isoformat())

            for (k, v) in tags.iteritems():
                tag_elem = doc.createElement('tag')
                tag_elem.setAttribute("k", k)
                tag_elem.setAttribute("v", v)
                elem.appendChild(tag_elem)

            for nd in nds:
                nd_elem = doc.createElement('nd')
                nd_elem.setAttribute("ref", str(nd))
                elem.appendChild(nd_elem)

            yield elem.toxml()
            yield '\n'

        cursor.execute("SELECT * FROM bbox_relations ORDER BY id")

        relation_cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)
        for row in cursor:
            tags = row.get('tags', {})
            relation_cursor.execute("""SELECT relation_id AS entity_id, member_id, member_type, member_role, sequence_id
                                       FROM relation_members f
                                       WHERE relation_id=%s
                                       ORDER BY sequence_id""", (row.get('id'),))

            elem = doc.createElement('relation')
            elem.setAttribute("id", str(row.get('id')))
            elem.setAttribute("version", str(row.get('version')))
            elem.setAttribute("changeset", str(row.get('changeset_id')))
            elem.setAttribute("uid", str(row.get('user_id')))
            elem.setAttribute("visible", "true")
            elem.setAttribute("timestamp", row.get('tstamp').isoformat())

            for (k, v) in tags.iteritems():
                tag_elem = doc.createElement('tag')
                tag_elem.setAttribute("k", k)
                tag_elem.setAttribute("v", v)
                elem.appendChild(tag_elem)

            for member in relation_cursor:
                member_type = member.get('member_type', None)
                if member_type == 'N':
                    member_type = 'node'
                elif member_type == 'W':
                    member_type = 'way'
                elif member_type == 'R':
                    member_type = 'relation'
                member['member_type'] = member_type

                member_elem = doc.createElement('member')
                member_elem.setAttribute("role", member.get('member_role'))
                member_elem.setAttribute("type", member.get('member_type'))
                member_elem.setAttribute("ref", str(member.get('member_id')))
                elem.appendChild(member_elem)

            yield elem.toxml()
            yield '\n'

        yield '</osm>\n'
    finally:
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

class QueryError(Exception):
    pass

def parse_xapi(predicate):
    query = []
    query_objs = []
    groups = re.findall(r'(?:\[(.*?)\])', predicate)
    for g in groups:
        (left, right) = g.split('=')
        if left == '@uid':
            query.append('uid = %s')
            query_objs.append(int(right))
        elif left == '@changeset':
            query.append('changeset_id = %s')
            query_objs.append(int(right))
        elif left == 'bbox':
            try:
                (l, b, r, t) = parse_bbox(right)
            except ValueError, e:
                raise QueryError('Invalid bbox.')

            if l > r:
                raise QueryError('Left > Right.')
            if b > t:
                raise QueryError('Bottom > Top.')
            if b < -90 or b > 90:
                raise QueryError('Bottom is out of range.')
            if t < -90 or t > 90:
                raise QueryError('Top is out of range.')
            if l < -180 or l > 180:
                raise QueryError('Left is out of range.')
            if r < -180 or r > 180:
                raise QueryError('Right is out of range.')

            query.append('ST_Intersects(geom, ST_GeometryFromText(\'POLYGON((%s %s, %s %s, %s %s, %s %s, %s %s))\', 4326))')
            query_objs.extend([l, b,
                               l, t,
                               r, t,
                               r, b,
                               l, b])
        else:
            ors = []
            orvs = []
            keys = left.split('|')
            vals = right.split('|')
            for (l,r) in itertools.product(keys, vals):
                if r == '*':
                    ors.append('(tags ? %s)')
                    orvs.append(l)
                else:
                    ors.append('(tags @> hstore(%s, %s))')
                    orvs.append(l)
                    orvs.append(r)
            query.append('(' + ' OR '.join(ors) + ')')
            query_objs.extend(orvs)
    query_str = ' AND '.join(query)
    return (query_str, query_objs)

def parse_bbox(bbox_str):
    return tuple(float(v) for v in bbox_str.split(','))

def parse_timestamp(osmosis_work_dir):
    f = open('{}/state.txt'.format(osmosis_work_dir), 'r')

    time_str = None
    for line in f:
        if line.startswith('timestamp='):
            time_str = line[10:].replace('\\', '').strip()

    f.close()

    return time_str

@app.route("/api/capabilities")
@app.route("/api/0.6/capabilities")
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
        cursor.connection.rollback()
        return Response('Node %s not found.' % ids, status=404)

    query_ways(cursor, 'FALSE')

    query_relations(cursor, 'FALSE')

    return Response(stream_osm_data(cursor), mimetype='text/xml')

@app.route('/api/0.6/nodes')
def nodes_as_queryarg():
    ids = request.args.get('nodes')
    return nodes(ids)

@app.route("/api/0.6/way/<string:ids>")
def ways(ids):
    ids = ids.split(',')

    cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)

    query_nodes(cursor, 'FALSE')

    query_ways(cursor, 'id IN %s', (tuple(ids),))

    if cursor.rowcount < 1:
        cursor.connection.rollback()
        return Response('Way %s not found.' % ids, status=404)

    cursor.execute("""ANALYZE bbox_ways""")

    backfill_way_nodes(cursor)

    cursor.execute("""ANALYZE bbox_nodes""")

    query_relations(cursor, 'FALSE')

    return Response(stream_osm_data(cursor), mimetype='text/xml')

@app.route('/api/0.6/ways')
def ways_as_queryarg():
    ids = request.args.get('ways')
    return ways(ids)

@app.route("/api/0.6/relation/<string:ids>")
def relations(ids):
    ids = ids.split(',')

    cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)

    query_nodes(cursor, 'FALSE')

    query_ways(cursor, 'FALSE')

    query_relations(cursor, 'id IN %s', (tuple(ids),))

    if cursor.rowcount < 1:
        cursor.connection.rollback()
        return Response('Relation %s not found.' % ids, status=404)

    return Response(stream_osm_data(cursor), mimetype='text/xml')

@app.route('/api/0.6/relations')
def relations_as_queryarg():
    ids = request.args.get('relations')
    return relations(ids)

@app.route('/api/0.6/map')
def map():
    bbox = request.args.get('bbox')

    try:
        (query_str, query_objs) = parse_xapi('[bbox=%s]' % bbox)
    except QueryError, e:
        return Response(e.message, status=400)
    except ValueError, e:
        return Response(e.message, status=400)

    cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)

    query_nodes(cursor, query_str, query_objs)
    cursor.execute("""ALTER TABLE ONLY bbox_nodes ADD CONSTRAINT pk_bbox_nodes PRIMARY KEY (id)""")

    query_ways(cursor, query_str.replace('geom', 'linestring'), query_objs)
    cursor.execute("""ALTER TABLE ONLY bbox_ways ADD CONSTRAINT pk_bbox_ways PRIMARY KEY (id)""")

    backfill_relations(cursor)
    backfill_parent_relations(cursor)
    backfill_way_nodes(cursor)

    cursor.execute("""ANALYZE bbox_nodes""")
    cursor.execute("""ANALYZE bbox_ways""")
    cursor.execute("""ANALYZE bbox_relations""")

    return Response(stream_osm_data(cursor, bbox=parse_bbox(bbox), timestamp=parse_timestamp(osmosis_work_dir)), mimetype='text/xml')

@app.route('/api/0.6/node<string:predicate>')
def search_nodes(predicate):
    try:
        (query_str, query_objs) = parse_xapi(predicate)
    except QueryError, e:
        return Response(e.message, status=400)
    except ValueError, e:
        return Response(e.message, status=400)

    cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)

    query_nodes(cursor, query_str, query_objs)

    query_ways(cursor, 'FALSE')

    query_relations(cursor, 'FALSE')

    return Response(stream_osm_data(cursor), mimetype='text/xml')

@app.route('/api/0.6/way<string:predicate>')
def search_ways(predicate):
    try:
        (query_str, query_objs) = parse_xapi(predicate)
    except QueryError, e:
        return Response(e.message, status=400)
    except ValueError, e:
        return Response(e.message, status=400)

    cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)

    query_nodes(cursor, 'FALSE')

    query_ways(cursor, query_str.replace('geom', 'linestring'), query_objs)
    backfill_way_nodes(cursor)

    query_relations(cursor, 'FALSE')

    return Response(stream_osm_data(cursor), mimetype='text/xml')

@app.route('/api/0.6/relation<string:predicate>')
def search_relations(predicate):
    try:
        (query_str, query_objs) = parse_xapi(predicate)
    except QueryError, e:
        return Response(e.message, status=400)
    except ValueError, e:
        return Response(e.message, status=400)

    cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)

    return Response(stream_osm_data(cursor), mimetype='text/xml')

@app.route('/api/0.6/*<string:predicate>')
def search_primitives(predicate):
    try:
        (query_str, query_objs) = parse_xapi(predicate)
    except QueryError, e:
        return Response(e.message, status=400)
    except ValueError, e:
        return Response(e.message, status=400)

    cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)

    query_nodes(cursor, query_str, query_objs)

    query_ways(cursor, query_str.replace('geom', 'linestring'), query_objs)
    backfill_way_nodes(cursor)

    query_relations(cursor, 'FALSE')

    return Response(stream_osm_data(cursor), mimetype='text/xml')

if __name__ == "__main__":
    app.run(debug=True)