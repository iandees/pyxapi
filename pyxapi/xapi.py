from lxml import etree
from flask import Flask, Response, request, g, stream_with_context, make_response, current_app
from functools import update_wrapper
import psycopg2
import psycopg2.extras
import re
import itertools
import json
from datetime import timedelta, datetime

app = Flask(__name__)
osmosis_work_dir = '/Users/iandees/.osmosis'


def crossdomain(origin=None, methods=None, headers=None,
                max_age=21600, attach_to_all=True,
                automatic_options=True):
    if methods is not None:
        methods = ', '.join(sorted(x.upper() for x in methods))
    if headers is not None and not isinstance(headers, basestring):
        headers = ', '.join(x.upper() for x in headers)
    if not isinstance(origin, basestring):
        origin = ', '.join(origin)
    if isinstance(max_age, timedelta):
        max_age = max_age.total_seconds()

    def get_methods():
        if methods is not None:
            return methods

        options_resp = current_app.make_default_options_response()
        return options_resp.headers['allow']

    def decorator(f):
        def wrapped_function(*args, **kwargs):
            if automatic_options and request.method == 'OPTIONS':
                resp = current_app.make_default_options_response()
            else:
                resp = make_response(f(*args, **kwargs))
            if not attach_to_all and request.method != 'OPTIONS':
                return resp

            h = resp.headers

            h['Access-Control-Allow-Origin'] = origin
            h['Access-Control-Allow-Methods'] = get_methods()
            h['Access-Control-Max-Age'] = str(max_age)
            if headers is not None:
                h['Access-Control-Allow-Headers'] = headers
            return resp

        f.provide_automatic_options = False
        return update_wrapper(wrapped_function, f)
    return decorator


@app.before_request
def before_request():
    g.db = psycopg2.connect(host='localhost', dbname='xapi', user='xapi', password='xapi')
    psycopg2.extras.register_hstore(g.db)
    g.cursor = g.db.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Single statement timeout set to 3 minutes
    g.cursor.execute('SET statement_timeout TO 180000;')

def json_default(o):
    otype = type(o)
    if type(o) is datetime:
        return o.isoformat()

def stream_osm_data_as_json(cursor, bbox=None, timestamp=None):
    """Streams OSM data from psql temp tables."""

    try:
        yield '{'

        if timestamp:
            yield '"timestamp": "{}",'.format(timestamp)

        yield '"version": "0.6", '
        yield '"generator": "pyxapi", '
        yield '"copyright": "OpenStreetMap and contributors", '
        yield '"attribution": "http://www.openstreetmap.org/copyright", '
        yield '"license": "http://opendatacommons.org/licenses/odbl/1-0/", '

        if bbox:
            yield '"bounds": {{"minlat": {1}, "minlon": {0}, "maxlat": {3}, "maxlon": {2}}},'.format(*bbox)

        cursor.execute('''SELECT bbox_nodes.id, version, changeset_id, ST_X(geom) as longitude, ST_Y(geom) as latitude, user_id, name, tstamp, tags
                        FROM bbox_nodes, users
                        WHERE user_id = users.id
                        ORDER BY id''')

        rows = cursor.rowcount
        n = 0

        yield '"nodes": ['
        for row in cursor:
            yield json.dumps({
                'id': row['id'],
                'version': row['version'],
                'changeset': row['changeset_id'],
                'user': row['name'],
                'uid': row['user_id'],
                'visible': True,
                'timestamp': row['tstamp'],
                'lat': row['latitude'],
                'lon': row['longitude'],
                'tags': row['tags']
            }, default=json_default)
            n += 1

            if n != rows:
                yield ','

        cursor.execute('''SELECT bbox_ways.id, version, user_id, tstamp, changeset_id, tags, nodes, name
                        FROM bbox_ways, users WHERE user_id = users.id ORDER BY id''')

        rows = cursor.rowcount
        n = 0

        yield '], "ways": ['
        for row in cursor:
            yield json.dumps({
                'id': row['id'],
                'version': row['version'],
                'changeset': row['changeset_id'],
                'user': row['name'],
                'uid': row['user_id'],
                'visible': True,
                'timestamp': row['tstamp'],
                'tags': row['tags'],
                'nds': row['nodes']
            }, default=json_default)
            n += 1

            if n != rows:
                yield ','

        cursor.execute('''SELECT bbox_relations.id, version, user_id, tstamp, changeset_id, tags, name
                        FROM bbox_relations, users where user_id = users.id ORDER BY id''')

        rows = cursor.rowcount
        n = 0

        yield '], "relations": ['
        relation_cursor = g.db.cursor(cursor_factory=psycopg2.extras.DictCursor)
        for row in cursor:

            members = []
            relation_cursor.execute("""SELECT relation_id AS entity_id, member_id, member_type, member_role, sequence_id
                                       FROM relation_members f
                                       WHERE relation_id=%s
                                       ORDER BY sequence_id""", (row.get('id'),))

            for member in relation_cursor:
                member_type = member.get('member_type', None)
                if member_type == 'N':
                    member_type = 'node'
                elif member_type == 'W':
                    member_type = 'way'
                elif member_type == 'R':
                    member_type = 'relation'
                members.append({
                    'role': member['member_role'],
                    'type': member_type,
                    'ref': member['member_id']
                })

            yield json.dumps({
                'id': row['id'],
                'version': row['version'],
                'changeset': row['changeset_id'],
                'user': row['name'],
                'uid': row['user_id'],
                'visible': True,
                'timestamp': row['tstamp'],
                'tags': row['tags'],
                'members': members
            }, default=json_default)
            n += 1

            if n != rows:
                yield ','

        yield ']}'
    finally:
        cursor.close()

def write_primitive_attributes_xml(element, primitive):
    element.set("id", str(primitive.get('id')))
    element.set("version", str(primitive.get('version')))
    element.set("changeset", str(primitive.get('changeset_id')))
    element.set("user", unicode(primitive.get('name'), 'utf8'))
    element.set("uid", str(primitive.get('user_id')))
    element.set("visible", "true")
    element.set("timestamp", primitive.get('tstamp').isoformat())

def write_tags_xml(parent_element, primitive):
    for (k, v) in primitive.get('tags', {}).iteritems():
        tag_elem = etree.Element('tag', {'k': unicode(k, 'utf8'), 'v': unicode(v, 'utf8')})
        parent_element.append(tag_elem)

def stream_osm_data_as_xml(cursor, bbox=None, timestamp=None):
    """Streams OSM data from psql temp tables."""

    try:
        yield '<?xml version="1.0" encoding="UTF-8"?>\n'

        osm_extra = ""
        if timestamp:
            osm_extra = ' xmlns:xapi="http://jxapi.openstreetmap.org/" xapi:timestamp="{}"'.format(timestamp)

        yield '<osm version="0.6" generator="pyxapi" copyright="OpenStreetMap and contributors" attribution="http://www.openstreetmap.org/copyright" license="http://opendatacommons.org/licenses/odbl/1-0/"{}>\n'.format(osm_extra)

        if bbox:
            yield '<bounds minlat="{1}" minlon="{0}" maxlat="{3}" maxlon="{2}"/>\n'.format(*bbox)

        cursor.execute('''SELECT bbox_nodes.id, version, changeset_id, ST_X(geom) as longitude, ST_Y(geom) as latitude, user_id, name, tstamp, tags
                        FROM bbox_nodes, users
                        WHERE user_id = users.id
                        ORDER BY id''')

        for row in cursor:
            elem = etree.Element('node', {
                "lat": "%3.7f" % (row.get('latitude')),
                "lon": "%3.7f" % (row.get('longitude'))
            })
            write_primitive_attributes_xml(elem, row)

            write_tags_xml(elem, row)

            yield etree.tostring(elem, encoding='utf8')
            yield '\n'

        cursor.execute('''SELECT bbox_ways.id, version, user_id, tstamp, changeset_id, tags, nodes, name
                        FROM bbox_ways, users WHERE user_id = users.id ORDER BY id''')

        for row in cursor:
            tags = row.get('tags', {})
            nds = row.get('nodes', [])

            elem = etree.Element('way')
            write_primitive_attributes_xml(elem, row)

            write_tags_xml(elem, row)

            for nd in nds:
                nd_elem = etree.Element('nd', ref=str(nd))
                elem.append(nd_elem)

            yield etree.tostring(elem, encoding='utf8')
            yield '\n'

        cursor.execute('''SELECT bbox_relations.id, version, user_id, tstamp, changeset_id, tags, name
                        FROM bbox_relations, users where user_id = users.id ORDER BY id''')

        relation_cursor = g.db.cursor(cursor_factory=psycopg2.extras.DictCursor)
        for row in cursor:
            relation_cursor.execute("""SELECT relation_id AS entity_id, member_id, member_type, member_role, sequence_id
                                       FROM relation_members f
                                       WHERE relation_id=%s
                                       ORDER BY sequence_id""", (row.get('id'),))

            elem = etree.Element('relation')
            write_primitive_attributes_xml(elem, row)

            write_tags_xml(elem, row)

            for member in relation_cursor:
                member_type = member.get('member_type', None)
                if member_type == 'N':
                    member_type = 'node'
                elif member_type == 'W':
                    member_type = 'way'
                elif member_type == 'R':
                    member_type = 'relation'
                member['member_type'] = member_type

                member_elem = etree.Element('member', {
                    'role': unicode(member.get('member_role'), 'utf8'),
                    'type': member.get('member_type'),
                    'ref': str(member.get('member_id'))
                })
                elem.append(member_elem)

            yield etree.tostring(elem, encoding='utf8')
            yield '\n'

        yield '</osm>\n'
    finally:
        cursor.close()

def query_nodes(cursor, where_str, where_obj=None):
    cursor.execute("""CREATE TEMPORARY TABLE bbox_nodes ON COMMIT DROP AS
                        SELECT *
                        FROM nodes
                        WHERE %s""" % where_str, where_obj)

def query_ways(cursor, where_str, where_obj=None):
    cursor.execute("""CREATE TEMPORARY TABLE bbox_ways ON COMMIT DROP AS
                        SELECT *
                        FROM ways
                        WHERE %s""" % where_str, where_obj)

def query_relations(cursor, where_str, where_obj=None):
    cursor.execute("""CREATE TEMPORARY TABLE bbox_relations ON COMMIT DROP AS
                        SELECT *
                        FROM relations
                        WHERE %s""" % where_str, where_obj)

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
    try:
        f = open('{}/state.txt'.format(osmosis_work_dir), 'r')
    except:
        return None

    time_str = None
    for line in f:
        if line.startswith('timestamp='):
            time_str = line[10:].replace('\\', '').strip()

    f.close()

    return time_str

@app.route("/api/capabilities")
@app.route("/api/0.6/capabilities")
def capabilities():
    ts = parse_timestamp(osmosis_work_dir)
    if ts:
        timestamp = ' xmlns:xapi="http://jxapi.openstreetmap.org/" xapi:timestamp="{}"'.format(ts)
    else:
        timestamp = ''

    xml = """<?xml version="1.0" encoding="UTF-8"?>
<osm version="0.6" generator="pyxapi" copyright="OpenStreetMap and contributors" attribution="http://www.openstreetmap.org/copyright" license="http://opendatacommons.org/licenses/odbl/1-0/"{}>
  <api>
    <version minimum="0.6" maximum="0.6"/>
    <area maximum="0.25"/>
    <timeout seconds="300"/>
  </api>
</osm>""".format(timestamp)
    return Response(xml, mimetype='application/xml')

def request_wants_json():
    best = request.accept_mimetypes.best_match(['application/json', 'application/xml'])
    return best == 'application/json' and \
           request.accept_mimetypes[best] >= request.accept_mimetypes['application/xml']

@app.route("/api/0.6/node/<string:ids>")
def nodes(ids):
    try:
        ids = [int(i) for i in ids.split(',')]
    except ValueError, e:
        g.cursor.close()
        return Response(e.message, status=400)

    if not ids:
        g.cursor.close()
        return Response('No IDs specified.', status=400)

    try:
        query_nodes(g.cursor, 'id IN %s', (tuple(ids),))

        if g.cursor.rowcount < 1:
            g.cursor.close()
            return Response('Node %s not found.' % ids, status=404)

        query_ways(g.cursor, 'FALSE')

        query_relations(g.cursor, 'FALSE')

    except Exception, e:
        g.cursor.close()
        return Response(e.message, status=500)

    if request_wants_json():
        return Response(stream_with_context(stream_osm_data_as_json(g.cursor, timestamp=parse_timestamp(osmosis_work_dir))), mimetype='application/json')
    else:
        return Response(stream_with_context(stream_osm_data_as_xml(g.cursor, timestamp=parse_timestamp(osmosis_work_dir))), mimetype='application/xml')

@app.route('/api/0.6/nodes')
def nodes_as_queryarg():
    ids = request.args.get('nodes', '')
    return nodes(ids)

@app.route("/api/0.6/way/<string:ids>")
def ways(ids):
    try:
        ids = [int(i) for i in ids.split(',')]
    except ValueError, e:
        g.cursor.close()
        return Response(e.message, status=400)

    if not ids:
        g.cursor.close()
        return Response('No IDs specified.', status=400)

    try:
        query_nodes(g.cursor, 'FALSE')

        query_ways(g.cursor, 'id IN %s', (tuple(ids),))

        if g.cursor.rowcount < 1:
            g.cursor.close()
            return Response('Way %s not found.' % ids, status=404)

        g.cursor.execute("""ANALYZE bbox_ways""")

        backfill_way_nodes(g.cursor)

        g.cursor.execute("""ANALYZE bbox_nodes""")

        query_relations(g.cursor, 'FALSE')
    except Exception, e:
        g.cursor.close()
        return Response(e.message, status=500)

    if request_wants_json():
        return Response(stream_with_context(stream_osm_data_as_json(g.cursor, timestamp=parse_timestamp(osmosis_work_dir))), mimetype='application/json')
    else:
        return Response(stream_with_context(stream_osm_data_as_xml(g.cursor, timestamp=parse_timestamp(osmosis_work_dir))), mimetype='application/xml')

@app.route('/api/0.6/ways')
def ways_as_queryarg():
    ids = request.args.get('ways', '')
    return ways(ids)

@app.route("/api/0.6/relation/<string:ids>")
def relations(ids):
    try:
        ids = [int(i) for i in ids.split(',')]
    except ValueError, e:
        g.cursor.close()
        return Response(e.message, status=400)

    if not ids:
        g.cursor.close()
        return Response('No IDs specified.', status=400)

    try:
        query_nodes(g.cursor, 'FALSE')

        query_ways(g.cursor, 'FALSE')

        query_relations(g.cursor, 'id IN %s', (tuple(ids),))

        if g.cursor.rowcount < 1:
            g.cursor.close()
            return Response('Relation %s not found.' % ids, status=404)
    except Exception, e:
        g.cursor.close()
        return Response(e.message, status=500)

    if request_wants_json():
        return Response(stream_with_context(stream_osm_data_as_json(g.cursor, timestamp=parse_timestamp(osmosis_work_dir))), mimetype='application/json')
    else:
        return Response(stream_with_context(stream_osm_data_as_xml(g.cursor, timestamp=parse_timestamp(osmosis_work_dir))), mimetype='application/xml')

@app.route('/api/0.6/relations')
def relations_as_queryarg():
    ids = request.args.get('relations', '')
    return relations(ids)

@app.route('/api/0.6/map')
@crossdomain(origin='*')
def map():
    bbox = request.args.get('bbox')

    if not bbox:
        g.cursor.close()
        return Response('No bbox specified.', status=400)

    try:
        (query_str, query_objs) = parse_xapi('[bbox=%s]' % bbox)
    except QueryError, e:
        g.cursor.close()
        return Response(e.message, status=400)
    except ValueError, e:
        g.cursor.close()
        return Response(e.message, status=400)

    try:
        query_nodes(g.cursor, query_str, query_objs)
        g.cursor.execute("""ALTER TABLE ONLY bbox_nodes ADD CONSTRAINT pk_bbox_nodes PRIMARY KEY (id)""")

        query_ways(g.cursor, query_str.replace('geom', 'linestring'), query_objs)
        g.cursor.execute("""ALTER TABLE ONLY bbox_ways ADD CONSTRAINT pk_bbox_ways PRIMARY KEY (id)""")

        backfill_relations(g.cursor)
        backfill_parent_relations(g.cursor)
        backfill_way_nodes(g.cursor)

        g.cursor.execute("""ANALYZE bbox_nodes""")
        g.cursor.execute("""ANALYZE bbox_ways""")
        g.cursor.execute("""ANALYZE bbox_relations""")
    except Exception, e:
        g.cursor.close()
        return Response(e.message, status=500)

    if request_wants_json():
        return Response(stream_with_context(stream_osm_data_as_json(g.cursor, bbox=parse_bbox(bbox), timestamp=parse_timestamp(osmosis_work_dir))), mimetype='application/json')
    else:
        return Response(stream_with_context(stream_osm_data_as_xml(g.cursor, bbox=parse_bbox(bbox), timestamp=parse_timestamp(osmosis_work_dir))), mimetype='application/xml')

@app.route('/api/0.6/node<string:predicate>')
def search_nodes(predicate):
    try:
        (query_str, query_objs) = parse_xapi(predicate)
    except QueryError, e:
        g.cursor.close()
        return Response(e.message, status=400)
    except ValueError, e:
        g.cursor.close()
        return Response(e.message, status=400)

    try:
        query_nodes(g.cursor, query_str, query_objs)

        query_ways(g.cursor, 'FALSE')

        query_relations(g.cursor, 'FALSE')
    except Exception, e:
        g.cursor.close()
        return Response(e.message, status=500)

    if request_wants_json():
        return Response(stream_with_context(stream_osm_data_as_json(g.cursor, timestamp=parse_timestamp(osmosis_work_dir))), mimetype='application/json')
    else:
        return Response(stream_with_context(stream_osm_data_as_xml(g.cursor, timestamp=parse_timestamp(osmosis_work_dir))), mimetype='application/xml')

@app.route('/api/0.6/way<string:predicate>')
def search_ways(predicate):
    try:
        (query_str, query_objs) = parse_xapi(predicate)
    except QueryError, e:
        g.cursor.close()
        return Response(e.message, status=400)
    except ValueError, e:
        g.cursor.close()
        return Response(e.message, status=400)

    try:
        query_nodes(g.cursor, 'FALSE')

        query_ways(g.cursor, query_str.replace('geom', 'linestring'), query_objs)
        backfill_way_nodes(g.cursor)

        query_relations(g.cursor, 'FALSE')
    except Exception, e:
        g.cursor.close()
        return Response(e.message, status=500)

    if request_wants_json():
        return Response(stream_with_context(stream_osm_data_as_json(g.cursor, timestamp=parse_timestamp(osmosis_work_dir))), mimetype='application/json')
    else:
        return Response(stream_with_context(stream_osm_data_as_xml(g.cursor, timestamp=parse_timestamp(osmosis_work_dir))), mimetype='application/xml')

@app.route('/api/0.6/relation<string:predicate>')
def search_relations(predicate):
    try:
        (query_str, query_objs) = parse_xapi(predicate)
    except QueryError, e:
        g.cursor.close()
        return Response(e.message, status=400)
    except ValueError, e:
        g.cursor.close()
        return Response(e.message, status=400)
    
    query_relations(g.cursor, query_str, query_objs)
    query_nodes(g.cursor, 'FALSE')
    query_ways(g.cursor, 'FALSE')

    if request_wants_json():
        return Response(stream_with_context(stream_osm_data_as_json(g.cursor, timestamp=parse_timestamp(osmosis_work_dir))), mimetype='application/json')
    else:
        return Response(stream_with_context(stream_osm_data_as_xml(g.cursor, timestamp=parse_timestamp(osmosis_work_dir))), mimetype='application/xml')

@app.route('/api/0.6/*<string:predicate>')
def search_primitives(predicate):
    try:
        (query_str, query_objs) = parse_xapi(predicate)
    except QueryError, e:
        g.cursor.close()
        return Response(e.message, status=400)
    except ValueError, e:
        g.cursor.close()
        return Response(e.message, status=400)

    try:
        query_nodes(g.cursor, query_str, query_objs)

        query_ways(g.cursor, query_str.replace('geom', 'linestring'), query_objs)
        backfill_way_nodes(g.cursor)

        query_relations(g.cursor, 'FALSE')
    except Exception, e:
        g.cursor.close()
        return Response(e.message, status=500)

    if request_wants_json():
        return Response(stream_with_context(stream_osm_data_as_json(g.cursor, timestamp=parse_timestamp(osmosis_work_dir))), mimetype='application/json')
    else:
        return Response(stream_with_context(stream_osm_data_as_xml(g.cursor, timestamp=parse_timestamp(osmosis_work_dir))), mimetype='application/xml')

if __name__ == "__main__":
    app.run(debug=True, port=5000, processes=5)