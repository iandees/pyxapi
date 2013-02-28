from xml.dom.minidom import Document
from flask import Flask, Response, request, g
import psycopg2
import psycopg2.extras
import re
import itertools
import logging

app = Flask(__name__)
osmosis_work_dir = '/Users/iandees/.osmosis'
db = psycopg2.connect(host='localhost', dbname='xapi', user='xapi', password='xapi')
psycopg2.extras.register_hstore(db)

def write_primitive_attributes_json(primitive):
    return '"id": {}, "version": {}, "changeset": {}, ' \
        '"user": "{}", "uid": {}, "visible": true, "timestamp": "{}"'.format(
            primitive.get('id'),
            primitive.get('version'),
            primitive.get('changeset_id'),
            primitive.get('name'),
            primitive.get('user_id'),
            primitive.get('tstamp').isoformat())

def write_tags_json(primitive):
    return '"tags": {%s}' % ','.join(['"{}": "{}"'.format(k, v) for (k,v) in primitive.get('tags', {}).iteritems()])

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
            n += 1
            yield '{'
            yield write_primitive_attributes_json(row)
            yield ', "lat": {0}, "lon": {1}, '.format(row.get('latitude'), row.get('longitude'))

            yield write_tags_json(row)

            if n == rows:
                yield '}'
            else:
                yield '},'

        cursor.execute('''SELECT bbox_ways.id, version, user_id, tstamp, changeset_id, tags, nodes, name
                        FROM bbox_ways, users WHERE user_id = users.id ORDER BY id''')

        rows = cursor.rowcount
        n = 0

        yield '], "ways": ['
        for row in cursor:
            n += 1
            yield '{'
            yield write_primitive_attributes_json(row)
            yield ','

            yield write_tags_json(row)

            yield ', "nds": [%s]' % ','.join(itertools.imap(str, row.get('nodes', [])))

            if n == rows:
                yield '}'
            else:
                yield '},'

        cursor.execute('''SELECT bbox_relations.id, version, user_id, tstamp, changeset_id, tags, name
                        FROM bbox_relations, users where user_id = users.id ORDER BY id''')

        rows = cursor.rowcount
        n = 0

        yield '], "relations": ['
        relation_cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)
        for row in cursor:
            n += 1
            yield '{'
            relation_cursor.execute("""SELECT relation_id AS entity_id, member_id, member_type, member_role, sequence_id
                                       FROM relation_members f
                                       WHERE relation_id=%s
                                       ORDER BY sequence_id""", (row.get('id'),))

            yield write_primitive_attributes_json(row)
            yield ','

            yield write_tags_json(row)

            yield ', "members": ['
            member_rows = relation_cursor.rowcount
            member_n = 0
            for member in relation_cursor:
                member_n += 1
                member_type = member.get('member_type', None)
                if member_type == 'N':
                    member_type = 'node'
                elif member_type == 'W':
                    member_type = 'way'
                elif member_type == 'R':
                    member_type = 'relation'
                member['member_type'] = member_type

                yield '{{"role": "{member_role}", "type": "{member_type}", "ref": {member_id}}}'.format(**member)

                if member_n != member_rows:
                    yield ','

            yield ']'

            if n == rows:
                yield '}'
            else:
                yield '},'

        yield ']}'
    finally:
        cursor.connection.rollback()

def write_primitive_attributes_xml(element, primitive):
    element.setAttribute("id", str(primitive.get('id')))
    element.setAttribute("version", str(primitive.get('version')))
    element.setAttribute("changeset", str(primitive.get('changeset_id')))
    element.setAttribute("user", str(primitive.get('name')))
    element.setAttribute("uid", str(primitive.get('user_id')))
    element.setAttribute("visible", "true")
    element.setAttribute("timestamp", primitive.get('tstamp').isoformat())

def write_tags_xml(doc, parent_element, primitive):
    for (k, v) in primitive.get('tags', {}).iteritems():
        tag_elem = doc.createElement('tag')
        tag_elem.setAttribute("k", k)
        tag_elem.setAttribute("v", v)
        parent_element.appendChild(tag_elem)

def stream_osm_data_as_xml(cursor, bbox=None, timestamp=None):
    """Streams OSM data from psql temp tables."""

    doc = Document()

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
            elem = doc.createElement('node')
            write_primitive_attributes(elem, row)
            elem.setAttribute("lat", str(row.get('latitude')))
            elem.setAttribute("lon", str(row.get('longitude')))

            write_tags(doc, elem, row)

            yield elem.toxml()
            yield '\n'

        cursor.execute('''SELECT bbox_ways.id, version, user_id, tstamp, changeset_id, tags, nodes, name
                        FROM bbox_ways, users WHERE user_id = users.id ORDER BY id''')

        for row in cursor:
            tags = row.get('tags', {})
            nds = row.get('nodes', [])

            elem = doc.createElement('way')
            write_primitive_attributes(elem, row)

            write_tags(doc, elem, row)

            for nd in nds:
                nd_elem = doc.createElement('nd')
                nd_elem.setAttribute("ref", str(nd))
                elem.appendChild(nd_elem)

            yield elem.toxml()
            yield '\n'

        cursor.execute('''SELECT bbox_relations.id, version, user_id, tstamp, changeset_id, tags, name
                        FROM bbox_relations, users where user_id = users.id ORDER BY id''')

        relation_cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)
        for row in cursor:
            tags = row.get('tags', {})
            relation_cursor.execute("""SELECT relation_id AS entity_id, member_id, member_type, member_role, sequence_id
                                       FROM relation_members f
                                       WHERE relation_id=%s
                                       ORDER BY sequence_id""", (row.get('id'),))

            elem = doc.createElement('relation')
            write_primitive_attributes(elem, row)

            write_tags(doc, elem, row)

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
        cursor.connection.rollback()

def stream_osm_data(cursor, bbox=None, timestamp=None):
    return stream_osm_data_as_json(cursor, bbox, timestamp)

def query_nodes(cursor, where_str, where_obj=None):
    cursor.execute("""CREATE TEMPORARY TABLE bbox_nodes ON COMMIT DROP AS
                        SELECT *
                        FROM nodes
                        WHERE %s""" % where_str, where_obj)
    app.logger.info(cursor.query)

def query_ways(cursor, where_str, where_obj=None):
    cursor.execute("""CREATE TEMPORARY TABLE bbox_ways ON COMMIT DROP AS
                        SELECT *
                        FROM ways
                        WHERE %s""" % where_str, where_obj)
    app.logger.info(cursor.query)

def query_relations(cursor, where_str, where_obj=None):
    cursor.execute("""CREATE TEMPORARY TABLE bbox_relations ON COMMIT DROP AS
                        SELECT *
                        FROM relations
                        WHERE %s""" % where_str, where_obj)
    app.logger.info(cursor.query)

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
    app.logger.info(cursor.query)

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
        app.logger.info(cursor.query)
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

@app.before_request
def before_request():
    g.cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)

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
    return Response(xml, mimetype='text/xml')

@app.route("/api/0.6/node/<string:ids>")
def nodes(ids):
    try:
        ids = [int(i) for i in ids.split(',')]
    except ValueError, e:
        g.cursor.connection.rollback()
        return Response(e.message, status=400)

    if not ids:
        g.cursor.connection.rollback()
        return Response('No IDs specified.', status=400)

    try:
        query_nodes(g.cursor, 'id IN %s', (tuple(ids),))

        if g.cursor.rowcount < 1:
            g.cursor.connection.rollback()
            return Response('Node %s not found.' % ids, status=404)

        query_ways(g.cursor, 'FALSE')

        query_relations(g.cursor, 'FALSE')

    except Exception, e:
        g.cursor.connection.rollback()
        return Response(e.message, status=500)

    return Response(stream_osm_data(g.cursor), mimetype='text/xml')

@app.route('/api/0.6/nodes')
def nodes_as_queryarg():
    ids = request.args.get('nodes', '')
    return nodes(ids)

@app.route("/api/0.6/way/<string:ids>")
def ways(ids):
    try:
        ids = [int(i) for i in ids.split(',')]
    except ValueError, e:
        g.cursor.connection.rollback()
        return Response(e.message, status=400)

    if not ids:
        g.cursor.connection.rollback()
        return Response('No IDs specified.', status=400)

    try:
        query_nodes(g.cursor, 'FALSE')

        query_ways(g.cursor, 'id IN %s', (tuple(ids),))

        if g.cursor.rowcount < 1:
            g.cursor.connection.rollback()
            return Response('Way %s not found.' % ids, status=404)

        g.cursor.execute("""ANALYZE bbox_ways""")

        backfill_way_nodes(g.cursor)

        g.cursor.execute("""ANALYZE bbox_nodes""")

        query_relations(g.cursor, 'FALSE')
    except Exception, e:
        g.cursor.connection.rollback()
        return Response(e.message, status=500)

    return Response(stream_osm_data(g.cursor), mimetype='text/xml')

@app.route('/api/0.6/ways')
def ways_as_queryarg():
    ids = request.args.get('ways', '')
    return ways(ids)

@app.route("/api/0.6/relation/<string:ids>")
def relations(ids):
    try:
        ids = [int(i) for i in ids.split(',')]
    except ValueError, e:
        g.cursor.connection.rollback()
        return Response(e.message, status=400)

    if not ids:
        g.cursor.connection.rollback()
        return Response('No IDs specified.', status=400)

    try:
        query_nodes(g.cursor, 'FALSE')

        query_ways(g.cursor, 'FALSE')

        query_relations(g.cursor, 'id IN %s', (tuple(ids),))

        if g.cursor.rowcount < 1:
            g.cursor.connection.rollback()
            return Response('Relation %s not found.' % ids, status=404)
    except Exception, e:
        g.cursor.connection.rollback()
        return Response(e.message, status=500)

    return Response(stream_osm_data(g.cursor), mimetype='text/xml')

@app.route('/api/0.6/relations')
def relations_as_queryarg():
    ids = request.args.get('relations', '')
    return relations(ids)

@app.route('/api/0.6/map')
def map():
    bbox = request.args.get('bbox')

    if not bbox:
        g.cursor.connection.rollback()
        return Response('No bbox specified.', status=400)

    try:
        (query_str, query_objs) = parse_xapi('[bbox=%s]' % bbox)
    except QueryError, e:
        g.cursor.connection.rollback()
        return Response(e.message, status=400)
    except ValueError, e:
        g.cursor.connection.rollback()
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
        g.cursor.connection.rollback()
        return Response(e.message, status=500)

    return Response(stream_osm_data(g.cursor, bbox=parse_bbox(bbox), timestamp=parse_timestamp(osmosis_work_dir)), mimetype='text/xml')

@app.route('/api/0.6/node<string:predicate>')
def search_nodes(predicate):
    try:
        (query_str, query_objs) = parse_xapi(predicate)
    except QueryError, e:
        g.cursor.connection.rollback()
        return Response(e.message, status=400)
    except ValueError, e:
        g.cursor.connection.rollback()
        return Response(e.message, status=400)

    try:
        query_nodes(g.cursor, query_str, query_objs)

        query_ways(g.cursor, 'FALSE')

        query_relations(g.cursor, 'FALSE')
    except Exception, e:
        g.cursor.connection.rollback()
        return Response(e.message, status=500)

    return Response(stream_osm_data(g.cursor), mimetype='text/xml')

@app.route('/api/0.6/way<string:predicate>')
def search_ways(predicate):
    try:
        (query_str, query_objs) = parse_xapi(predicate)
    except QueryError, e:
        g.cursor.connection.rollback()
        return Response(e.message, status=400)
    except ValueError, e:
        g.cursor.connection.rollback()
        return Response(e.message, status=400)

    try:
        query_nodes(g.cursor, 'FALSE')

        query_ways(g.cursor, query_str.replace('geom', 'linestring'), query_objs)
        backfill_way_nodes(g.cursor)

        query_relations(g.cursor, 'FALSE')
    except Exception, e:
        g.cursor.connection.rollback()
        return Response(e.message, status=500)

    return Response(stream_osm_data(g.cursor), mimetype='text/xml')

@app.route('/api/0.6/relation<string:predicate>')
def search_relations(predicate):
    try:
        (query_str, query_objs) = parse_xapi(predicate)
    except QueryError, e:
        g.cursor.connection.rollback()
        return Response(e.message, status=400)
    except ValueError, e:
        g.cursor.connection.rollback()
        return Response(e.message, status=400)

    return Response(stream_osm_data(g.cursor), mimetype='text/xml')

@app.route('/api/0.6/*<string:predicate>')
def search_primitives(predicate):
    try:
        (query_str, query_objs) = parse_xapi(predicate)
    except QueryError, e:
        g.cursor.connection.rollback()
        return Response(e.message, status=400)
    except ValueError, e:
        g.cursor.connection.rollback()
        return Response(e.message, status=400)

    try:
        query_nodes(g.cursor, query_str, query_objs)

        query_ways(g.cursor, query_str.replace('geom', 'linestring'), query_objs)
        backfill_way_nodes(g.cursor)

        query_relations(g.cursor, 'FALSE')
    except Exception, e:
        g.cursor.connection.rollback()
        return Response(e.message, status=500)

    return Response(stream_osm_data(g.cursor), mimetype='text/xml')

if __name__ == "__main__":
    app.run(debug=True)