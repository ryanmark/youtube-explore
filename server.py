import json
from datetime import datetime
from redis import Redis
from rq import Queue
from slugify import slugify
from flask import Flask, render_template, request
from worker import conn as redis
from follow_youtube_recommendations import YoutubeFollower

q = Queue(connection=redis)
app = Flask(__name__)

def do_search(slug, query, search_results, branching, depth, gl, language, recent, loopok):
    redis.hset(slug, 'status', 'processing')
    yf = YoutubeFollower(verbose=True, name=query, alltime=False, gl=gl, language=language, recent=recent, loopok=loopok)
    top_recommended, counts = yf.go_deeper_from(query,
                      search_results=search_results,
                      branching=branching,
                      depth=depth)
    top_videos = yf.get_top_videos(top_recommended, counts, 1000)
    yf.print_videos(top_recommended, counts, 50)
    # yf.save_video_infos(slug + '-' + keyword)

    redis.hmset(slug, {
        'top_videos': json.dumps(top_videos),
        'status': 'done',
        'finished': dt_to_timestamp(datetime.now())
    })

def merge_dicts(x, y):
    """Given two dicts, merge them into a new dict as a shallow copy."""
    z = x.copy()
    z.update(y)
    return z

def dt_to_timestamp(dt):
    if dt.utcoffset():
        utc_naive = dt.replace(tzinfo=None) - dt.utcoffset()
    else:
        utc_naive = dt
    return (utc_naive - datetime(1970, 1, 1)).total_seconds()

def get_search(slug):
    dat = redis.hgetall(slug)
    if not dat:
        return None
    dat['slug'] = slug
    if 'top_videos' in dat and dat['top_videos']:
        dat['top_videos'] = json.loads(dat['top_videos'])
    if 'started' in dat and dat['started']:
        dat['started'] = datetime.utcfromtimestamp(float(dat['started']))
    if 'finished' in dat and dat['finished']:
        dat['finished'] = datetime.utcfromtimestamp(float(dat['finished']))
    return dat

@app.route("/")
def index():
    slugs = redis.smembers('search-slugs')
    searches = [get_search(slug) for slug in slugs]
    sorted_searches = sorted(searches, key=lambda x: x['started'], reverse=True)
    return render_template('index.html', searches=sorted_searches)

@app.route("/new-search", methods=['POST'])
def new_search():
    now = datetime.now()
    slug = '-'.join(['search', slugify(request.form['query']), now.strftime('%Y-%m-%d-%H%M%S')])

    if redis.exists(slug):
        return 'Duplicate job', 500

    params = {
        'query': request.form['query'],
        'searches': int(request.form['searches']),
        'branch': int(request.form['branch']),
        'depth': int(request.form['depth']),
        'country': request.form.get('country', 'US'),
        'lang': request.form.get('lang', 'en-US'),
        'recent': bool(request.form.get('recent', False)),
        'loopok': bool(request.form.get('loopok', False)),
        'alltime': bool(request.form.get('alltime', False)),
        'started': dt_to_timestamp(now)
    }
    redis.hmset(slug, merge_dicts(params, {'status': 'queued'}))

    q.enqueue(
        do_search,
        slug,
        params['query'],
        params['searches'],
        params['branch'],
        params['depth'],
        params['country'],
        params['lang'],
        params['recent'],
        params['loopok'],
        timeout='2h'
    )

    redis.sadd('search-slugs', slug)

    return 'Search queued', 202

@app.route("/<slug>")
def search(slug):
    dat = get_search(slug)
    if dat:
        return render_template('search.html', search=dat)
    else:
        return 'Not found', 404
