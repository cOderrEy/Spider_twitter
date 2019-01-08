import json, requests, os, pymysql, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from lxml import etree

def tweet_reader(tweet_url):
    r = requests.get(tweet_url)
    selector = etree.HTML(r.text)
    context = selector.xpath('//p[@class="TweetTextSize TweetTextSize--jumbo js-tweet-text tweet-text"]')
    date = selector.xpath('//span[@class="metadata"]/span/text()')
    context = str(etree.tostring(context[0]), encoding='utf-8')
    context = context.replace('\\', '\\\\')
    context = context.replace('\"', '\\\"')
    context = context.replace("\'", "\\\'")
    time.sleep(5)
    return {"context": context, "date": date[0]}

def dump(name, data, db):
    cursor = db.cursor()
    try:
        cursor.execute("INSERT INTO TWEETS(`user`, context, time) VALUES('%s', '%s', '%s');"%(name, data['context'], data['date']))
        #print("OK")
        db.commit()
        return False
    except:
        #print(data)
        db.rollback()
        return True

def run(name):
    url = 'https://twitter.com/i/profiles/show/%s/timeline/tweets'%name
    r = requests.get(url)
    pool = ThreadPoolExecutor(max_workers=25)
    all_task = []
    if(r.status_code != 200):
        return False
    while True:
        data = json.loads(r.text)
        selector = etree.HTML(data['items_html'])
        tweets = selector.xpath("//@data-permalink-path")
        for tweet in tweets:
            tweet_url = 'http://twitter.com'+ tweet
            all_task.append(pool.submit(tweet_reader, tweet_url))
        if data['has_more_items']:
            url = 'https://twitter.com/i/profiles/show/%s/timeline/tweets?include_available_features=1&include_entities=1&max_position=%s&reset_error_state=false'%(name, data['min_position'])
            r = requests.get(url)
            time.sleep(5)
            continue
        else:
            db = pymysql.connect("127.0.0.1", "spider", 'spider', 'spider_result')
            for task in as_completed(all_task):
                result = task.result()
                if dump(name, result, db):
                    with open(os.path.dirname(os.path.abspath(__file__))+'/error_data.log', 'a') as f:
                        f.write("==============================================================================================\n")
                        f.write(name+'\n')
                        f.write(json.dumps(data)+'\n')
                        f.write("==============================================================================================\n")
            db.close()
            return True

if __name__ == "__main__":
    print(run("Twitter"))