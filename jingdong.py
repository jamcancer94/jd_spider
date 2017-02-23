# coding:utf-8

import requests
import json
import time
import re
from lxml import html
from pymongo import MongoClient
from multiprocessing.dummy import Pool as ThreadPool

# mongodb
client = MongoClient()
dbName = 'jingdong'
dbTable = 'link_all_2'
tab = client[dbName][dbTable]
dbTable2 = 'phone_detail'
tab2 = client[dbName][dbTable2]
dbTable3 = 'link_all'
tab3 = client[dbName][dbTable3]

header = {'User-Agent': 'xxxxx'}
cookie = {'Cookie': 'xxxxxx'}

# 获取商品ID，入库
def getItem():
    id = {}
    for i in range(130):
        url = 'https://list.jd.com/list.html?cat=9987,653,655&page=' + str(i + 1) + \
              '&stock=0&sort=sort_rank_asc&trans=1&JL=4_7_0#J_main'
        # getItem(url)
        rep = requests.get(url, headers=header, cookies=cookie)
        if rep.status_code == 200:
            sel = html.fromstring(rep.content)
            link = sel.xpath('//ul/li/div/div[@class="p-img"]/a/@href')
            # items = re.findall('item.jd.com/(\d+).html', link, re.S)
            for k in link:
                print re.findall('item.jd.com/(\d+).html', k, re.S)[0]
                id['_id'] = re.findall('item.jd.com/(\d+).html', k, re.S)[0]
                tab.insert(id)
            print rep.status_code
        else:
            print i

# 去重
def getID():
    link = tab.find()
    link_all = tab3.find()
    phone_id = []
    phone_id_all = []
    for i in link:
        phone_id.append(i['_id'])
    for k in link_all:
        phone_id_all.append(k['_id'])

    print len(phone_id)
    print len(phone_id_all)

    phone_id_remain = [var for var in phone_id_all if var not in phone_id]

    phone_id_same = [var for var in phone_id_all if var in phone_id]

    rel_list = list(set(phone_id)^set(phone_id_all))
    # for i in phone_id_all:
    #     if i not in phone_id:
    #         phone_id_remain.append(i)

    print len(phone_id_remain)
    print len(phone_id_same)
    print len(rel_list)
    # id_s = set(phone_id)
    # print len(id_s)

    # if len(id_s) == len(phone_id):
    #     print u'没有重复元素'
    return rel_list

def getDetail(id):
    phone_id = id
    item = {}
    url_phone = 'https://item.m.jd.com/product/{}.html'.format(phone_id)

    try:
        try:
            # 商品详情
            url_detail = 'https://item.m.jd.com/ware/detail.json?wareId=' + str(phone_id)
            detail_json = requests.get(url_detail).content
            detail_json_result = json.loads(detail_json, 'utf-8')

            # 商品名称
            name = detail_json_result['ware']['wname']
            if name:
                item[u'商品名称'] = name

            # 配送信息
            service = detail_json_result['ware']['service']
            if service:
                item[u'配送信息'] = service

            # 日期和其他信息
            detail_code = detail_json_result['ware']['wi']['code']
            try:
                brand = re.findall(u'<td class="tdTitle">品牌</td><td>(.*?)</td>', detail_code)[0]
                if brand:
                    item[u'品牌'] = brand
            except Exception:
                pass

            try:
                data = re.findall(u'<td class="tdTitle">上市年份</td><td>(.*?)</td>', detail_code)[0]
                if data:
                    item[u'上市年份'] = data
            except Exception:
                pass

        except Exception:
            pass

        try:
            # 商品评论
            url_comment = 'https://item.m.jd.com/ware/getDetailCommentList.json?wareId=' + str(phone_id)
            comment_json = requests.get(url_comment).content
            comment_json_result = json.loads(comment_json, 'utf-8')

            # comments
            allCnt = comment_json_result['wareDetailComment']['allCnt']
            item[u'评论总数'] = allCnt
            badCnt = comment_json_result['wareDetailComment']['badCnt']
            item[u'差评'] = badCnt
            goodCnt = comment_json_result['wareDetailComment']['goodCnt']
            item[u'好评'] = goodCnt
            item[u'中评'] = comment_json_result['wareDetailComment']['normalCnt']
            item[u'有图评论'] = comment_json_result['wareDetailComment']['pictureCnt']
            item[u'好评率'] = float(goodCnt) / float(allCnt) * 100
            item[u'差评率'] = float(badCnt) / float(allCnt) * 100
        except Exception:
            pass

    except Exception:
        pass

    try:

        phonePage = requests.get(url_phone).content
        page_sel = html.fromstring(phonePage)
        price = page_sel.xpath('//span[@class="big-price"]/text()')[0]
        try:
            zi_yin = page_sel.xpath('//span[@class="label-text white-text"]/text()')[0]
            item[u'自营'] = u'自营'
        except:
            item[u'自营'] = u'否'
        if price:
            item[u'价格'] = price

    except Exception:
        pass

    item['phone_id'] = phone_id
    tab2.insert(item)
    print u'商品%s 写入完成' % phone_id
    time.sleep(5)


if __name__ == '__main__':

    # 爬取ID
    # getItem()

    # 去重
    getID()

    phone_id = getID()
    pool = ThreadPool(10)
    time_start = time.time()
    pool.map(getDetail, phone_id)
    pool.close()
    pool.join()
    print u'写入完成'
    print 'finished:%s' % (time.time() - time_start)