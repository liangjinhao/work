import urllib.request
import re
import time
from pyquery import PyQuery as pq
import urllib.parse
import urllib.request
import math

Data = dict()

"""
本脚本访问爱站网 https://www.aizhan.com 查询url的相关信息，比如访问IP数，page rank值，索引数目等等
"""


def get_site_info(url):
    """
    访问爱站网 https://www.aizhan.com 查询一个 url 的相关信息
    :param url:
    :return:
    """

    original_url = url
    try:
        url = 'https://www.aizhan.com/cha/' + url.replace('http://', '').replace('https://', '')
        user_agent = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64)'
        values = {'name': 'Brown',
                  'location': 'Beijing',
                  'language': 'Python'}
        headers = {'User-Agent': user_agent}
        data = urllib.parse.urlencode(values)
        data = data.encode('ascii')
        req = urllib.request.Request(url, data, headers)
        html_doc = str(urllib.request.urlopen(req).read(), 'utf-8')
    except Exception as e:
        print(url, e)
        return None

    doc = pq(html_doc)

    baidurank_ip = doc('#baidurank_ip').text()
    if re.match('[\d,]+ ~ [\d,]+', baidurank_ip):
        ips = [i.strip() for i in baidurank_ip.split('~')]
        baidurank_ip = int((int(ips[0].replace(',', '')) + int(ips[1].replace(',', '')))/2)
    else:
        baidurank_ip = 0

    baidurank_m_ip = doc('#baidurank_m_ip').text()
    if re.match('[\d,]+ ~ [\d,]+', baidurank_m_ip):
        ips = [i.strip() for i in baidurank_m_ip.split('~')]
        baidurank_m_ip = int((int(ips[0].replace(',', '')) + int(ips[1].replace(',', '')))/2)
    else:
        baidurank_m_ip = 0

    alexa_rank = doc('#alexa_rank').text()
    if re.match('\d+', alexa_rank):
        alexa_rank = int(alexa_rank)
    else:
        alexa_rank = None

    baiduindex = doc('#baiduindex').text()
    if re.match('[\d,]+', baiduindex):
        baiduindex = int(baiduindex.replace(',', ''))
    else:
        baiduindex = 0

    shoulu_baidu = doc('#shoulu1_baidu').text()
    if re.match('[\d,]+', shoulu_baidu):
        shoulu_baidu = int(shoulu_baidu.replace(',', ''))
    else:
        shoulu_baidu = 0

    fanlian_baidu = doc('#shoulu2_baidu').text()
    if re.match('[\d,]+', fanlian_baidu):
        fanlian_baidu = int(fanlian_baidu.replace(',', ''))
    else:
        fanlian_baidu = 0

    baidurank_br_raw = doc('#baidurank_br')('img')[0].attrib['src'].split('/')[-1].strip('.png')
    if re.match('\d{1,2}', baidurank_br_raw):
        baidurank_br = int(baidurank_br_raw)
    else:
        baidurank_br = None
        print('Wrong baidurank_br Rank: ', doc('#baidurank_br')('img')[0].attrib['src'])

    baidurank_mbr_raw = doc('#baidurank_mbr')('img')[0].attrib['src'].split('/')[-1].strip('.png')
    if re.match('\d{1,2}', baidurank_mbr_raw):
        baidurank_mbr = int(baidurank_mbr_raw)
    else:
        baidurank_mbr = None
        print('Wrong baidurank_mbr Rank: ', doc('#baidurank_mbr')('img')[0].attrib['src'])

    sogou_pr_raw = doc('#sogou_pr')('img')[0].attrib['src'].split('/')[-1].strip('.png')
    if re.match('\d{1,2}', sogou_pr_raw):
        sogou_pr = int(sogou_pr_raw)
    else:
        sogou_pr = None
        print('Wrong sogou_pr Rank: ', doc('#sogou_pr')('img')[0].attrib['src'])

    google_pr_raw = doc('#google_pr')('img')[0].attrib['src'].split('/')[-1].strip('.png')
    if re.match('\d{1,2}', google_pr_raw):
        google_pr = int(google_pr_raw)
    else:
        google_pr = None
        print('Wrong google_pr Rank: ', doc('#google_pr')('img')[0].attrib['src'])

    pr_sum = 0
    num = 0
    for i in [baidurank_br, baidurank_mbr, sogou_pr, google_pr]:
        if i is not None:
            pr_sum += i
            num += 1
    pr = int(pr_sum/num) if num != 0 else None

    result = {'url': original_url,
              'info': {
                  'baidurank_ip': baidurank_ip,
                  'baidurank_m_ip': baidurank_m_ip,
                  'alexa_rank': alexa_rank,
                  'baiduindex': baiduindex,
                  'shoulu_baidu': shoulu_baidu,
                  'fanlian_baidu': fanlian_baidu,
                  'pr': pr
                 }
              }
    print(url, alexa_rank, baidurank_ip, baiduindex, baidurank_m_ip, shoulu_baidu, fanlian_baidu, pr)
    return result


if __name__ == '__main__':

    with open('urls_new.txt') as f1, open('urls_new_info.txt', 'w') as f2:
        for line in f1:
            res = get_site_info(line.strip())
            if res is None:
                print('=========!!========', line.strip())
                continue
            Data[res['url']] = res['info']
            f2.write(res['url'] + '\t' +
                     str(res['info']['baidurank_ip']) + '\t' +
                     str(res['info']['baidurank_m_ip']) + '\t' +
                     str(res['info']['alexa_rank']) + '\t' +
                     str(res['info']['baiduindex']) + '\t' +
                     str(res['info']['shoulu_baidu']) + '\t' +
                     str(res['info']['fanlian_baidu']) + '\t' +
                     str(res['info']['pr']) + '\n'
                     )
            time.sleep(0.001)

    baidurank_ip_total = 0
    baidurank_m_ip_total = 0
    alexa_rank_total = 0
    baiduindex_total = 0
    shoulu_baidu_total = 0
    fanlian_baidu_total = 0
    pr_total = 0
    count = 0

    for res in Data:
        count += 1

        baidurank_ip_total += Data[res]['baidurank_ip']
        baidurank_m_ip_total += Data[res]['baidurank_m_ip']
        alexa_rank_total += Data[res]['alexa_rank'] if Data[res]['alexa_rank'] is not None else 0
        baiduindex_total += Data[res]['baiduindex']
        shoulu_baidu_total += Data[res]['shoulu_baidu']
        fanlian_baidu_total += Data[res]['fanlian_baidu']
        pr_total += Data[res]['pr']

    AVE_baidurank_ip = int(baidurank_ip_total/count)
    AVE_baidurank_m_ip = int(baidurank_m_ip_total/count)
    AVE_alexa_rank = int(alexa_rank_total/count)
    AVE_baiduindex = int(baiduindex_total/count)
    AVE_shoulu_baidu = int(shoulu_baidu_total/count)
    AVE_fanlian_baidu = int(fanlian_baidu_total/count)
    AVE_pr_total = int(pr_total/count)

    print('AVE_baidurank_ip:', AVE_baidurank_ip,
          '\nAVE_baidurank_m_ip:', AVE_baidurank_m_ip,
          '\nAVE_alexa_rank:', AVE_alexa_rank,
          '\nAVE_baiduindex:', AVE_baiduindex,
          '\nAVE_shoulu_baidu:', AVE_shoulu_baidu,
          '\nAVE_fanlian_baidu:', AVE_fanlian_baidu,
          '\nAVE_pr_total:', AVE_pr_total)

    score_map = {}

    for res in Data:

        ip_score = math.log((AVE_baidurank_ip + AVE_baidurank_m_ip)/(Data[res]['baidurank_ip'] + Data[res]['baidurank_m_ip'] + 1))
        link_score = math.log((AVE_shoulu_baidu + AVE_fanlian_baidu)/(Data[res]['shoulu_baidu'] + Data[res]['fanlian_baidu'] + 1))
        alexa_score = math.log(Data[res]['alexa_rank']/AVE_alexa_rank) if Data[res]['alexa_rank'] else math.log(100000/AVE_alexa_rank)
        index_score = math.log(AVE_baiduindex/(Data[res]['baiduindex'] + 1))
        pr_score = math.log(AVE_pr_total/(Data[res]['pr'] + 1))
        final_score = 0.4*ip_score + 1*link_score + 0.8*alexa_score + 1*index_score + 10*pr_score
        print(res, final_score, '= 0.4*', ip_score, '+ 1*', link_score, '+ 0.8*', alexa_score, '+ 1*', index_score, '+ 10*', pr_score)
        # print(res, final_score, ip_score, link_score, alexa_score, index_score)
        score_map[res] = final_score

    sorted_score = sorted(score_map.items(), key=lambda x: x[1])

    with open('result.txt', 'w') as f:
        index = 1
        for i in sorted_score:
            f.write(str(i[0]) + '\t' +
                    str(i[1]) + '\t' +
                    str(Data[i[0]]['baidurank_ip']) + '\t' +
                    str(Data[i[0]]['baidurank_m_ip']) + '\t' +
                    str(Data[i[0]]['alexa_rank']) + '\t' +
                    str(Data[i[0]]['baiduindex']) + '\t' +
                    str(Data[i[0]]['shoulu_baidu']) + '\t' +
                    str(Data[i[0]]['fanlian_baidu']) + '\t' +
                    str(Data[i[0]]['pr']) + '\n')
            index += 1
