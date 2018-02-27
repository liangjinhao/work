from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.sql import SQLContext, SparkSession
import pshc
import pymysql.cursors
import pymysql


"""
生成详情页面
"""


INDUSTRY_MAPPING = {
    '110000': '农林牧渔',
    '110100': '农林牧渔,种植业',
    '110101': '农林牧渔,种植业,种子生产',
    '110102': '农林牧渔,种植业,果蔬生产',
    '110103': '农林牧渔,种植业,其他种植业',
    '110200': '农林牧渔,渔业',
    '110201': '农林牧渔,渔业,海洋捕捞',
    '110202': '农林牧渔,渔业,水产养殖',
    '110300': '农林牧渔,林业',
    '110301': '农林牧渔,林业,林业',
    '110400': '农林牧渔,饲料',
    '110401': '农林牧渔,饲料,饲料',
    '110500': '农林牧渔,农产品加工',
    '110501': '农林牧渔,农产品加工,果蔬加工',
    '110502': '农林牧渔,农产品加工,粮油加工',
    '110504': '农林牧渔,农产品加工,其他农产品加工',
    '110600': '农林牧渔,农业综合',
    '110601': '农林牧渔,农业综合,农业综合',
    '110700': '农林牧渔,畜禽养殖',
    '110701': '农林牧渔,畜禽养殖,畜禽养殖',
    '110800': '农林牧渔,动物保健',
    '110801': '农林牧渔,动物保健,动物保健',
    '210000': '采掘',
    '210100': '采掘,石油开采',
    '210101': '采掘,石油开采,石油开采',
    '210200': '采掘,煤炭开采',
    '210201': '采掘,煤炭开采,煤炭开采',
    '210202': '采掘,煤炭开采,焦炭加工',
    '210300': '采掘,其他采掘',
    '210301': '采掘,其他采掘,其他采掘',
    '210400': '采掘,采掘服务',
    '210401': '采掘,采掘服务,油气钻采服务',
    '210402': '采掘,采掘服务,其他采掘服务',
    '220000': '化工',
    '220100': '化工,石油化工',
    '220101': '化工,石油化工,石油加工',
    '220103': '化工,石油化工,石油贸易',
    '220200': '化工,化学原料',
    '220201': '化工,化学原料,纯碱',
    '220202': '化工,化学原料,氯碱',
    '220203': '化工,化学原料,无机盐',
    '220204': '化工,化学原料,其他化学原料',
    '220300': '化工,化学制品',
    '220301': '化工,化学制品,氮肥',
    '220302': '化工,化学制品,磷肥',
    '220303': '化工,化学制品,农药',
    '220304': '化工,化学制品,日用化学产品',
    '220305': '化工,化学制品,涂料油漆油墨制造',
    '220306': '化工,化学制品,钾肥',
    '220307': '化工,化学制品,民爆用品',
    '220308': '化工,化学制品,纺织化学用品',
    '220309': '化工,化学制品,其他化学制品',
    '220310': '化工,化学制品,复合肥',
    '220311': '化工,化学制品,氟化工及制冷剂',
    '220312': '化工,化学制品,磷化工及磷酸盐',
    '220313': '化工,化学制品,聚氨酯',
    '220314': '化工,化学制品,玻纤',
    '220400': '化工,化学纤维',
    '220401': '化工,化学纤维,涤纶',
    '220402': '化工,化学纤维,维纶',
    '220403': '化工,化学纤维,粘胶',
    '220404': '化工,化学纤维,其他纤维',
    '220405': '化工,化学纤维,氨纶',
    '220500': '化工,塑料',
    '220501': '化工,塑料,其他塑料制品',
    '220502': '化工,塑料,合成革',
    '220503': '化工,塑料,改性塑料',
    '220600': '化工,橡胶',
    '220601': '化工,橡胶,轮胎',
    '220602': '化工,橡胶,其他橡胶制品',
    '220603': '化工,橡胶,炭黑',
    '230000': '钢铁',
    '230100': '钢铁,钢铁',
    '230101': '钢铁,钢铁,普钢',
    '230102': '钢铁,钢铁,特钢',
    '240000': '有色金属',
    '240200': '有色金属,金属非金属新材料',
    '240201': '有色金属,金属非金属新材料,金属新材料',
    '240202': '有色金属,金属非金属新材料,磁性材料',
    '240203': '有色金属,金属非金属新材料,非金属新材料',
    '240300': '有色金属,工业金属',
    '240301': '有色金属,工业金属,铝',
    '240302': '有色金属,工业金属,铜',
    '240303': '有色金属,工业金属,铅锌',
    '240400': '有色金属,黄金',
    '240401': '有色金属,黄金,黄金',
    '240500': '有色金属,稀有金属',
    '240501': '有色金属,稀有金属,稀土',
    '240502': '有色金属,稀有金属,钨',
    '240503': '有色金属,稀有金属,锂',
    '240504': '有色金属,稀有金属,其他稀有小金属',
    '270000': '电子',
    '270100': '电子,半导体',
    '270101': '电子,半导体,集成电路',
    '270102': '电子,半导体,分立器件',
    '270103': '电子,半导体,半导体材料',
    '270200': '电子,元件',
    '270202': '电子,元件,印制电路板',
    '270203': '电子,元件,被动元件',
    '270300': '电子,光学光电子',
    '270301': '电子,光学光电子,显示器件',
    '270302': '电子,光学光电子,LED',
    '270303': '电子,光学光电子,光学元件',
    '270400': '电子,其他电子',
    '270401': '电子,其他电子,其他电子',
    '270500': '电子,电子制造',
    '270501': '电子,电子制造,电子系统组装',
    '270502': '电子,电子制造,电子零部件制造',
    '280000': '汽车',
    '280100': '汽车,汽车整车',
    '280101': '汽车,汽车整车,乘用车',
    '280102': '汽车,汽车整车,商用载货车',
    '280103': '汽车,汽车整车,商用载客车',
    '280200': '汽车,汽车零部件',
    '280201': '汽车,汽车零部件,汽车零部件',
    '280300': '汽车,汽车服务',
    '280301': '汽车,汽车服务,汽车服务',
    '280400': '汽车,其他交运设备',
    '280401': '汽车,其他交运设备,其他交运设备',
    '330000': '家用电器',
    '330100': '家用电器,白色家电',
    '330101': '家用电器,白色家电,冰箱',
    '330102': '家用电器,白色家电,空调',
    '330103': '家用电器,白色家电,洗衣机',
    '330104': '家用电器,白色家电,小家电',
    '330105': '家用电器,白色家电,家电零部件',
    '330200': '家用电器,视听器材',
    '330201': '家用电器,视听器材,彩电',
    '330202': '家用电器,视听器材,其它视听器材',
    '340000': '食品饮料',
    '340300': '食品饮料,饮料制造',
    '340301': '食品饮料,饮料制造,白酒',
    '340302': '食品饮料,饮料制造,啤酒',
    '340303': '食品饮料,饮料制造,其他酒类',
    '340304': '食品饮料,饮料制造,软饮料',
    '340305': '食品饮料,饮料制造,葡萄酒',
    '340306': '食品饮料,饮料制造,黄酒',
    '340400': '食品饮料,食品加工',
    '340401': '食品饮料,食品加工,肉制品',
    '340402': '食品饮料,食品加工,调味发酵品',
    '340403': '食品饮料,食品加工,乳品',
    '340404': '食品饮料,食品加工,食品综合',
    '350000': '纺织服装',
    '350100': '纺织服装,纺织制造',
    '350101': '纺织服装,纺织制造,毛纺',
    '350102': '纺织服装,纺织制造,棉纺',
    '350103': '纺织服装,纺织制造,丝绸',
    '350104': '纺织服装,纺织制造,印染',
    '350105': '纺织服装,纺织制造,辅料',
    '350106': '纺织服装,纺织制造,其他纺织',
    '350200': '纺织服装,服装家纺',
    '350202': '纺织服装,服装家纺,男装',
    '350203': '纺织服装,服装家纺,女装',
    '350204': '纺织服装,服装家纺,休闲服装',
    '350205': '纺织服装,服装家纺,鞋帽',
    '350206': '纺织服装,服装家纺,家纺',
    '350207': '纺织服装,服装家纺,其他服装',
    '360000': '轻工制造',
    '360100': '轻工制造,造纸',
    '360101': '轻工制造,造纸,造纸',
    '360200': '轻工制造,包装印刷',
    '360201': '轻工制造,包装印刷,包装印刷',
    '360300': '轻工制造,家用轻工',
    '360302': '轻工制造,家用轻工,家具',
    '360303': '轻工制造,家用轻工,其他家用轻工',
    '360304': '轻工制造,家用轻工,珠宝首饰',
    '360305': '轻工制造,家用轻工,文娱用品',
    '360400': '轻工制造,其他轻工制造',
    '360401': '轻工制造,其他轻工制造,其他轻工制造',
    '370000': '医药生物',
    '370100': '医药生物,化学制药',
    '370101': '医药生物,化学制药,化学原料药',
    '370102': '医药生物,化学制药,化学制剂',
    '370200': '医药生物,中药',
    '370201': '医药生物,中药,中药',
    '370300': '医药生物,生物制品',
    '370301': '医药生物,生物制品,生物制品',
    '370400': '医药生物,医药商业',
    '370401': '医药生物,医药商业,医药商业',
    '370500': '医药生物,医疗器械',
    '370501': '医药生物,医疗器械,医疗器械',
    '370600': '医药生物,医疗服务',
    '370601': '医药生物,医疗服务,医疗服务',
    '410000': '公用事业',
    '410100': '公用事业,电力',
    '410101': '公用事业,电力,火电',
    '410102': '公用事业,电力,水电',
    '410103': '公用事业,电力,燃机发电',
    '410104': '公用事业,电力,热电',
    '410105': '公用事业,电力,新能源发电',
    '410200': '公用事业,水务',
    '410201': '公用事业,水务,水务',
    '410300': '公用事业,燃气',
    '410301': '公用事业,燃气,燃气',
    '410400': '公用事业,环保工程及服务',
    '410401': '公用事业,环保工程及服务,环保工程及服务',
    '420000': '交通运输',
    '420100': '交通运输,港口',
    '420101': '交通运输,港口,港口',
    '420200': '交通运输,高速公路',
    '420201': '交通运输,高速公路,高速公路',
    '420300': '交通运输,公交',
    '420301': '交通运输,公交,公交',
    '420400': '交通运输,航空运输',
    '420401': '交通运输,航空运输,航空运输',
    '420500': '交通运输,机场',
    '420501': '交通运输,机场,机场',
    '420600': '交通运输,航运',
    '420601': '交通运输,航运,航运',
    '420700': '交通运输,铁路运输',
    '420701': '交通运输,铁路运输,铁路运输',
    '420800': '交通运输,物流',
    '420801': '交通运输,物流,物流',
    '430000': '房地产',
    '430100': '房地产,房地产开发',
    '430101': '房地产,房地产开发,房地产开发',
    '430200': '房地产,园区开发',
    '430201': '房地产,园区开发,园区开发',
    '450000': '商业贸易',
    '450200': '商业贸易,贸易',
    '450201': '商业贸易,贸易,贸易',
    '450300': '商业贸易,一般零售',
    '450301': '商业贸易,一般零售,百货',
    '450302': '商业贸易,一般零售,超市',
    '450303': '商业贸易,一般零售,多业态零售',
    '450400': '商业贸易,专业零售',
    '450401': '商业贸易,专业零售,专业连锁',
    '450500': '商业贸易,商业物业经营',
    '450501': '商业贸易,商业物业经营,一般物业经营',
    '450502': '商业贸易,商业物业经营,专业市场',
    '460000': '休闲服务',
    '460100': '休闲服务,景点',
    '460101': '休闲服务,景点,人工景点',
    '460102': '休闲服务,景点,自然景点',
    '460200': '休闲服务,酒店',
    '460201': '休闲服务,酒店,酒店',
    '460300': '休闲服务,旅游综合',
    '460301': '休闲服务,旅游综合,旅游综合',
    '460400': '休闲服务,餐饮',
    '460401': '休闲服务,餐饮,餐饮',
    '460500': '休闲服务,其他休闲服务',
    '460501': '休闲服务,其他休闲服务,其他休闲服务',
    '480000': '银行',
    '480100': '银行,银行',
    '480101': '银行,银行,银行',
    '490000': '非银金融',
    '490100': '非银金融,证券',
    '490101': '非银金融,证券,证券',
    '490200': '非银金融,保险',
    '490201': '非银金融,保险,保险',
    '490300': '非银金融,多元金融',
    '490301': '非银金融,多元金融,多元金融',
    '510000': '综合',
    '510100': '综合,综合',
    '510101': '综合,综合,综合',
    '610000': '建筑建材',
    '610100': '建筑建材,水泥制造',
    '610101': '建筑建材,水泥制造,水泥制造',
    '610200': '建筑建材,玻璃制造',
    '610201': '建筑建材,玻璃制造,玻璃制造',
    '610300': '建筑建材,其他建材',
    '610301': '建筑建材,其他建材,耐火材料',
    '610302': '建筑建材,其他建材,管材',
    '610303': '建筑建材,其他建材,其他建材',
    '620000': '建筑装饰',
    '620100': '建筑装饰,房屋建设',
    '620101': '建筑装饰,房屋建设,房屋建设',
    '620200': '建筑装饰,装修装饰',
    '620201': '建筑装饰,装修装饰,装修装饰',
    '620300': '建筑装饰,基础建设',
    '620301': '建筑装饰,基础建设,城轨建设',
    '620302': '建筑装饰,基础建设,路桥施工',
    '620303': '建筑装饰,基础建设,水利工程',
    '620304': '建筑装饰,基础建设,铁路建设',
    '620305': '建筑装饰,基础建设,其他基础建设',
    '620400': '建筑装饰,专业工程',
    '620401': '建筑装饰,专业工程,钢结构',
    '620402': '建筑装饰,专业工程,化学工程',
    '620403': '建筑装饰,专业工程,国际工程承包',
    '620404': '建筑装饰,专业工程,其他专业工程',
    '620500': '建筑装饰,园林工程',
    '620501': '建筑装饰,园林工程,园林工程',
    '630000': '电气设备',
    '630100': '电气设备,电机',
    '630101': '电气设备,电机,电机',
    '630200': '电气设备,电气自动化设备',
    '630201': '电气设备,电气自动化设备,电网自动化',
    '630202': '电气设备,电气自动化设备,工控自动化',
    '630203': '电气设备,电气自动化设备,计量仪表',
    '630300': '电气设备,电源设备',
    '630301': '电气设备,电源设备,综合电力设备商',
    '630302': '电气设备,电源设备,风电设备',
    '630303': '电气设备,电源设备,光伏设备',
    '630304': '电气设备,电源设备,火电设备',
    '630305': '电气设备,电源设备,储能设备',
    '630306': '电气设备,电源设备,其它电源设备',
    '630400': '电气设备,高低压设备',
    '630401': '电气设备,高低压设备,高压设备',
    '630402': '电气设备,高低压设备,中压设备',
    '630403': '电气设备,高低压设备,低压设备',
    '630404': '电气设备,高低压设备,线缆部件及其他',
    '640000': '机械设备',
    '640100': '机械设备,通用机械',
    '640101': '机械设备,通用机械,机床工具',
    '640102': '机械设备,通用机械,机械基础件',
    '640103': '机械设备,通用机械,磨具磨料',
    '640104': '机械设备,通用机械,内燃机',
    '640105': '机械设备,通用机械,制冷空调设备',
    '640106': '机械设备,通用机械,其它通用机械',
    '640200': '机械设备,专用设备',
    '640201': '机械设备,专用设备,工程机械',
    '640202': '机械设备,专用设备,重型机械',
    '640203': '机械设备,专用设备,冶金矿采化工设备',
    '640204': '机械设备,专用设备,楼宇设备',
    '640205': '机械设备,专用设备,环保设备',
    '640206': '机械设备,专用设备,纺织服装设备',
    '640207': '机械设备,专用设备,农用机械',
    '640208': '机械设备,专用设备,印刷包装机械',
    '640209': '机械设备,专用设备,其它专用机械',
    '640300': '机械设备,仪器仪表',
    '640301': '机械设备,仪器仪表,仪器仪表',
    '640400': '机械设备,金属制品',
    '640401': '机械设备,金属制品,金属制品',
    '640500': '机械设备,运输设备',
    '640501': '机械设备,运输设备,铁路设备',
    '650000': '国防军工',
    '650100': '国防军工,航天装备',
    '650101': '国防军工,航天装备,航天装备',
    '650200': '国防军工,航空装备',
    '650201': '国防军工,航空装备,航空装备',
    '650300': '国防军工,地面兵装',
    '650301': '国防军工,地面兵装,地面兵装',
    '650400': '国防军工,船舶制造',
    '650401': '国防军工,船舶制造,船舶制造',
    '710000': '计算机',
    '710100': '计算机,计算机设备',
    '710101': '计算机,计算机设备,计算机设备',
    '710200': '计算机,计算机应用',
    '710201': '计算机,计算机应用,软件开发',
    '710202': '计算机,计算机应用,IT服务',
    '720000': '传媒',
    '720100': '传媒,文化传媒',
    '720101': '传媒,文化传媒,平面媒体',
    '720102': '传媒,文化传媒,影视动漫',
    '720103': '传媒,文化传媒,有线电视网络',
    '720104': '传媒,文化传媒,其他文化传媒',
    '720200': '传媒,营销传播',
    '720201': '传媒,营销传播,营销服务',
    '720300': '传媒,互联网传媒',
    '720301': '传媒,互联网传媒,互联网信息服务',
    '720302': '传媒,互联网传媒,移动互联网服务',
    '720303': '传媒,互联网传媒,其他互联网服务',
    '730000': '通信',
    '730100': '通信,通信运营',
    '730101': '通信,通信运营,通信运营',
    '730200': '通信,通信设备',
    '730201': '通信,通信设备,终端设备',
    '730202': '通信,通信设备,通信传输设备',
    '730203': '通信,通信设备,通信配套服务'
}


def add_file_info(x):
    """
    这一步的操作是查询 Mysql，获取每个图片的研报信息，并确定出每个图片所属的行业
    :param x:
    :return:
    """
    host = '10.117.211.16'
    port = 6033
    user = 'core_bj'
    password = 'alUXKHIrJoAOuI26'
    db = 'core_doc'
    table = 'hibor'
    connection = pymysql.connect(host=host, port=port, user=user, password=password,
                                 db=db, charset='utf8', cursorclass=pymysql.cursors.DictCursor)

    result = []
    for row in x:
        new_row = dict({
            "id": '',
            "title": '',
            "legends": '',
            "img_url": '',
            "create_time": '',
            "fileId": '',
            "fileUrl": '',
            "typetitle": '',
            "rating": '',
            "stockname": '',
            "author": '',
            "publish": '',
            "file_title": '',
            "industry_id": '',
        })

        new_row['id'] = row['id']
        new_row['create_time'] = row['create_time']
        new_row['img_url'] = row['pngFile']
        new_row['title'] = row['title']
        new_row['fileId'] = row['fileId']
        new_row['fileUrl'] = row['fileUrl']

        legends = row['legends'] if 'legends' in row else row['legends_str']
        new_legends = []

        if legends is not None and legends is not []:
            if isinstance(eval(legends), list):
                for i in eval(legends):
                    text = i['text'] if 'text' in i else str(i)
                    if text is not None:
                        new_legends.append(text)
            else:
                print(row)

        if new_legends is not []:
            new_row['legends'] = ','.join(new_legends)
        else:
            new_row['legends'] = ''

        # 查询 Mysql 获取相关信息
        cursor = connection.cursor()
        sql = "SELECT * FROM " + db + "." + table + " WHERE id = " + new_row['fileId'] + ";"
        cursor.execute(sql)
        cursor_row = cursor.fetchone()

        # 如果在 Hibor 表中没找到该数据，则跳过
        if cursor_row is None:
            continue

        new_row["typetitle"] = cursor_row['typetitle'] if 'typetitle' in cursor_row else ''
        new_row["rating"] = cursor_row['rating'] if 'rating' in cursor_row else ''
        new_row["stockname"] = cursor_row['stockname'] if 'stockname' in cursor_row else ''
        new_row["author"] = cursor_row['author'] if 'author' in cursor_row else ''
        new_row["publish"] = cursor_row['publish'] if 'publish' in cursor_row else ''
        new_row["file_title"] = cursor_row['title'] if 'title' in cursor_row else ''
        new_row["industry_id"] = cursor_row['industry_id'] if 'industry_id' in cursor_row else ''

        if new_row["industry_id"] is not None and new_row["industry_id"] in INDUSTRY_MAPPING:
            new_row["industry_id"] = INDUSTRY_MAPPING[new_row["industry_id"]]
        else:
            new_row["industry_id"] = '其他'
            # 如果该张图片无行业类型，则忽略
            continue

        result.append(new_row)

    return result


def norm_data(x):
    host = '10.25.170.41'
    port = 3306
    user = 'bj_static_ro'
    password = 'ad26cc2017eb'
    db = 'data_center'
    table = 'sec_industry_new'
    connection = pymysql.connect(host=host, port=port, user=user, password=password,
                                 db=db, charset='utf8', cursorclass=pymysql.cursors.DictCursor)

    result = []
    for row in x:
        new_row = dict()
        new_row['id'] = row['id']
        new_row['create_time'] = row['create_time']
        new_row['img_url'] = row['pngFile']
        new_row['title'] = row['title']
        new_row['fileId'] = row['fileId']
        new_row['fileUrl'] = row['fileUrl']
        new_row["typetitle"] = row['typetitle']
        new_row["rating"] = row['rating']
        new_row["stockname"] = row['stockname']
        new_row["stockcode"] = row['stockcode']
        new_row["author"] = row['author']
        new_row["publish"] = row['publish']
        new_row["file_title"] = row['file_title']
        new_row["category_id"] = row["category_id"]

        # 目前只要沪深股的
        if not new_row["category_id"].startswith('R001'):
            continue

        cursor = connection.cursor()
        sql = "SELECT * FROM " + db + "." + table + " WHERE stk_code = " + new_row['stockcode'] + \
              ", and indu_standard=1001014" + ";"
        cursor.execute(sql)
        cursor_row = cursor.fetchone()

        if cursor_row is None:
            new_row["industry_id"] = ''
            new_row["industry"] = ''
        else:
            new_row["industry_id"] = cursor_row['third_indu_code']
            new_row["industry"] = cursor_row['third_indu_name']

        legends = row['legends_str']
        new_legends = []
        if legends is not None and legends is not []:
            if isinstance(eval(legends), list):
                for i in eval(legends):
                    text = i['text'] if 'text' in i else str(i)
                    if text is not None:
                        new_legends.append(text)
        if new_legends is not []:
            new_row['legends'] = ','.join(new_legends)
        else:
            new_row['legends'] = ''

        result.append(new_row)
    return result


def extract(x):
    result = []
    for row in x:
        result.append((row['fileId'], row['img_url']))
    return result


if __name__ == '__main__':

    conf = SparkConf().setAppName("GetInfoTable")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    sparkSession = SparkSession.builder\
        .enableHiveSupport() \
        .config(conf=conf)\
        .getOrCreate()

    connector = pshc.PSHC(sc, sqlContext)

    catelog = {
        "table": {"namespace": "default", "name": "hb_charts"},
        "rowkey": "id",
        "columns": {
            "id": {"cf": "rowkey", "col": "key", "type": "string"},
            # "algorithmCommitTime": {"cf": "data", "col": "algorithmCommitTime", "type": "string"},
            # "area": {"cf": "data", "col": "area", "type": "string"},
            # "chartType": {"cf": "data", "col": "chartType", "type": "string"},
            # "chart_version": {"cf": "data", "col": "chart_version", "type": "string"},
            # "confidence": {"cf": "data", "col": "confidence", "type": "string"},
            "create_time": {"cf": "data", "col": "create_time", "type": "string"},
            # "data": {"cf": "data", "col": "data", "type": "string"},  # 图片数据
            # "deleted": {"cf": "data", "col": "deleted", "type": "string"},
            "fileId": {"cf": "data", "col": "fileId", "type": "string"},
            # "filePath": {"cf": "data", "col": "filePath", "type": "string"},
            "fileUrl": {"cf": "data", "col": "fileUrl", "type": "string"},  # PDF文件url
            # "fonts": {"cf": "data", "col": "fonts", "type": "string"},
            # "hAxis": {"cf": "data", "col": "hAxis", "type": "string"},  # X轴
            # "hAxisTextD": {"cf": "data", "col": "hAxisTextD", "type": "string"},  # X轴上方文本
            # "hAxisTextU": {"cf": "data", "col": "hAxisTextU", "type": "string"},  # X轴下方文本
            # "is_ppt": {"cf": "data", "col": "is_ppt", "type": "string"},
            # "last_updated": {"cf": "data", "col": "last_updated", "type": "string"},
            "legends": {"cf": "data", "col": "legends", "type": "string"},  # 图例
            # "lvAxis": {"cf": "data", "col": "lvAxis", "type": "string"},  # Y轴左
            # "ocrEngine": {"cf": "data", "col": "ocrEngine", "type": "string"},
            # "pageIndex": {"cf": "data", "col": "pageIndex", "type": "string"},
            # "page_area": {"cf": "data", "col": "page_area", "type": "string"},
            "pngFile": {"cf": "data", "col": "pngFile", "type": "string"},  # 图片url
            # "rvAxis": {"cf": "data", "col": "rvAxis", "type": "string"},  # Y轴右
            # "source": {"cf": "data", "col": "source", "type": "string"},  #
            # "state": {"cf": "data", "col": "state", "type": "string"},
            # "svgFile": {"cf": "data", "col": "svgFile", "type": "string"},
            # "text_info": {"cf": "data", "col": "text_info", "type": "string"},  # 文本信息
            "title": {"cf": "data", "col": "title", "type": "string"},  # 标题
            # "vAxisTextL": {"cf": "data", "col": "vAxisTextL", "type": "string"},  # Y轴左边文本
            # "vAxisTextR": {"cf": "data", "col": "vAxisTextR", "type": "string"},  # Y轴右边文本
        }
    }

    # startTime = datetime.datetime.strptime('2018-2-9 11:59:59', '%Y-%m-%d %H:%M:%S').strftime('%s') + '000'
    # stopTime = datetime.datetime.strptime('2018-05-01 1:0:0', '%Y-%m-%d %H:%M:%S').strftime('%s') + '000'
    #
    # hb_charts_df = connector.get_df_from_hbase(catelog, start_row=None, stop_row=None, start_time=startTime,
    #                                            stop_time=stopTime, repartition_num=None, cached=True)
    # hb_charts_df.show()
    # # print('----hb_charts_df COUNT:---\n', hb_charts_df.count())
    #
    # hb_charts_hibor_rdd = hb_charts_df.rdd.mapPartitions(add_file_info)\
    #     .persist(storageLevel=StorageLevel.DISK_ONLY)

    hb_charts_df = sparkSession.sql('SELECT * FROM abc.hb_charts')
    hibor_df = sparkSession\
        .sql('SELECT hibor_key, stockcode, stockname, title, typetitle, rating, author, publish, category_id '
             'FROM abc.hibor')\
        .rdd.map(lambda x: (x['hibor_key'].split(':')[-1], x['stockcode'], x['stockname'], x['title'],
                           x['typetitle'], x['rating'], x['author'], x['publish'], x['category_id']))\
        .toDF(['hibor_key', 'stockcode', 'stockname', 'title', 'typetitle', 'rating', 'author', 'publish', 'category_id'])

    df1 = hb_charts_df.registerTempTable('df1')
    df2 = hibor_df.registerTempTable('df2')

    # 先生成合并 hb_charts 和 hibor 的合成 DataFrame
    hb_charts_hibor_tmp = sparkSession.sql(
        "SELECT df1.key, df1.create_time, df1.pngFile, df1.title, df1.fileId, df1.fileUrl, df1.legends_str, "
        "df2.typetitle, df2.rating, df2.stockname, df2.stockcode, df2.author, df2.publish, df2.title as file_title, "
        "df2.category_id "
        "FROM df1 JOIN df2 ON df1.fileId = df2.hibor_key")
    hb_charts_hibor_rdd = hb_charts_hibor_tmp.rdd.mapPartitions(norm_data).persist(storageLevel=StorageLevel.DISK_ONLY)
    hb_charts_hibor_df = sparkSession.createDataFrame(hb_charts_hibor_rdd)
    print('----hb_charts_hibor_df COUNT:---\n', hb_charts_hibor_df.count())
    hb_charts_hibor_df.show()

    # 计算出研报文件和图片对应的 DataFrame
    file_to_img_catelog = {
        "columns": {
            "fileId": {"cf": "data", "col": "fileId", "type": "string"},
            "peer_imgs": {"cf": "data", "col": "img_url", "type": "string"},
        }
    }
    file_to_img_rdd = hb_charts_hibor_rdd.mapPartitions(extract)\
        .reduceByKey(lambda a, b: a + ',' + b)\
        .persist(storageLevel=StorageLevel.DISK_ONLY)
    file_to_img_df = sparkSession.createDataFrame(file_to_img_rdd,
                                                  schema=connector.catelog_to_schema(file_to_img_catelog))
    print('----file_to_img_df COUNT:---\n', file_to_img_df.count())
    file_to_img_df.show()
    file_to_img_df.write.saveAsTable('abc.file_to_img', mode='overwrite')

    # 生成 InfoTable 的 DataFrame
    hb_charts_hibor_df.registerTempTable('table1')
    file_to_img_df.registerTempTable('table2')
    html_df = sparkSession.sql("SELECT table1.*, table2.peer_imgs "
                               "FROM table1 JOIN table2 ON table1.fileId == table2.fileId")
    print('----html_df COUNT:---\n', html_df.count())
    html_df.show()

    # 将 InfoTable 保存至 Hbase
    html_catelog = {
        "table": {"namespace": "default", "name": "SEO_info"},
        "rowkey": "id",
        "columns": {
            "id": {"cf": "rowkey", "col": "key", "type": "string"},
            "title": {"cf": "data", "col": "content", "type": "string"},
            "legends": {"cf": "data", "col": "keywords", "type": "string"},
            "img_url": {"cf": "data", "col": "img_url", "type": "string"},
            "create_time": {"cf": "data", "col": "create_time", "type": "string"},
            "fileId": {"cf": "data", "col": "fileId", "type": "string"},
            "fileUrl": {"cf": "data", "col": "fileUrl", "type": "string"},
            "peer_imgs": {"cf": "data", "col": "peer_imgs", "type": "string"},
            "typetitle": {"cf": "data", "col": "typetitle", "type": "string"},
            "rating": {"cf": "data", "col": "rating", "type": "string"},
            "stockname": {"cf": "data", "col": "stockname", "type": "string"},
            "stockcode": {"cf": "data", "col": "stockcode", "type": "string"},
            "author": {"cf": "data", "col": "author", "type": "string"},
            "publish": {"cf": "data", "col": "publish", "type": "string"},
            "file_title": {"cf": "data", "col": "file_title", "type": "string"},
            "industry_id": {"cf": "data", "col": "industry_id", "type": "string"},
            "industry": {"cf": "data", "col": "industry", "type": "string"},
            "category_id": {"cf": "data", "col": "category_id", "type": "string"},
        }
    }
    connector.save_df_to_hbase(html_df, html_catelog)
