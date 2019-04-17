#coding=utf-8

#导入路径
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import requests
import re
# 暂时使用baidu-aip 用来数字识别
# import pytesseract
from PIL import Image
import PIL.ImageOps
from aip import AipOcr
import json
from base import webbase
from common import conf,logoutput,getxml,mysql
import pandas as pd

class Ziroom():

    def __init__(self):
        setup = webbase.SetUp()
        dr = setup.web_setup()
        self.driver = webbase.Web(dr)
        self.cf = conf.Conf()
        self.driver.get_url(self.cf.get_conf_data("ziroom")["roomurl"])
        self.lg = logoutput.Logger()
        self.gx = getxml.XmlOperation()
        self.db = mysql.Mysql()
        self.db.connect_mysql()
        self.cur = self.db.cur
        self.data=pd.DataFrame(columns=["图片地址","平米数","楼层","房屋格局","交通位置","价格","小区名称","租房页面url","收费方式"])

    def get_data(self):

        picUrl=self.gx.get_xml_data("room_list_page","pic_url_link")
        self.driver.scroll_page()
        lst=self.driver.get_elements(picUrl,waittime=3)
        # 获取图片地址和小区名称
        tmpDf=pd.DataFrame(columns=["图片地址","平米数","楼层","房屋格局","交通位置","价格","小区名称","租房页面url","收费方式"])
        tmpLstPic=[]
        tmpLstName=[]
        for l in lst:
            tmpLstPic.append(l.get_attribute("src"))
            # 图片标签的alt属性 是小区名称
            tmpLstName.append(l.get_attribute("alt"))
        tmpDf["图片地址"]=tmpLstPic
        tmpDf["小区名称"]=tmpLstName
        # self.lg.info(tmpDf)

        # 获取房屋信息
        roomInfoElement=self.gx.get_xml_data("room_list_page","room_info_text")
        roomLst=self.driver.get_elements(roomInfoElement)
        tmpLstSize=[]       # 房间大小
        tmpLstfloor=[]      # 楼层
        tmpLstPattern=[]    # 房间格局
        for r in roomLst:
            tmp=r.text.replace(" ","")
            tmp=tmp.split("|")
            tmpLstSize.append(tmp[0])
            tmpLstfloor.append(tmp[1])
            tmpLstPattern.append(tmp[2])

        tmpDf["平米数"]=tmpLstSize
        tmpDf["楼层"]=tmpLstfloor
        tmpDf["房屋格局"]=tmpLstPattern
        # 获取交通位置
        placeElement=self.gx.get_xml_data("room_list_page","place_text")
        placeLst=self.driver.get_elements(placeElement)
        tmpPlace=[]
        for p in placeLst:
            if p.text=="":
                tmpPlace.append("无交通信息")
            else:
                tmpPlace.append(p.text)
        tmpDf["交通位置"]=tmpPlace

        # 租房页面url
        moreElement=self.gx.get_xml_data("room_list_page","room_page_url")
        moreLst=self.driver.get_elements(moreElement)
        tmpUrlLst=[]
        for m in moreLst:
            tmpUrlLst.append(m.get_attribute("href"))

        tmpDf["租房页面url"]=tmpUrlLst
        priceList=self.get_price()
        # self.lg.info(priceList)
        tmpDf["价格"]=priceList
        # self.lg.info(tmpDf["价格"])

        # 收费方式
        priceWayElement=self.gx.get_xml_data("room_list_page","price_way")
        priceWayElements=self.driver.get_elements(priceWayElement)
        tmpPriceWayList=[]
        for p in priceWayElements:
            tmpPriceWayList.append(p.text)
        # print(tmpPriceWayList)
        tmpDf["收费方式"]=tmpPriceWayList
        self.data = self.data.append(tmpDf, ignore_index=True, verify_integrity=False,sort=False)



    def get_price(self):
        # 获取价格
        # pageInfo=self.driver.get_page_source()
        h = {"User-Agent": "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9.1.6) ",
                   "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                   "Accept-Language": "en-us",
                   "Connection": "keep-alive",
                   "Accept-Charset": "GB2312,utf-8;q=0.7,*;q=0.7"}
        res=requests.get(self.driver.driver.current_url,headers=h)

        if res.status_code==200:
            pageInfo=res.text
        else:
            # pageInfo = ""
            return self.get_price()
        pattern=re.compile(r"{\"image\".*?};")
        result=re.findall(pattern,pageInfo)[0].replace(";","")
        priceInfo=json.loads(result)
        SCR_CONF="ScreenShotPath"
        os.chdir(self.cf.get_conf_data(SCR_CONF)['path'])
        # print("http://static8.ziroom.com"+priceInfo["image"][1:])
        r=requests.get("https:"+priceInfo["image"])
        if r.status_code==200:
            with open("price.png","wb") as code:
                code.write(r.content)
        else:
            return self.get_price()
        tmpImage = Image.open("price.png").convert('L')
        tmpNewImage = PIL.ImageOps.invert(tmpImage)
        tmpNewImage.save('new_price.png')
        APP_ID="15434003"
        API_KEY="Gy3h1sbjm4bqIOpebd1gFivf"
        SECRET_KEY="195D88BYGAkFpgIS18ZtRiPTdZoqOa7c"
        client = AipOcr(APP_ID, API_KEY, SECRET_KEY)
        with open("new_price.png", 'rb') as fp:
            image=fp.read()
        priceNum=client.numbers(image)
        numberList=str(priceNum["words_result"][0]["words"])
        if len(numberList)!=10:
            return self.get_price()
        else:
            priceList = []
            priceListIndex=priceInfo["offset"]
            for pr in priceListIndex:
                tmpPrice=""
                for p in pr:
                    tmpPrice+=str(numberList[int(p)])
                priceList.append(tmpPrice)
            os.remove("new_price.png")
            os.remove("price.png")
            # self.lg.info("获取的价格为")
            # self.lg.info(priceList)
            return priceList
        # newVcode=Image.open("new_price.png").convert('L')
        # vcode = pytesseract.image_to_string(newVcode,config='--psm 7')
        # print("识别文字为："+vcode)




    def next_is_click(self):
        nextElement=self.gx.get_xml_data("room_list_page","next_btn")
        flag=self.driver.is_exist(nextElement)
        return flag


    def click_next(self):

        nextElement=self.gx.get_xml_data("room_list_page","next_btn")
        self.driver.click(nextElement)


    def run_get_data(self):
        self.get_data()
        while(self.next_is_click()):
            self.click_next()
            self.get_data()






    def insert_db(self):
        self.db.connect_mysql()

        runTime=time.strftime("%Y-%m-%d", time.localtime())

        _sql='''
        insert into get_data_number_of_times (run_date,ex_1) values (\"{rd}\",\"{ex_1}\")
        '''.format(rd=runTime,ex_1="自如网")
        self.db.cur.execute(_sql)
        gID=int(self.db.cur.lastrowid)
        # self.db.sql_commit()

        # print(self.data)
        # a=self.data.iterrows()
        # print("next"+next(a))
        # print(type(a))



        try:

            for index,d in self.data.iterrows():
                sql='''
                insert into room_info (pic_src,room_size,floor,room_pattern,place,price,community,page_url,g_id,price_way,rent_way,source,region) values (\"{ps}\",\"{rs}\",\"{f}\",\"{rp}\",\"{p}\",\"{pri}\",\"{com}\",\"{pu}\",{g},\"{pw}\",\"{rw}\",\"{so}\",\"{re}\");
                '''.format(ps=d["图片地址"],rs=d["平米数"],f=d["楼层"],rp=d["房屋格局"],p=d["交通位置"],pri=d["价格"],com=d["小区名称"],pu=d["租房页面url"],g=gID,pw=d["收费方式"],rw="合租",so="自如网",re="四季青")
                self.db.cur.execute(sql)
                self.lg.info(sql)

        except:
            self.lg.error("插入数据失败！")
        finally:
            self.db.sql_commit()
            self.db.close_connect()




        # # 价格
        # priceElement=self.gx.get_xml_data("room_list_page","price_text")
        # priceLst=self.driver.get_elements(priceElement)
        # for pr in priceLst:
        #
        #     print(pr.text)


    def clsoe_driver(self):
        self.driver.driver.close()



if __name__=="__main__":
    z=Ziroom()
    try:
        z.run_get_data()
        z.insert_db()
    finally:
        z.clsoe_driver()


