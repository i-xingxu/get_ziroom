#coding=utf-8

#导入路径
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import requests
import re
from PIL import Image
import PIL.ImageOps
import json
from base import webbase
from common import conf,logoutput,getxml,mysql
import pandas as pd

class Lianjia():

    def __init__(self):
        setup = webbase.SetUp()
        dr = setup.web_setup()
        self.driver = webbase.Web(dr)
        self.cf = conf.Conf()
        self.driver.get_url(self.cf.get_conf_data("ziroom")["lianjia"])
        self.lg = logoutput.Logger()
        self.gx = getxml.XmlOperation()
        self.db = mysql.Mysql()
        self.db.connect_mysql()
        self.cur = self.db.cur
        self.data=pd.DataFrame(columns=["图片地址","平米数","楼层","房屋格局","交通位置","价格","小区名称","租房页面url","收费方式","租房类型"])

    def get_list_data(self):
        tmpDf=pd.DataFrame(columns=["图片地址","平米数","楼层","房屋格局","交通位置","价格","小区名称","租房页面url","收费方式","租房类型"])

        # 获取图片url
        picUrlElement=self.gx.get_xml_data("lianjia_list_page","img_url")
        # self.driver.scroll_page("bottom")
        pirUrlElementList=self.driver.get_elements(picUrlElement)
        tmpPicUrlList=[]
        for picList in pirUrlElementList:
            tmpPicUrlList.append(picList.get_attribute("data-src"))

        # 获取租房页面url
        tmpPageUrlList=[]
        pageUrlElement=self.gx.get_xml_data("lianjia_list_page","page_url")
        pageUrlList=self.driver.get_elements(pageUrlElement)
        for page in pageUrlList:
            tmpPageUrlList.append(page.get_attribute("href"))

        # 获取价格
        tmpPriceList=[]
        tmpPricePay=[]
        priceElement=self.gx.get_xml_data("lianjia_list_page","price_list")
        priceList=self.driver.get_elements(priceElement)
        for p in priceList:
            t=p.text.split(" ")
            tmpPriceList.append(t[0])
            tmpPricePay.append(t[1])

        # 房屋格局
        tmpPattertList=[]
        tmpSizeList=[]
        patternElement=self.gx.get_xml_data("lianjia_list_page","pattern_list_text")
        patternList=self.driver.get_elements(patternElement)
        for pat in patternList:
            t=pat.text.split("/")
            tmpPattertList.append(t[3])
            tmpSizeList.append(t[1])
        # print(tmpPattertList)
        # print(tmpSizeList)

        # 获取楼层 小区名称 交通位置
        tmpFloorList=[]
        tmpPlaceList=[]
        tmpNameList=[]
        tmpRentList=[]
        for u in tmpPageUrlList:
            r=self.get_url_data(u)
            tmpFloorList.append(r["楼层"])
            tmpPlaceList.append(r["交通位置"])
            tmpNameList.append(r["小区名称"])
            tmpRentList.append(r["租房类型"])

        # self.get_url_data("https://bj.lianjia.com/zufang/BJ2193400735482789888.html")


        tmpDf["图片地址"]=tmpPicUrlList
        tmpDf["租房页面url"]=tmpPageUrlList
        tmpDf["价格"]=tmpPriceList
        tmpDf["收费方式"]=tmpPricePay
        tmpDf["房屋格局"]=tmpPattertList
        tmpDf["平米数"]=tmpSizeList
        tmpDf["楼层"]=tmpFloorList
        tmpDf["交通位置"]=tmpPlaceList
        tmpDf["小区名称"]=tmpNameList
        tmpDf["租房类型"]=tmpRentList
        # print(tmpDf["租房类型"])
        self.data = self.data.append(tmpDf, ignore_index=True, verify_integrity=False, sort=False)

    def get_url_data(self,url):

        roomInfo={}

        h = {"User-Agent": "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9.1.6) ",
                   "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                   "Accept-Language": "en-us",
                   "Connection": "keep-alive",
                   "Accept-Charset": "GB2312,utf-8;q=0.7,*;q=0.7"}
        res = requests.get(url, headers=h)
        if res.status_code == 200:
            pageInfo = res.text
        else:
            return  self.get_url_data(url)

        pattern=re.compile(r"class=\"fl oneline\">楼层：.*?</li>")
        # patternPlace=re.compile(r"距离.*?<span>(.*?)</span>(.*?)</span>",re.S)
        d=re.findall(pattern, pageInfo)[0]
        d=d.split(">")[1]
        d=d.split("<")[0]
        d=d.split("：")[1]
        roomInfo["楼层"]=d

        resultPlace=re.search(r"距离\n.*?<span>(.*?)</span>.*?<span>(.*?)</span>",pageInfo,re.S)
        if resultPlace:
            roomInfo["交通位置"]=resultPlace.group(1)+resultPlace.group(2)
        else:
            roomInfo["交通位置"]="无交通信息"

        resultName=re.search(r"g_conf.name = \'(.*?)\';",pageInfo,re.S)
        if resultName:
            roomInfo["小区名称"]=resultName.group(1)
        # print(roomInfo)
        resultRent=re.search(r"class=\"house\"></i>(.*?)</span>",pageInfo,re.S)
        if resultRent:
            roomInfo["租房类型"]=resultRent.group(1)

        return roomInfo

    def next_exist(self):

        pageListElement=self.gx.get_xml_data("lianjia_list_page","next_link")
        flag=self.driver.is_exist(pageListElement)
        if flag:
            return True
        else:
            return False
        # print(pageList)

    def click_next(self):

        nextLinkElement=self.gx.get_xml_data("lianjia_list_page","next_link")
        self.driver.click(nextLinkElement)

    def page_list(self):
        pageNumberElement = self.gx.get_xml_data("lianjia_list_page", "pagenumber_url_list")
        pageNumberList=self.driver.get_elements(pageNumberElement)
        pageNumberUrlList=[]
        for p in pageNumberList:
            pageNumberUrlList.append(p.get_attribute("href"))
        if len(pageNumberUrlList)==0:
            return False
        else:
            return pageNumberUrlList[2:]

    def insert_db(self):
        self.db.connect_mysql()
        runTime=time.strftime("%Y-%m-%d", time.localtime())

        _sql='''
        insert into get_data_number_of_times (run_date,ex_1) values (\"{rd}\",\"{ex_1}\")
        '''.format(rd=runTime,ex_1="链家")
        self.db.cur.execute(_sql)
        gID=int(self.db.cur.lastrowid)
        try:
            for index,d in self.data.iterrows():
                sql='''
                insert into room_info (pic_src,room_size,floor,room_pattern,place,price,community,page_url,g_id,price_way,rent_way,source,region) values (\"{ps}\",\"{rs}\",\"{f}\",\"{rp}\",\"{p}\",\"{pri}\",\"{com}\",\"{pu}\",{g},\"{pw}\",\"{rw}\",\"{so}\",\"{re}\");
                '''.format(ps=d["图片地址"],rs=d["平米数"],f=d["楼层"],rp=d["房屋格局"],p=d["交通位置"],pri=d["价格"],com=d["小区名称"],pu=d["租房页面url"],g=gID,pw=d["收费方式"],rw=d["租房类型"],so="链家",re="四季青")
                self.db.cur.execute(sql)
                self.lg.info(sql)
        except:
            self.lg.error("插入数据失败！")
        finally:
            self.db.sql_commit()
            self.db.close_connect()

    def run(self):

        if self.next_exist():
            while (1):
                self.get_list_data()
                if self.next_exist()==False:
                    break
                else:
                    self.click_next()
        else:
            self.get_list_data()
            lst=self.page_list()
            if lst:
                for u in lst:
                    self.driver.get_url(u)
                    self.get_list_data()

    def close_driver(self):
        self.driver.driver.close()







if __name__=="__main__":
    l=Lianjia()
    # l.get_list_data()
    try:
        l.run()
        l.insert_db()
    finally:
        l.close_driver()