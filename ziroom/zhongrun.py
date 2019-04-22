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

class ZhongRun():

    def __init__(self):
        setup = webbase.SetUp()
        dr = setup.web_setup()
        drForUrl=setup.web_setup()
        self.driverForUrl=webbase.Web(drForUrl)
        self.driver = webbase.Web(dr)
        self.cf = conf.Conf()
        self.driver.get_url(self.cf.get_conf_data("ziroom")["zhongrun"])
        self.lg = logoutput.Logger()
        self.gx = getxml.XmlOperation()
        self.db = mysql.Mysql()
        self.db.connect_mysql()
        self.cur = self.db.cur
        self.data=pd.DataFrame(columns=["图片地址","平米数","楼层","房屋格局","交通位置","价格","小区名称","租房页面url","收费方式","租房类型"])

    def select_place(self):

        quyuElement=self.gx.get_xml_data("zhongrun_list_page","area_link")
        self.driver.click(quyuElement,waittime=2)
        placeElement=self.gx.get_xml_data("zhongrun_list_page","place_link")
        self.driver.click(placeElement,waittime=2)

    def get_list_data(self):

        tmpDf=pd.DataFrame(columns=["图片地址","平米数","楼层","房屋格局","交通位置","价格","小区名称","租房页面url","收费方式","租房类型"])

        # 图片地址
        tmpPicUrlList=[]
        picUrlElements=self.gx.get_xml_data("zhongrun_list_page","img_url_list")
        picUrlList=self.driver.get_elements(picUrlElements,waittime=2)
        for p in picUrlList:
            tmpPicUrlList.append(p.get_attribute("src"))
        tmpDf["图片地址"]=tmpPicUrlList

        # 租房页面url
        tmpPageUrlList=[]
        pageUrlElements=self.gx.get_xml_data("zhongrun_list_page","page_url_list")
        pageUrlList=self.driver.get_elements(pageUrlElements)
        for u in pageUrlList:
            tmpPageUrlList.append(u.get_attribute("href"))
        tmpDf["租房页面url"]=tmpPageUrlList

        # 租房类型
        tmpRentList=[]
        rentElements=self.gx.get_xml_data("zhongrun_list_page","rent_test")
        rentTestList=self.driver.get_elements(rentElements)
        for t in rentTestList:
            tmpRentList.append(t.text.split("|")[1])
        tmpDf["租房类型"] = tmpRentList


        # print(tmpPageUrlList)
        tmpPriceList=[]
        tmpPricePay=[]
        tmpFloorList=[]
        tmpSizeList=[]
        tmpPattertList=[]
        tmpNameList=[]
        tmpPlaceList=[]
        for r in tmpPageUrlList:
            result=self.get_url_data(r)
            tmpPriceList.append(result["价格"])
            tmpPricePay.append(result["收费方式"])
            tmpFloorList.append(result["楼层"])
            tmpSizeList.append(result["平米数"])
            tmpPattertList.append(result["房屋格局"])
            tmpNameList.append(result["小区名称"])
            tmpPlaceList.append(result["交通位置"])
        tmpDf["价格"]=tmpPriceList
        tmpDf["收费方式"]=tmpPricePay
        tmpDf["楼层"]=tmpFloorList
        tmpDf["平米数"]=tmpSizeList
        tmpDf["房屋格局"]=tmpPattertList
        tmpDf["小区名称"]=tmpNameList
        tmpDf["交通位置"]=tmpPlaceList

        # print(tmpRentList)
        # print(tmpDf["小区名称"])
        self.data = self.data.append(tmpDf, ignore_index=True, verify_integrity=False, sort=False)


    def get_url_data(self,url):

        roomInfo={}


        # print(url)
        # h = {"User-Agent": "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9.1.6) ",
        #            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        #            "Accept-Language": "en-us",
        #            "Connection": "keep-alive",
        #            "Accept-Charset": "GB2312,utf-8;q=0.7,*;q=0.7"}
        # res = requests.get(url, headers=h)
        # res.encoding="utf-8"
        # if res.status_code == 200:
        #     pageInfo=res.text
        # else:
        #     return self.get_url_data(url)
        self.driverForUrl.get_url(url)
        pageInfo=self.driverForUrl.get_page_source()

        resultPrice=re.search("￥<strong>(.*?)</strong>(.*?)</h4>",pageInfo,re.S)
        if resultPrice:
            roomInfo["价格"]=resultPrice.group(1)
            roomInfo["收费方式"]=resultPrice.group(2)

        resultFloor=re.search("层</span>(.*?)</p>",pageInfo,re.S)
        if resultFloor:
            roomInfo["楼层"]=resultFloor.group(1)

        resultSize=re.search("积</span>(.*?)</p>",pageInfo,re.S)
        if resultSize:
            roomInfo["平米数"]=resultSize.group(1)

        resultPattert=re.search("室</span>(.*?)</p>",pageInfo,re.S)
        if resultPattert:
            roomInfo["房屋格局"]=resultPattert.group(1)

        resultName=re.search("区</span>(.*?)</p>",pageInfo,re.S)
        if resultName:
            roomInfo["小区名称"]=resultName.group(1)

        roomInfo["交通位置"]="无交通信息"



        # print(roomInfo["小区名称"])

        # print(res.text)
        return roomInfo

    def next_exist(self):

        nextElement=self.gx.get_xml_data("zhongrun_list_page","next_link")

        flag=self.driver.is_exist(nextElement)
        return flag

    def click_next(self):

        nextElement = self.gx.get_xml_data("zhongrun_list_page", "next_link")
        if self.next_exist():
            self.driver.click(nextElement)


    def run(self):

        self.select_place()
        self.get_list_data()
        while self.next_exist():
            self.click_next()
            self.get_list_data()

    def close_driver(self):

        self.driverForUrl.driver.close()
        self.driver.driver.close()

    def insert_db(self):
        self.db.connect_mysql()
        runTime=time.strftime("%Y-%m-%d", time.localtime())

        _sql='''
        insert into get_data_number_of_times (run_date,ex_1) values (\"{rd}\",\"{ex_1}\")
        '''.format(rd=runTime,ex_1="中润置家")
        self.db.cur.execute(_sql)
        gID=int(self.db.cur.lastrowid)
        try:
            for index,d in self.data.iterrows():
                sql='''
                insert into room_info (pic_src,room_size,floor,room_pattern,place,price,community,page_url,g_id,price_way,rent_way,source,region) values (\"{ps}\",\"{rs}\",\"{f}\",\"{rp}\",\"{p}\",\"{pri}\",\"{com}\",\"{pu}\",{g},\"{pw}\",\"{rw}\",\"{so}\",\"{re}\");
                '''.format(ps=d["图片地址"],rs=d["平米数"],f=d["楼层"],rp=d["房屋格局"],p=d["交通位置"],pri=d["价格"],com=d["小区名称"],pu=d["租房页面url"],g=gID,pw=d["收费方式"],rw=d["租房类型"],so="中润置家",re="四季青")
                self.db.cur.execute(sql)
                self.lg.info(sql)
        except:
            self.lg.error("插入数据失败！")
        finally:
            self.db.sql_commit()
            self.db.close_connect()

if __name__=="__main__":

    z=ZhongRun()
    # z.select_place()
    # z.get_list_data()
    try:
        z.run()
        z.insert_db()
    finally:
        z.close_driver()
